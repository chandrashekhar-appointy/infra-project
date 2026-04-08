package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/option"
)

type Config struct {
	Port                  string
	DatabaseURL           string
	RedisURL              string
	RedisKeyPrefix        string
	BucketName            string
	BucketCredentialsFile string
	PubSubProjectID       string
	PubSubTopic           string
	PubSubSubscription    string
	PubSubCredentialsFile string
	AutoSmokeOnStart      bool
}

type SmokeResult struct {
	RunID                string            `json:"run_id"`
	StartedAt            time.Time         `json:"started_at"`
	Database             map[string]any    `json:"database"`
	Redis                map[string]any    `json:"redis"`
	Bucket               map[string]any    `json:"bucket"`
	PubSub               map[string]any    `json:"pubsub"`
	Success              bool              `json:"success"`
	Error                string            `json:"error,omitempty"`
	EffectiveEnvironment map[string]string `json:"effective_environment"`
}

type App struct {
	cfg        Config
	httpClient *http.Client
	mu         sync.RWMutex
	lastResult *SmokeResult
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	app := &App{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}

	if cfg.AutoSmokeOnStart {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			result := app.runSmoke(ctx)
			app.setLastResult(result)
			payload, _ := json.MarshalIndent(result, "", "  ")
			log.Printf("startup smoke result:\n%s", payload)
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleIndex)
	mux.HandleFunc("/health", app.handleHealth)
	mux.HandleFunc("/smoke", app.handleSmoke)

	addr := ":" + cfg.Port
	log.Printf("infra smoke app listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{
		Port:                  envOrDefault("APP_PORT", "8080"),
		DatabaseURL:           strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RedisURL:              strings.TrimSpace(os.Getenv("REDIS_URL")),
		RedisKeyPrefix:        envOrDefault("REDIS_KEY_PREFIX", "infra-smoke"),
		BucketName:            strings.TrimSpace(os.Getenv("BUCKET_NAME")),
		BucketCredentialsFile: strings.TrimSpace(os.Getenv("BUCKET_CREDENTIALS_FILE")),
		PubSubProjectID:       strings.TrimSpace(os.Getenv("PUBSUB_PROJECT_ID")),
		PubSubTopic:           strings.TrimSpace(os.Getenv("PUBSUB_TOPIC")),
		PubSubSubscription:    strings.TrimSpace(os.Getenv("PUBSUB_SUBSCRIPTION")),
		PubSubCredentialsFile: strings.TrimSpace(os.Getenv("PUBSUB_CREDENTIALS_FILE")),
		AutoSmokeOnStart:      strings.EqualFold(strings.TrimSpace(os.Getenv("AUTO_SMOKE_ON_START")), "true"),
	}

	missing := make([]string, 0)
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.RedisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if cfg.BucketName == "" {
		missing = append(missing, "BUCKET_NAME")
	}
	if cfg.PubSubProjectID == "" {
		missing = append(missing, "PUBSUB_PROJECT_ID")
	}
	if cfg.PubSubTopic == "" {
		missing = append(missing, "PUBSUB_TOPIC")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"message":     "infra smoke app is running",
		"health_path": "/health",
		"smoke_path":  "/smoke",
		"last_result": a.getLastResult(),
		"configuration": map[string]any{
			"bucket_name":         a.cfg.BucketName,
			"pubsub_project_id":   a.cfg.PubSubProjectID,
			"pubsub_topic":        a.cfg.PubSubTopic,
			"pubsub_subscription": a.cfg.PubSubSubscription,
			"redis_key_prefix":    a.cfg.RedisKeyPrefix,
		},
	})
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handleSmoke(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	result := a.runSmoke(ctx)
	a.setLastResult(result)
	status := http.StatusOK
	if !result.Success {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, result)
}

func (a *App) runSmoke(ctx context.Context) *SmokeResult {
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano())
	result := &SmokeResult{
		RunID:     runID,
		StartedAt: time.Now().UTC(),
		Database:  map[string]any{},
		Redis:     map[string]any{},
		Bucket:    map[string]any{},
		PubSub:    map[string]any{},
		EffectiveEnvironment: map[string]string{
			"bucket_name":         a.cfg.BucketName,
			"pubsub_project_id":   a.cfg.PubSubProjectID,
			"pubsub_topic":        a.cfg.PubSubTopic,
			"pubsub_subscription": a.cfg.PubSubSubscription,
			"redis_key_prefix":    a.cfg.RedisKeyPrefix,
			"bucket_auth_mode":    authMode(a.cfg.BucketCredentialsFile),
			"pubsub_auth_mode":    authMode(a.cfg.PubSubCredentialsFile),
		},
	}

	failures := make([]string, 0)
	if err := a.testDatabase(ctx, runID, result.Database); err != nil {
		failures = append(failures, fmt.Sprintf("database: %v", err))
	}
	if err := a.testRedis(ctx, runID, result.Redis); err != nil {
		failures = append(failures, fmt.Sprintf("redis: %v", err))
	}
	if err := a.testBucket(ctx, runID, result.Bucket); err != nil {
		failures = append(failures, fmt.Sprintf("bucket: %v", err))
	}
	if err := a.testPubSub(ctx, runID, result.PubSub); err != nil {
		failures = append(failures, fmt.Sprintf("pubsub: %v", err))
	}
	if len(failures) == 0 {
		result.Success = true
		return result
	}
	result.Error = strings.Join(failures, "; ")
	return result
}

func (a *App) testDatabase(ctx context.Context, runID string, out map[string]any) error {
	db, err := sql.Open("pgx", a.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS infra_smoke_runs (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			note TEXT NOT NULL
		)
	`); err != nil {
		return err
	}
	note := fmt.Sprintf("infra smoke run %s", runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO infra_smoke_runs (id, note)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note
	`, runID, note); err != nil {
		return err
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM infra_smoke_runs`).Scan(&count); err != nil {
		return err
	}
	out["inserted_id"] = runID
	out["row_count"] = count
	return nil
}

func (a *App) testRedis(ctx context.Context, runID string, out map[string]any) error {
	opt, err := redis.ParseURL(a.cfg.RedisURL)
	if err != nil {
		return err
	}
	client := redis.NewClient(opt)
	defer client.Close()

	key := fmt.Sprintf("%s:%s", a.cfg.RedisKeyPrefix, runID)
	value := fmt.Sprintf("redis-ok-%s", runID)
	if err := client.Set(ctx, key, value, 2*time.Minute).Err(); err != nil {
		return err
	}
	got, err := client.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	out["key"] = key
	out["value"] = got
	return nil
}

func (a *App) testBucket(ctx context.Context, runID string, out map[string]any) error {
	client, err := a.newStorageClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(a.cfg.BucketName)
	objectName := fmt.Sprintf("infra-smoke/%s.json", runID)
	payload, _ := json.Marshal(map[string]any{
		"run_id":     runID,
		"written_at": time.Now().UTC().Format(time.RFC3339),
	})
	wc := bucket.Object(objectName).NewWriter(ctx)
	wc.ContentType = "application/json"
	if _, err := wc.Write(payload); err != nil {
		_ = wc.Close()
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	rc, err := bucket.Object(objectName).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	out["object"] = objectName
	out["bytes"] = len(body)
	out["verified"] = bytes.Equal(bytes.TrimSpace(body), bytes.TrimSpace(payload))
	return nil
}

func (a *App) testPubSub(ctx context.Context, runID string, out map[string]any) error {
	client, err := a.newPubSubClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	topic := client.Topic(a.cfg.PubSubTopic)
	defer topic.Stop()
	msg := &pubsub.Message{
		Data: []byte(fmt.Sprintf("infra smoke pubsub %s", runID)),
		Attributes: map[string]string{
			"run_id": runID,
			"source": "infra-project",
		},
	}
	res := topic.Publish(ctx, msg)
	serverID, err := res.Get(ctx)
	if err != nil {
		return err
	}
	out["published_message_id"] = serverID

	if strings.TrimSpace(a.cfg.PubSubSubscription) != "" {
		sub := client.Subscription(a.cfg.PubSubSubscription)
		pullCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		var got map[string]any
		err = sub.Receive(pullCtx, func(_ context.Context, m *pubsub.Message) {
			got = map[string]any{
				"received_message_id": m.ID,
				"received_run_id":     m.Attributes["run_id"],
			}
			m.Ack()
			cancel()
		})
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if got != nil {
			for k, v := range got {
				out[k] = v
			}
		}
	}
	return nil
}

func (a *App) newStorageClient(ctx context.Context) (*storage.Client, error) {
	if file := strings.TrimSpace(a.cfg.BucketCredentialsFile); file != "" {
		return storage.NewClient(ctx, option.WithCredentialsFile(file))
	}
	return storage.NewClient(ctx)
}

func (a *App) newPubSubClient(ctx context.Context) (*pubsub.Client, error) {
	if file := strings.TrimSpace(a.cfg.PubSubCredentialsFile); file != "" {
		return pubsub.NewClient(ctx, a.cfg.PubSubProjectID, option.WithCredentialsFile(file))
	}
	return pubsub.NewClient(ctx, a.cfg.PubSubProjectID)
}

func (a *App) setLastResult(result *SmokeResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastResult = result
}

func (a *App) getLastResult() *SmokeResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastResult
}

func authMode(file string) string {
	if strings.TrimSpace(file) != "" {
		return "credentials-file"
	}
	return "default-credentials"
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
