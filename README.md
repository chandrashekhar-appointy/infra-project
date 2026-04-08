# Infra Smoke App

A tiny Go app that exercises four Bifrost-managed infra types:

- PostgreSQL via `DATABASE_URL`
- Redis via `REDIS_URL`
- GCS bucket via `BUCKET_NAME`
- Pub/Sub via `PUBSUB_PROJECT_ID` + `PUBSUB_TOPIC`

It works in two modes:

- local mode with explicit JSON keys for bucket/pubsub
- deployed mode with Bifrost runtime bindings and workload identity

## Endpoints

- `GET /health`
- `GET /smoke`
- `GET /`

## Local-only auth envs

- `BUCKET_CREDENTIALS_FILE`
- `PUBSUB_CREDENTIALS_FILE`

## Optional envs

- `APP_PORT` default `8080`
- `REDIS_KEY_PREFIX` default `infra-smoke`
- `AUTO_SMOKE_ON_START=true`
- `PUBSUB_SUBSCRIPTION` to attempt a receive after publish
