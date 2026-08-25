# Keycloak Event Listener (Kafka)

Keycloak SPI (`ext-event-kafka`) that forwards user and admin events to Kafka as JSON.

## Behavior

- Filters events by realm (`KAFKA_ALLOWED_REALMS`) and by type/operation (`events-config.json`).
- User events → `KAFKA_USER_TOPIC`, keyed by user id (fallback: realm id).
- Admin events → `KAFKA_ADMIN_TOPIC`, keyed by realm id.
- Shared singleton producer; send failures are logged, never thrown.

## Event filter (`events-config.json`)

Loaded from classpath (`KAFKA_CONFIG_FILE`, default `events-config.json`). Values must match Keycloak's `EventType` / `ResourceType` / `OperationType`. Falls back to `LOGIN`/`REGISTER`/`LOGOUT` + admin `USER` CRUD if missing/invalid.

```json
{
  "userEvents": ["LOGIN", "LOGOUT", "REGISTER", "UPDATE_PROFILE", "UPDATE_EMAIL", "VERIFY_EMAIL", "RESET_PASSWORD"],
  "adminEvents": {
    "USER": ["CREATE", "UPDATE", "DELETE"],
    "REALM": ["UPDATE"],
    "CLIENT": ["CREATE", "UPDATE", "DELETE"]
  }
}
```

## Environment variables

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_CLIENT_ID` | `keycloak` |
| `KAFKA_ACKS` | `all` |
| `KAFKA_RETRIES` | `3` |
| `KAFKA_COMPRESSION` | `gzip` |
| `KAFKA_USER_TOPIC` | `keycloak-user-events` |
| `KAFKA_ADMIN_TOPIC` | `keycloak-admin-events` |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` |
| `KAFKA_SASL_USERNAME` | _(empty)_ |
| `KAFKA_SASL_PASSWORD` | _(empty)_ |
| `KAFKA_ALLOWED_REALMS` | _(empty = all)_, comma-separated or `*` |
| `KAFKA_CONFIG_FILE` | `events-config.json` |

## Build

```bash
./gradlew clean build
# -> build/libs/keycloak-event-listener-<version>.jar
```

## Run locally

```bash
cd deployment
docker compose up -d
```

Brings up Keycloak (`:8084`, admin/password) with the provider wired in, Kafka + Kafka UI (`:8088`), Postgres + Adminer (`:8080`).

## Deploy

1. Copy jar to Keycloak's `providers/`.
2. Copy `events-config.json` to the classpath location `KAFKA_CONFIG_FILE` points to.
3. Set `KAFKA_*` env vars.
4. Restart (or `kc.sh build` first, depending on distribution mode).
