# Buzz Docker Compose deployment

This is the single-node/VPS deployment bundle. It is intentionally separate from
the root `docker-compose.yml`, which remains local development infrastructure.

## Quick start

```bash
cd deploy/compose
cp .env.example .env
$EDITOR .env       # replace every CHANGE_ME value
./run.sh start
```

For a public VPS with automatic Let's Encrypt certificates:

```bash
cd deploy/compose
BUZZ_COMPOSE_TLS=true ./run.sh start
```

The bootstrap script should eventually replace manual `.env` editing for normal
users. It is responsible for generating stable secrets and, optionally, an owner
keypair.

## Production notes

- Requires Docker Compose v2.24.4 or newer; the TLS override uses Compose's
  `!reset` tag to remove the direct relay port when Caddy terminates HTTPS.
- `BUZZ_IMAGE` must be pinned to an immutable OCI digest (`repository@sha256:...`); floating tags such as `:main`, `:latest`, or semver tags are rejected by `run.sh`.
- Keep `BUZZ_RELAY_PRIVATE_KEY`, `BUZZ_GIT_HOOK_HMAC_SECRET`, database/Redis,
  and S3 secrets stable across restarts.
- `RELAY_OWNER_PUBKEY` is intentionally not prefixed with `BUZZ_`; it must be a
  64-character hex Nostr pubkey when closed relay mode is enabled.
- `BUZZ_AUTO_MIGRATE` is opt-in. Set `BUZZ_AUTO_MIGRATE=true` or run
  `buzz-admin migrate` before starting the relay when bootstrapping a fresh
  database. Auto-migration requires an image that includes embedded SQLx
  migrations.
- The stack uses Postgres, Redis, MinIO, and a git data volume because
  those are real Buzz dependencies today. Minimal mode can simplify this later.
- Mobile push remains off by default. To use the public gateway, keep the
  template's explicit `BUZZ_PUSH_GATEWAY_DELIVERY_URL` and set
  `BUZZ_PUSH_ENABLED=true`. To use another gateway, replace the exact HTTPS
  `/v1/deliveries/apns` URL before enabling push.
- The bundled Compose stack fixes the relay endpoint to `http://minio:9000` and
  `BUZZ_S3_ADDRESSING_STYLE=path`: Docker DNS resolves `minio`, not
  `<bucket>.minio`. It is not configurable for an external S3 provider through
  `.env`; use the Helm chart or a custom Compose configuration for providers
  such as new Railway Storage Buckets that require `virtual` addressing.

Run `./run.sh backup-hint` for the backup checklist.

## Backup and restore verification

Use the Postgres client from the same major version as the server. Capture the
Postgres dump and the object/git volumes from one maintenance window; do not
call a backup complete when only Postgres was captured.

```bash
# Postgres custom-format backup and catalog inspection.
docker compose exec -T postgres sh -ec \
  'pg_dump --format=custom --no-owner -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  > backups/buzz-$(date -u +%Y%m%dT%H%M%SZ).dump
pg_restore --list backups/buzz-*.dump
sha256sum backups/buzz-*.dump > backups/SHA256SUMS
```

Restore drills MUST target a disposable database or isolated Compose project.
Never restore over the live database as a test. After restoring, compare event
and audit-row counts and verify each tenant chain with the operator CLI:

```bash
DATABASE_URL="$RESTORED_DATABASE_URL" buzz-admin verify-audit \
  --community-id "$COMMUNITY_ID" --from-seq 1 --to-seq 100000
```

For chains longer than 100,000 entries, verify contiguous 100,000-entry ranges
until the final sequence.

A successful `pg_restore` is not sufficient: the restored audit chain must
verify and the restored object/git volume manifests must match the backup
checksums. A failed verification is a restore failure and must block rollout.

## Validation

Before sharing an install link publicly, verify a fresh install with:

```bash
cd deploy/compose
cp .env.example .env
$EDITOR .env
./run.sh config
./run.sh start
curl -fsS "http://127.0.0.1:$(grep -E '^BUZZ_HTTP_PORT=' .env | cut -d= -f2-)/_liveness"
./run.sh status
```
