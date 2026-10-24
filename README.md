# payment

Ferrumec payment service, rebuilt on the `ferrumec` and `viewset` crates.
It keeps account balances, a credit ledger, and deferred debits that wait for funds.

## Run

```sh
sqlx migrate run                      # migrations/ is idempotent
cargo run                             # Postgres + Redis + NATS (DATABASE_URL, REDIS_URL, NATS_URL)
cargo run --no-default-features       # Postgres + in-process cache/events (local dev)
```

`ferrumec::launch` serves on `127.0.0.1:8080`, loads `permissions.json` from the working directory, and starts OpenTelemetry. Don't install your own `tracing` subscriber in `main`.

## Routes (namespace `payment`, under `/api`)

| Route | Notes |
|---|---|
| `POST /api/payment/credit` `{user_id, amount}` | Same behaviour as before. Not permission-gated, as before. |
| `GET /api/payment/statement/{user_id}?since=<RFC3339>` | Same as before. |
| `/api/payment/admin/{accounts,credits,processed-debits}[/{id}]` | viewset CRUD; gated by `permissions.json`. |

Events: consumes `payment.debit.request`; emits `payment.debit.success` and `payment.debit.failed` through ferrumec's transactional `Outbox`.

## Permissions: read this

ferrumec's middleware only protects routes listed in `permissions.json`. A route with no entry is allowed. The file lists every admin route against two bits:

- `40`: read (`GET`)
- `41`: write (`POST`, `PUT`, `PATCH`, `DELETE`)

These bit numbers are placeholders. Change them to match your auth service's role bitmask. If you add routes or change the namespace, update the file too.

## What changed from the previous version

- `infra`, `typed-eventbus`, `actixutils`, `moka` and the local `CacheFactory`, `AuthSession` and `main` wiring are gone. They are replaced by `ferrumec::{Infra, Module, launch, Outbox}` and viewset's `ApiError`.
- A debit and its idempotency record now settle in one transaction, under a row lock on the account. Before, `try_debit` and `mark_processed` were separate steps, so a crash between them could double-debit on redelivery.
- Success and failure events are written to the outbox in the same transaction as the ledger change. Before, they were published after the commit and could be lost.
- `POST /credit` no longer returns an error once the credit has committed. Failures while retrying parked debits are logged and retried on the next credit. Before, a 500 invited the caller to retry and double-credit.
- Redelivering a `debit.request` for a debit that is already parked keeps the original deadline.
- Transient handler errors are returned to the event stream, so an at-least-once stream redelivers. Invalid requests are dropped.
- The admin viewsets have no caches. The domain code writes the same tables with raw SQL, so the old per-entity caches could serve stale balances.
- Amounts above `i64::MAX` are rejected with 422. They used to wrap to a negative number.
- The producer typo `"paymemt"` on the retried-success event is fixed.
