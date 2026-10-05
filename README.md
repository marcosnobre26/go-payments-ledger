# go-payments-ledger

A payments microservice in Go: a **double-entry ledger** with **idempotent transfers** and a **secure webhook receiver** that processes payment-provider events asynchronously, with retries. It runs locally with Docker Compose or on Kubernetes, with Prometheus metrics and a Grafana dashboard.

![CI](https://github.com/marcosnobre26/go-payments-ledger/actions/workflows/ci.yml/badge.svg)

## Contents

- [Purpose and context](#purpose-and-context)
- [Features](#features)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
- [API reference](#api-reference)
- [Kubernetes](#kubernetes)
- [Observability: Grafana and Prometheus](#observability-grafana-and-prometheus)
- [Seeding demo data](#seeding-demo-data)
- [Browsing the database](#browsing-the-database)
- [Testing](#testing)
- [Configuration and Make targets](#configuration-and-make-targets)
- [Troubleshooting](#troubleshooting)
- [Project structure](#project-structure)
- [Roadmap](#roadmap)

## Purpose and context

Any product that moves money (digital wallets, marketplaces, subscription platforms, payment gateways) faces the same small set of hard problems, regardless of language or framework:

- **Never move money twice.** Clients and networks retry requests. A retried transfer must return the original result, not create a second one.
- **Never lose or double-apply a provider event.** Payment providers (Stripe, Mercado Pago, PagSeguro…) confirm payments through webhooks that can arrive late, out of order or more than once.
- **Never trust an unsigned request.** Anyone can call a public webhook URL; only events signed by the provider may touch balances.
- **Never overdraw an account under concurrency.** Two simultaneous transfers from the same account must not both succeed if the balance only covers one.
- **Always be able to explain every cent.** Balances must be auditable and reconcilable, which is why financial systems use double-entry bookkeeping.

This project implements those guarantees end to end in a deliberately small domain, so each technique is easy to read, test and explain:

- an **account ledger** where deposits and transfers are recorded as balanced double entries;
- a **webhook pipeline** that verifies, deduplicates and asynchronously applies `payment.succeeded` events from a payment provider (simulated as `acmepay`);
- an **idempotent transfer API** that is safe to retry;
- the **operational layer** expected in production: metrics, dashboards, health probes, graceful shutdown and a Kubernetes deployment with multiple replicas.

Out of scope on purpose: authentication/authorization of API clients, currency conversion, refunds and multi-provider configuration. They are natural extensions (see the [Roadmap](#roadmap)) but would not add new ideas to the core problems above.

## Features

- **Double-entry ledger.** Every movement is a transaction whose entries sum to zero. Balances are cached on the account and updated in the same database transaction.
- **Idempotent transfers.** `POST /v1/transfers` requires an `Idempotency-Key`. A retried request returns the original result (`Idempotent-Replayed: true`); reusing a key with a different payload is rejected.
- **Signed webhooks.** HMAC-SHA256 over `timestamp.body`, constant-time comparison and a 5-minute tolerance window against replay attacks.
- **Deduplicated, asynchronous processing.** Events are stored once (`UNIQUE (provider, event_id)`), acknowledged immediately with `202`, and applied by a background worker.
- **Retries with exponential backoff.** Transient failures are retried (2s, 4s, 8s… capped at 10 min); business-rule failures (unknown account, currency mismatch) fail fast.
- **Safe under concurrency.** Row locks taken in a deterministic order prevent deadlocks; a `CHECK` constraint guarantees balances never go negative.
- **Observability.** Prometheus metrics (`/metrics`) for HTTP traffic, latency, transfers and the webhook pipeline, plus a provisioned Grafana dashboard.
- **Runs on Kubernetes.** Manifests for a local `kind` cluster: multiple API replicas with liveness/readiness probes, PostgreSQL as a StatefulSet, Prometheus with Kubernetes service discovery, and Grafana.
- **Seed data.** A seed command (also packaged as a Kubernetes Job) populates the database through the real domain code and verifies ledger integrity at the end.
- **Production basics.** Structured JSON logs (`slog`), request IDs, panic recovery, body size limits, HTTP timeouts, graceful shutdown and hardened containers (non-root, read-only filesystem).

## Architecture

```mermaid
flowchart LR
    P[Payment provider] -- signed webhook --> R[POST /v1/webhooks/:provider]
    R -- verify HMAC + dedupe --> E[(webhook_events)]
    R -- 202 Accepted --> P
    W[Worker<br/>FOR UPDATE SKIP LOCKED] -- poll due events --> E
    W -- deposit --> L[(accounts<br/>transactions<br/>ledger_entries)]
    C[Client] -- Idempotency-Key --> T[POST /v1/transfers]
    T -- one DB transaction --> L
    T --> I[(idempotency_keys)]
```

### Data model

| Table | Purpose |
|---|---|
| `accounts` | Customer accounts (one currency each) and one system *settlement* account per currency, used as the counterpart of deposits. Holds the cached `balance` in minor units. |
| `transactions` | One row per money movement (`transfer` or `deposit`). `reference` stores the provider event and is `UNIQUE`. |
| `ledger_entries` | The double entries: a negative entry on the source account and a positive one on the destination. They always sum to zero per transaction. |
| `idempotency_keys` | Request fingerprint and resulting transaction for every `Idempotency-Key`. |
| `webhook_events` | Every provider event received, with processing status (`pending`, `processed`, `failed`), attempts and last error. |

## Design decisions

| Decision | Why |
|---|---|
| Amounts as `int64` minor units (cents) | Floating point cannot represent money exactly. |
| Double-entry instead of a single balance column | Every cent is traceable; the ledger can always be audited and reconciled. |
| Idempotency key stored **in the same transaction** as the postings | The key and the money movement commit or roll back together, so a retry can never apply a transfer twice. Concurrent requests with the same key serialize on the primary key. |
| Accounts locked in sorted ID order | Two opposite transfers (A→B and B→A) would otherwise deadlock. |
| Receive fast, process later | Providers time out and retry; acknowledging in milliseconds and processing in a worker keeps the integration stable. |
| `FOR UPDATE SKIP LOCKED` | Several worker replicas can run in parallel without processing the same event. |
| Savepoint around each event | A failed posting is rolled back while the failed attempt is still recorded in the same transaction. |
| `transactions.reference` is `UNIQUE` | A second idempotency layer: the same provider event can never produce two deposits. |
| Seeds go through the domain services, not raw `INSERT`s | Seeded data obeys the same invariants as production data. |

## Quick start (Docker Compose)

Requirements: Docker, or Go 1.22+ with PostgreSQL 13+.

```bash
make up              # Postgres + API on :8080
```

Try it:

```bash
# create two accounts
A=$(curl -s -X POST localhost:8080/v1/accounts -d '{"currency":"BRL"}' | jq -r .id)
B=$(curl -s -X POST localhost:8080/v1/accounts -d '{"currency":"BRL"}' | jq -r .id)

# the provider confirms a R$100.00 payment into A (send it twice: credited once)
go run ./cmd/webhook-sim -account $A -amount 10000 -event evt_123
go run ./cmd/webhook-sim -account $A -amount 10000 -event evt_123

curl -s localhost:8080/v1/accounts/$A                 # balance: 10000

# transfer R$25.00; repeating the same key replays the original result
curl -s -X POST localhost:8080/v1/transfers \
  -H "Idempotency-Key: order-42" \
  -d "{\"from_account_id\":\"$A\",\"to_account_id\":\"$B\",\"amount\":2500,\"currency\":\"BRL\"}"

curl -s localhost:8080/v1/accounts/$A/entries         # account statement
```

`make down` stops everything and removes the database volume.

## API reference

Base URL: `http://localhost:8080`. All request and response bodies are JSON. Amounts are **integers in minor units** (`10000` = R$100.00). Currencies are 3-letter ISO 4217 codes (`BRL`, `USD`).

Every response carries an `X-Request-ID` header (generated, or echoed if the client sends one) that also appears in the logs.

Errors always have the same shape:

```json
{ "error": { "code": "insufficient_funds", "message": "insufficient funds" } }
```

| Code | Status | When |
|---|---|---|
| `invalid_json` | 400 | Malformed body or unknown fields |
| `validation_error` | 400 | Invalid currency, non-positive amount, same source and destination |
| `missing_idempotency_key` | 400 | `POST /v1/transfers` without `Idempotency-Key` (or longer than 255 chars) |
| `invalid_event` | 400 | Webhook body is not JSON or lacks `id`/`type` |
| `invalid_signature` | 401 | Missing, malformed, expired or wrong webhook signature |
| `account_not_found` | 404 | Unknown or malformed account ID |
| `unknown_provider` | 404 | Webhook path for a provider that is not configured |
| `body_too_large` | 413 | Body above 1 MiB |
| `insufficient_funds` | 422 | Source balance lower than the amount |
| `currency_mismatch` | 422 | Accounts and transfer use different currencies |
| `idempotency_key_reused` | 422 | Same key sent with a different payload |
| `not_ready` | 503 | Readiness check failed (database unreachable) |
| `internal_error` | 500 | Unexpected error (details only in the logs) |

### `POST /v1/accounts`

Creates an account.

```http
POST /v1/accounts
Content-Type: application/json

{ "currency": "BRL" }
```

`201 Created`

```json
{
  "id": "6f1c1d0e-8a2b-4a55-9f3e-2b9c7a1d4e10",
  "currency": "BRL",
  "balance": 0,
  "created_at": "2026-10-05T12:00:00Z"
}
```

Errors: `400 validation_error`, `400 invalid_json`.

### `GET /v1/accounts/{id}`

Returns the account with its current balance.

`200 OK`: same shape as above. Errors: `404 account_not_found`.

### `GET /v1/accounts/{id}/entries`

Account statement: the latest 50 ledger entries, newest first. Debits are negative, credits positive.

`200 OK`

```json
{
  "data": [
    { "transaction_id": "b2…", "kind": "transfer", "amount": -2500, "currency": "BRL", "created_at": "2026-10-05T12:05:00Z" },
    { "transaction_id": "a1…", "kind": "deposit",  "amount": 10000, "currency": "BRL", "created_at": "2026-10-05T12:01:00Z" }
  ]
}
```

Errors: `404 account_not_found`.

### `POST /v1/transfers`

Moves money between two accounts of the same currency. **Requires** an `Idempotency-Key` header: use one unique key per business operation (e.g. an order ID) and reuse it when retrying.

```http
POST /v1/transfers
Content-Type: application/json
Idempotency-Key: order-42

{
  "from_account_id": "6f1c1d0e-8a2b-4a55-9f3e-2b9c7a1d4e10",
  "to_account_id":   "0d9e8c7b-6a5f-4e3d-2c1b-0a9f8e7d6c5b",
  "amount": 2500,
  "currency": "BRL"
}
```

| Response | Meaning |
|---|---|
| `201 Created` | Transfer executed |
| `200 OK` + header `Idempotent-Replayed: true` | Key already used with the same payload: the original transfer is returned, no money moves |

```json
{
  "id": "b2c4…",
  "from_account_id": "6f1c1d0e-…",
  "to_account_id": "0d9e8c7b-…",
  "amount": 2500,
  "currency": "BRL",
  "created_at": "2026-10-05T12:05:00Z"
}
```

Errors: `400 missing_idempotency_key`, `400 validation_error`, `404 account_not_found`, `422 insufficient_funds`, `422 currency_mismatch`, `422 idempotency_key_reused`.

A transfer that fails (e.g. insufficient funds) does **not** consume its key, so the client can retry with the same key once the balance allows it.

### `POST /v1/webhooks/{provider}`

Receives an event from a payment provider. The default provider name is `acmepay` (configurable with `WEBHOOK_PROVIDER`).

```http
POST /v1/webhooks/acmepay
Content-Type: application/json
X-Signature: t=1759665600,v1=5f2b…

{
  "id": "evt_123",
  "type": "payment.succeeded",
  "data": { "account_id": "6f1c1d0e-…", "amount": 10000, "currency": "BRL" }
}
```

**Signature.** `v1` is the hex-encoded `HMAC-SHA256(WEBHOOK_SECRET, "<t>.<raw body>")`, where `t` is the Unix timestamp in seconds. Requests older or newer than 5 minutes are rejected. The signature must be computed over the exact bytes sent. `go run ./cmd/webhook-sim` and the Postman collection sign requests for you.

`202 Accepted`

```json
{ "received": true, "duplicate": false }
```

- `duplicate: true` means the event ID was already received; it is acknowledged (so the provider stops retrying) and not applied again.
- The response only means the event was stored. A background worker applies it about a second later.
- `payment.succeeded` credits the account. Any other event type is acknowledged and ignored, so the provider can add event types without breaking the integration.

Errors: `400 invalid_event`, `401 invalid_signature`, `404 unknown_provider`, `413 body_too_large`.

### Operational endpoints

| Method | Path | Response |
|---|---|---|
| `GET` | `/healthz` | `200 {"status":"ok"}`: the process is alive (Kubernetes liveness probe) |
| `GET` | `/readyz` | `200 {"status":"ready"}` or `503 not_ready`: database reachable (readiness probe) |
| `GET` | `/metrics` | Prometheus text format (see [Metrics](#metrics)) |

### Postman

A Postman collection with every request, automatic webhook signing and response tests lives in `docs/go-payments-ledger.postman_collection.json`. Import it, run the **1. Setup** folder first, then **2. Webhooks** and **3. Transfers**. `base_url` defaults to `http://localhost:8080`.

## Kubernetes

The `deploy/` folder runs the whole stack on a local Kubernetes cluster with [kind](https://kind.sigs.k8s.io/).

Requirements: Docker, `kubectl` and `kind` (`go install sigs.k8s.io/kind@v0.24.0`).

```bash
make k8s-up        # create the cluster, build the image, deploy everything
make k8s-forward   # API :8080, Prometheus :9090, Grafana :3000 (keep it running)
make load          # in another terminal: generate realistic traffic
```

```mermaid
flowchart LR
    subgraph ns[namespace: ledger]
        API1[api pod] & API2[api pod] --> PG[(postgres<br/>StatefulSet + PVC)]
        PROM[Prometheus] -- scrapes /metrics --> API1 & API2
        GRAF[Grafana] -- PromQL --> PROM
        SEED[seed Job] -. on demand .-> PG
    end
```

What the setup demonstrates:

| Piece | Why |
|---|---|
| 2 API replicas, each running the webhook worker | Horizontal scaling. `FOR UPDATE SKIP LOCKED` guarantees an event is processed by only one replica. |
| Migrations under a PostgreSQL advisory lock | Replicas starting together do not run DDL concurrently. |
| Liveness `/healthz` vs readiness `/readyz` | A pod that loses the database stops receiving traffic instead of being restarted in a loop. |
| `maxUnavailable: 0` rolling updates + graceful shutdown | Deploys (`make k8s-redeploy`) without dropped requests. |
| Prometheus Kubernetes service discovery | Scaling adds or removes scrape targets automatically. |
| Routes labelled by pattern (`/v1/accounts/{id}`) | Raw paths with IDs would explode metric cardinality. |
| Non-root (numeric UID 65532), read-only filesystem, dropped capabilities | Container hardening. The UID is numeric because Kubernetes cannot verify `runAsNonRoot` for a named user. |
| Seed as a one-off `Job` | Data loading runs inside the cluster with the same image and secrets as the API. |

Useful commands:

```bash
make k8s-status                                       # pods, services, volumes
kubectl -n ledger get pods -w                         # watch pods live
kubectl -n ledger logs -f -l app=api --prefix         # logs of every API replica
kubectl -n ledger scale deploy/api --replicas=4       # scale out
kubectl -n ledger delete pod -l app=api --wait=false  # self-healing: pods are recreated
make k8s-redeploy                                     # rebuild and roll out with zero downtime
make k8s-down                                         # delete the cluster
```

`kubectl port-forward` sticks to a single pod and stops when that pod is replaced. After restarting or scaling pods, run `make k8s-forward` again. To see traffic spread across replicas, use `make load` and the **Requests per pod** panel in Grafana.

## Observability: Grafana and Prometheus

With the cluster running and `make k8s-forward` open in a terminal:

| Tool | URL | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | No login (anonymous admin, local demo only). The **go-payments-ledger** dashboard opens as the home page; it is also under **Dashboards → go-payments-ledger**. |
| Prometheus | http://localhost:9090 | **Status → Targets** (`/targets`) should list one `ledger-api` target per API replica, all **UP**. |

The dashboard refreshes every 5 seconds. Run `make load` to see it move. Panels:

- **Requests per second**, **Error rate (5xx)**, **p95 latency**, **Webhook backlog**
- Requests and p95 latency by route, responses by status code
- Transfers by result: `created`, `replayed`, `insufficient_funds`…
- Webhooks received (new vs duplicate) and rejected (forged signatures)
- Webhook processing results: `processed`, `retry`, `failed`
- Requests, goroutines and heap per pod

If panels show **No data**, check the Prometheus targets page first, then make sure traffic is flowing (`make load`).

### Metrics

| Metric | Type | Labels |
|---|---|---|
| `http_requests_total` | counter | `method`, `route`, `status` |
| `http_request_duration_seconds` | histogram | `method`, `route` |
| `ledger_transfers_total` | counter | `result` (`created`, `replayed`, `insufficient_funds`…) |
| `webhook_events_received_total` | counter | `provider`, `duplicate` |
| `webhook_events_rejected_total` | counter | `provider`, `reason` |
| `webhook_events_processed_total` | counter | `result` (`processed`, `retry`, `failed`) |
| `webhook_events_pending` | gauge | — (backlog waiting for the worker) |
| `go_goroutines`, `go_memstats_heap_alloc_bytes` | gauge | — |

The metrics package is dependency-free (Prometheus text format written by hand); moving to the official `client_golang` only touches `internal/metrics`.

## Seeding demo data

`cmd/seed` populates the database with realistic data. It uses the same code paths as production (deposits arrive as webhooks and are applied by the worker, transfers go through the ledger service with idempotency keys) and ends with an integrity check.

Default run:

- 20 BRL accounts and 3 USD accounts, each funded by a provider webhook;
- every 5th webhook redelivered on purpose (deduplicated), plus one event for an unknown account (ends as `failed`);
- 150 transfers between BRL accounts, some retried with the same key (replayed) and some rejected for insufficient funds.

### On Kubernetes (seed Job)

```bash
make k8s-seed
```

This rebuilds the image, loads it into kind, recreates the `seed` Job (`deploy/k8s/jobs/seed-job.yaml`) and prints its logs. Change the Job `args` to seed different volumes.

### Against any database (local, Compose or port-forward)

```bash
make seed                                   # uses DATABASE_URL, default localhost:5432
make seed SEED_ARGS="-reset -rand-seed 42"  # wipe everything, then seed reproducible data
```

To reach the cluster database from your machine, open a port-forward first:

```bash
kubectl -n ledger port-forward pod/postgres-0 5432:5432
```

| Flag | Default | Description |
|---|---|---|
| `-dsn` | `$DATABASE_URL` or `localhost:5432` | PostgreSQL connection string |
| `-accounts` | `20` | Number of BRL accounts |
| `-usd-accounts` | `3` | Number of USD accounts |
| `-transfers` | `150` | Transfers to attempt |
| `-rand-seed` | current time | Fix it to reproduce the same data |
| `-reset` | `false` | **Deletes all data** before seeding |

Expected output:

```
accounts: 20 BRL, 3 USD
  webhooks stored                            24
  webhooks duplicated (ignored)              5
  transfers created                          129
  transfers replayed (idempotent)            11
  transfers rejected (insufficient funds)    21
webhook_events by status: failed=1 processed=23
integrity: unbalanced transactions=0, balances out of sync=0
```

The command exits with an error if the integrity check fails.

## Browsing the database

### Adminer in the cluster (web UI)

```bash
kubectl -n ledger run adminer --image=adminer --port=8080
kubectl -n ledger wait --for=condition=Ready pod/adminer --timeout=90s
kubectl -n ledger port-forward pod/adminer 8081:8080
```

Open http://localhost:8081 and log in:

| Field | Value |
|---|---|
| System | PostgreSQL |
| Server | `postgres` (the Service name inside the cluster) |
| Username / Password | `ledger` / `ledger` |
| Database | `ledger` |

Remove it afterwards with `kubectl -n ledger delete pod adminer`.

### psql inside the Postgres pod

```bash
kubectl -n ledger exec -it postgres-0 -- psql -U ledger -d ledger
```

### Desktop clients (DBeaver, VS Code extensions)

```bash
kubectl -n ledger port-forward pod/postgres-0 5432:5432
```

Connect to `localhost:5432`, database `ledger`, user `ledger`, password `ledger`. A browser cannot open this port: PostgreSQL does not speak HTTP.

### Queries worth running

```sql
-- every transaction is balanced (expected: no rows)
SELECT transaction_id, SUM(amount)
FROM ledger_entries
GROUP BY transaction_id
HAVING SUM(amount) <> 0;

-- cached balances match the ledger (expected: no rows)
SELECT a.id, a.balance, COALESCE(SUM(e.amount), 0) AS ledger_sum
FROM accounts a
LEFT JOIN ledger_entries e ON e.account_id = a.id
GROUP BY a.id, a.balance
HAVING a.balance <> COALESCE(SUM(e.amount), 0);

-- webhook pipeline status
SELECT status, COUNT(*) FROM webhook_events GROUP BY status;

-- failed events and why
SELECT event_id, attempts, last_error FROM webhook_events WHERE status = 'failed';

-- idempotency keys and the transactions they protected
SELECT key, transaction_id, created_at FROM idempotency_keys ORDER BY created_at DESC LIMIT 10;
```

## Testing

```bash
make test
docker compose up -d db
make test-integration
```

The integration suite covers money movement, insufficient funds, idempotent replay, key reuse, validation, webhook deduplication, signature rejection, metrics and readiness endpoints, and **25 concurrent transfers racing for the same balance** (exactly 10 succeed, the balance never goes negative).

CI runs vet, `gofmt`, the full test suite against PostgreSQL with the race detector, and validates the Kubernetes manifests with `kubeconform`.

## Configuration and Make targets

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable` | PostgreSQL connection string |
| `WEBHOOK_SECRET` | — (required) | Secret used to verify webhook signatures |
| `WEBHOOK_PROVIDER` | `acmepay` | Provider name in `/v1/webhooks/{provider}` |
| `PORT` | `8080` | HTTP port |

| Target | Description |
|---|---|
| `make up` / `make down` | Start / stop Postgres + API with Docker Compose |
| `make run` | Run the API locally (needs a database) |
| `make test` / `make test-integration` | Unit tests / full suite against Postgres |
| `make lint` | `go vet` and `gofmt` check |
| `make k8s-up` / `make k8s-down` | Create / delete the kind cluster with everything deployed |
| `make k8s-redeploy` | Rebuild the image and roll out the API |
| `make k8s-forward` | Port-forward API, Prometheus and Grafana |
| `make k8s-status` | Pods, services and volumes |
| `make load` | Generate realistic traffic for 5 minutes |
| `make seed` / `make k8s-seed` | Seed data locally / as a Kubernetes Job |

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `curl: Failed to connect to localhost port 8080` | `make k8s-forward` is not running, or dropped after pods restarted. Run it again in its own terminal. |
| API pods in `CreateContainerConfigError` | The pod spec must set a numeric `runAsUser` (65532) for the distroless image. Check `kubectl -n ledger describe pod -l app=api`. |
| `permission denied ... docker.sock` | Add your user to the `docker` group (`sudo usermod -aG docker $USER`) and start a new session (on WSL: `wsl --shutdown`). |
| Postman/browser on Windows cannot reach a port-forward in WSL | Port-forward with `--address 0.0.0.0` and use the IP from `hostname -I`. |
| `address already in use` on a port-forward | Another process (Docker Compose, a local Postgres or a previous port-forward) is using the port. |
| Grafana panels show **No data** | Check http://localhost:9090/targets and generate traffic with `make load`. |

## Project structure

```
cmd/api/              service entrypoint (config, wiring, graceful shutdown)
cmd/webhook-sim/      CLI that simulates a provider sending signed webhooks
cmd/loadgen/          traffic generator for demos and dashboards
cmd/seed/             demo data seeding with integrity check
internal/ledger/      domain: accounts, double-entry postings, idempotent transfers
internal/webhook/     signature verification, event store, async worker
internal/httpapi/     routes, handlers, error mapping, middleware
internal/database/    connection and embedded SQL migrations
internal/metrics/     Prometheus metrics (counters, histograms, gauges)
deploy/               kind cluster config and Kubernetes manifests (kustomize)
deploy/k8s/jobs/      on-demand Jobs (seed)
docs/                 Postman collection
```

## Roadmap

- [x] Prometheus metrics and Grafana dashboard
- [x] Kubernetes manifests (kind) with probes, rolling updates and service discovery
- [x] Seed command and Kubernetes Job
- [ ] Publish ledger events to a message broker (transactional outbox → SQS/Kafka)
- [ ] OpenTelemetry tracing and centralized logs (Loki)
- [ ] Horizontal Pod Autoscaler based on request rate
- [ ] Expiration (TTL) for idempotency keys
- [ ] OpenAPI specification
- [ ] Refund event (`payment.refunded`) and reversal transactions
- [ ] API client authentication
