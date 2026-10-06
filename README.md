# go-payments-ledger

A payments microservice in Go: a **double-entry ledger** with **idempotent transfers**, **payouts through an external provider** that survive crashes without paying twice, and a **secure webhook receiver** that processes provider events asynchronously, with retries. It runs locally with Docker Compose or on Kubernetes, with Prometheus metrics and a Grafana dashboard.

![CI](https://github.com/marcosnobre26/go-payments-ledger/actions/workflows/ci.yml/badge.svg)

## Contents

- [Purpose and context](#purpose-and-context)
- [Features](#features)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Payouts: calling an external provider safely](#payouts-calling-an-external-provider-safely)
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
- **Never charge twice when an external provider is involved.** A process can crash after the provider moved the money but before the database commit. The system must still know what happened and settle it exactly once.
- **Never trust an unsigned request.** Anyone can call a public webhook URL; only events signed by the provider may touch balances.
- **Never overdraw an account under concurrency.** Two simultaneous transfers from the same account must not both succeed if the balance only covers one.
- **Always be able to explain every cent.** Balances must be auditable and reconcilable, which is why financial systems use double-entry bookkeeping.

This project implements those guarantees end to end in a deliberately small domain, so each technique is easy to read, test and explain:

- an **account ledger** where deposits and transfers are recorded as balanced double entries;
- a **webhook pipeline** that verifies, deduplicates and asynchronously applies `payment.succeeded` events from a payment provider (simulated as `acmepay`);
- an **idempotent transfer API** that is safe to retry;
- **payouts** that send money out through a (simulated) payment provider, recorded before the provider is called and reconciled until the provider confirms the outcome;
- the **operational layer** expected in production: metrics, dashboards, health probes, graceful shutdown and a Kubernetes deployment with multiple replicas.

Out of scope on purpose: authentication/authorization of API clients, currency conversion, refunds of deposits and multi-provider configuration. They are natural extensions (see the [Roadmap](#roadmap)) but would not add new ideas to the core problems above.

## Features

- **Double-entry ledger.** Every movement is a transaction whose entries sum to zero. Balances are cached on the account and updated in the same database transaction.
- **Idempotent transfers.** `POST /v1/transfers` requires an `Idempotency-Key`. A retried request returns the original result (`Idempotent-Replayed: true`); reusing a key with a different payload is rejected.
- **Crash-safe payouts.** Funds are reserved and the payout is committed as `pending` *before* calling the provider; the provider receives the payout ID as its own idempotency key; webhooks and a reconciler drive every payout to `paid` or `failed`. An unknown outcome is never treated as a failure.
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
    C -- Idempotency-Key --> PO[POST /v1/payouts]
    PO -- reserve + pending, one DB transaction --> L
    PO -- payout ID as idempotency key --> PR[Payout provider]
    PR -- payout.paid / payout.failed --> R
    RC[Reconciler] -- resubmit / poll status --> PR
```

### Data model

| Table | Purpose |
|---|---|
| `accounts` | Customer accounts (one currency each) and two system accounts per currency: *settlement* (counterpart of money entering or leaving the platform) and *clearing* (funds reserved for payouts in flight). Holds the cached `balance` in minor units. |
| `transactions` | One row per money movement (`transfer`, `deposit`, `payout_reserve`, `payout_settle`, `payout_reverse`). `reference` is `UNIQUE`. |
| `ledger_entries` | The double entries: a negative entry on the source account and a positive one on the destination. They always sum to zero per transaction. |
| `idempotency_keys` | Request fingerprint and resulting transaction for every `Idempotency-Key`. |
| `payouts` | Every payout with its status (`pending`, `submitted`, `paid`, `failed`), client idempotency key, provider reference, attempts and when the reconciler should look at it again. |
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
| Payout committed as `pending` before calling the provider | The intent and the reserved funds are durable before any external side effect. |
| Payout ID sent as the provider's idempotency key | Resubmitting after a crash or timeout returns the existing payout instead of paying again. |
| Unknown provider outcomes stay in flight | Only an explicit rejection releases funds; a timeout or 5xx might have paid. |

## Payouts: calling an external provider safely

Transfers only move money inside the ledger, so one database transaction is enough to make them idempotent. A **payout** is different: the money leaves the platform through an external provider, and no database transaction can include the provider's side effect. The classic failure:

1. the service calls the provider;
2. the provider pays;
3. the service crashes (or the response times out) **before** recording anything.

If nothing was recorded first, the system has no trace of the payout, and a retry would pay a second time.

### The flow

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant DB as PostgreSQL
    participant P as Provider
    C->>A: POST /v1/payouts (Idempotency-Key)
    A->>DB: BEGIN: insert payout 'pending' + reserve funds (account -> clearing): COMMIT
    A->>P: create payout (Idempotency-Key = payout ID)
    alt provider accepts
        P-->>A: 201 processing
        A->>DB: status 'submitted'
    else provider rejects
        P-->>A: 422
        A->>DB: status 'failed' + release funds (clearing -> account)
    else timeout / 5xx / crash
        A->>DB: stays 'pending' (outcome unknown)
    end
    A-->>C: 202 Accepted (current status)
    P-)A: webhook payout.paid / payout.failed
    A->>DB: 'paid' (clearing -> settlement) or 'failed' (clearing -> account)
```

1. **Commit first.** In one database transaction, the payout is inserted as `pending` with the client's `Idempotency-Key`, and the funds move from the account to the **clearing** account. Insufficient funds or an unknown account roll everything back, and the key is not consumed.
2. **Call the provider with a stable key.** The payout ID is sent as the provider's own `Idempotency-Key`. If the same payout is submitted again, the provider returns the payout it already has.
3. **Settle from the provider, never from a guess.**
   - `payout.paid` (webhook, or the provider's response) moves the funds from clearing to **settlement**: the money has left the platform.
   - `payout.failed`, or a synchronous rejection (`4xx`), returns the funds to the account.
   - A timeout, a `5xx` or a crash leaves the payout `pending`. It might have been paid, so the funds stay reserved.

### States

```mermaid
stateDiagram-v2
    [*] --> pending: funds reserved
    pending --> submitted: provider accepted
    pending --> failed: provider rejected
    pending --> paid: webhook arrived before the response was recorded
    submitted --> paid: webhook or reconciler
    submitted --> failed: webhook or reconciler
    paid --> [*]
    failed --> [*]
```

`paid` and `failed` are final. A late or duplicated event for a final payout is a no-op; a contradicting one (`failed` after `paid`) is rejected and kept as a failed webhook event for investigation.

### Reconciliation

Every API replica runs a reconciler every 5 seconds. It takes a short **lease** on each due payout (`FOR UPDATE SKIP LOCKED`), so replicas never work on the same payout at the same time:

| Situation | What the reconciler does |
|---|---|
| `pending` (the provider call failed or the process died after the commit) | Resubmits with the **same** idempotency key, with exponential backoff. If the provider had already created the payout, it returns it, and nothing is paid twice. |
| `submitted` for longer than `PAYOUT_STALE_AFTER` (webhook lost or delayed) | Asks the provider for the status (`GET /v1/payouts/{id}`) and settles it. |

### Failure scenarios

| What goes wrong | Result |
|---|---|
| Crash right after the commit, before calling the provider | Payout stays `pending`; the reconciler submits it. |
| Provider pays, but its response is lost (timeout/5xx) | Stays `pending`; the resubmission returns the existing payout; the webhook or a status poll settles it. Paid once. |
| Provider's webhook never arrives | After `PAYOUT_STALE_AFTER`, the reconciler polls the provider. |
| Webhook delivered twice | Deduplicated by event ID; settling is idempotent anyway. |
| Webhook arrives before the API recorded `submitted` | Settles directly from `pending`. |
| Client retries `POST /v1/payouts` | Same `Idempotency-Key` returns the same payout (`200`, `Idempotent-Replayed: true`). |
| Provider rejects (invalid destination) | `failed`, funds returned to the account immediately. |

All of these are covered by integration tests, including the worst case: response **and** webhook lost.

### The simulated provider

`cmd/fakeprovider` plays the external provider (it is deployed with Docker Compose and Kubernetes). It is idempotent on `Idempotency-Key`, reports outcomes by signed webhook after `WEBHOOK_DELAY`, exposes `GET /v1/payouts/{id}` and `GET /stats` (payouts created vs. submit calls), and injects the failures above:

| Variable | Default | Effect |
|---|---|---|
| `FAIL_AFTER_RECORD_RATE` | `0.1` | Share of new payouts that are recorded and then answered with `500` |
| `DROP_WEBHOOK_RATE` | `0.1` | Share of outcome webhooks that are never delivered |
| `WEBHOOK_DELAY` | `3s` | Time until the outcome is known |
| `API_WEBHOOK_URL` | — | Where webhooks are sent |

Destinations starting with `invalid` are rejected immediately (`422`); destinations starting with `reject` are accepted and fail later (`payout.failed`). Anything else is paid.

With failures injected, `GET /stats` on the provider shows more submit calls than payouts created: those are resubmissions that the idempotency key turned into no-ops.

## Quick start (Docker Compose)

Requirements: Docker, or Go 1.22+ with PostgreSQL 13+.

```bash
make up              # Postgres + API on :8080 + simulated payout provider on :8081
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

# pay R$15.00 out to an external destination
P=$(curl -s -X POST localhost:8080/v1/payouts \
  -H "Idempotency-Key: payout-1" \
  -d "{\"account_id\":\"$A\",\"amount\":1500,\"currency\":\"BRL\",\"destination\":\"pix:marcos@example.com\"}" | jq -r .id)

curl -s localhost:8080/v1/payouts/$P                  # pending/submitted, then paid a few seconds later
curl -s localhost:8081/stats                          # provider side: payouts created vs. submit calls
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
| `validation_error` | 400 | Invalid currency, non-positive amount, same source and destination, missing payout destination |
| `missing_idempotency_key` | 400 | `POST /v1/transfers` or `POST /v1/payouts` without `Idempotency-Key` (or longer than 255 chars) |
| `invalid_event` | 400 | Webhook body is not JSON or lacks `id`/`type` |
| `invalid_signature` | 401 | Missing, malformed, expired or wrong webhook signature |
| `account_not_found` | 404 | Unknown or malformed account ID |
| `payout_not_found` | 404 | Unknown or malformed payout ID |
| `unknown_provider` | 404 | Webhook path for a provider that is not configured |
| `body_too_large` | 413 | Body above 1 MiB |
| `insufficient_funds` | 422 | Source balance lower than the amount |
| `currency_mismatch` | 422 | Accounts and transfer use different currencies |
| `idempotency_key_reused` | 422 | Same key sent with a different payload |
| `not_ready` | 503 | Readiness check failed (database unreachable) |
| `payouts_disabled` | 503 | `PAYOUT_PROVIDER_URL` is not configured |
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

### `POST /v1/payouts`

Sends money from an account to an external destination through the payout provider. **Requires** an `Idempotency-Key` header. See [Payouts](#payouts-calling-an-external-provider-safely) for the full flow.

```http
POST /v1/payouts
Content-Type: application/json
Idempotency-Key: withdrawal-7731

{
  "account_id": "6f1c1d0e-8a2b-4a55-9f3e-2b9c7a1d4e10",
  "amount": 1500,
  "currency": "BRL",
  "destination": "pix:marcos@example.com"
}
```

| Response | Meaning |
|---|---|
| `202 Accepted` | Payout created and funds reserved. The final outcome arrives later. |
| `200 OK` + header `Idempotent-Replayed: true` | Key already used with the same payload: the existing payout is returned. |

```json
{
  "id": "c3d1…",
  "account_id": "6f1c1d0e-…",
  "amount": 1500,
  "currency": "BRL",
  "destination": "pix:marcos@example.com",
  "status": "submitted",
  "provider_ref": "po_8f2a…",
  "attempts": 1,
  "created_at": "2026-10-05T12:10:00Z",
  "updated_at": "2026-10-05T12:10:00Z"
}
```

`status` is `pending` (provider not confirmed yet; `last_error` shows why), `submitted` (accepted, waiting for the outcome), `paid` or `failed`. The account balance drops when the payout is created (funds reserved) and only comes back if the payout fails.

Errors: `400 missing_idempotency_key`, `400 validation_error`, `404 account_not_found`, `422 insufficient_funds`, `422 currency_mismatch`, `422 idempotency_key_reused`, `503 payouts_disabled`.

### `GET /v1/payouts/{id}`

Returns the payout and its current status. Errors: `404 payout_not_found`.

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
- `payment.succeeded` credits the account (`data.account_id`, `data.amount`, `data.currency`).
- `payout.paid` and `payout.failed` settle a payout (`data.payout_id`, `data.provider_payout_id`, `data.failure_reason`).
- Any other event type is acknowledged and ignored, so the provider can add event types without breaking the integration.

Errors: `400 invalid_event`, `401 invalid_signature`, `404 unknown_provider`, `413 body_too_large`.

### Operational endpoints

| Method | Path | Response |
|---|---|---|
| `GET` | `/healthz` | `200 {"status":"ok"}`: the process is alive (Kubernetes liveness probe) |
| `GET` | `/readyz` | `200 {"status":"ready"}` or `503 not_ready`: database reachable (readiness probe) |
| `GET` | `/metrics` | Prometheus text format (see [Metrics](#metrics)) |

### Postman

A Postman collection with every request, automatic webhook signing and response tests lives in `docs/go-payments-ledger.postman_collection.json`. Import it, run the **1. Setup** folder first, then **2. Webhooks**, **3. Transfers** and **4. Payouts**. `base_url` defaults to `http://localhost:8080`.

## Kubernetes

The `deploy/` folder runs the whole stack on a local Kubernetes cluster with [kind](https://kind.sigs.k8s.io/).

Requirements: Docker, `kubectl` and `kind` (`go install sigs.k8s.io/kind@v0.24.0`).

```bash
make k8s-up        # create the cluster, build the image, deploy everything
make k8s-forward   # API :8080, provider :8081, Prometheus :9090, Grafana :3000 (keep it running)
make load          # in another terminal: generate realistic traffic
```

```mermaid
flowchart LR
    subgraph ns[namespace: ledger]
        API1[api pod] & API2[api pod] --> PG[(postgres<br/>StatefulSet + PVC)]
        PROM[Prometheus] -- scrapes /metrics --> API1 & API2
        GRAF[Grafana] -- PromQL --> PROM
        SEED[seed Job] -. on demand .-> PG
        API1 & API2 -- payouts --> PRV[payout-provider]
        PRV -- webhooks --> API1 & API2
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
| `payout-provider` Deployment with failure injection | Shows payouts recovering from lost responses and lost webhooks while you watch the dashboard. |
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
- Payout lifecycle events (`reserved`, `submitted`, `submit_error`, `resubmitted`, `paid`, `failed`…) and payouts in flight

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
| `ledger_payouts_total` | counter | `event` (`reserved`, `submitted`, `submit_error`, `resubmitted`, `rejected`, `paid`, `failed`, `replayed`) |
| `payouts_in_flight` | gauge | — (`pending` + `submitted`; should return to zero) |
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

-- payouts by status, and the ones still in flight
SELECT status, COUNT(*) FROM payouts GROUP BY status;
SELECT id, status, attempts, last_error, next_attempt_at FROM payouts WHERE status IN ('pending', 'submitted');

-- the clearing account holds exactly the payouts in flight (expected: equal values)
SELECT (SELECT balance FROM accounts WHERE system_role = 'clearing' AND currency = 'BRL') AS clearing_balance,
       (SELECT COALESCE(SUM(amount), 0) FROM payouts WHERE status IN ('pending', 'submitted') AND currency = 'BRL') AS in_flight;

-- idempotency keys and the transactions they protected
SELECT key, transaction_id, created_at FROM idempotency_keys ORDER BY created_at DESC LIMIT 10;
```

## Testing

The suite has two layers:

- **Unit tests** have no external dependencies and always run.
- **Integration tests** run against a real PostgreSQL, because the guarantees this project cares about (locking, constraints, idempotency under concurrency) live in the database. They are skipped automatically when `TEST_DATABASE_URL` is not set.

```bash
make test                     # unit tests only (integration tests are skipped)

docker compose up -d db
make test-integration         # everything, against Postgres, with the race detector
```

Other useful commands:

```bash
go test -v -run TestVerify ./internal/webhook      # run a single test, verbose
go test -cover ./...                                # coverage per package
TEST_DATABASE_URL="postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable" \
  go test -cover -count=1 -p 1 ./...                # coverage including integration tests
```

`-p 1` runs packages one at a time, since they share the same database. On Windows, `-race` requires a C compiler; drop the flag there (CI on Linux keeps it).

### Unit tests

| File | What it verifies |
|---|---|
| `internal/webhook/signature_test.go` | Webhook signature verification: valid signature, tampered body, wrong secret, replay after the 5-minute tolerance, timestamp from the future, missing header, malformed header, missing `v1`. Also that retry backoff is exponential and capped. |
| `internal/metrics/metrics_test.go` | Prometheus exposition format: counters, cumulative histogram buckets, `_sum`/`_count`, label escaping, and a panic when a metric is used with the wrong number of labels. |

### Integration tests

| File | What it verifies |
|---|---|
| `internal/ledger/ledger_test.go` | A transfer moves the exact amount; insufficient funds is rejected without changing balances; the same idempotency key replays the original transfer and a key reused with a different payload is rejected; validation (zero/negative amount, same account, invalid currency, currency mismatch, unknown account); **25 concurrent transfers racing for the same balance**: exactly 10 succeed and the balance never goes negative. |
| `internal/payout/payout_test.go` | Payouts against the simulated provider: reservation then settlement by webhook; idempotent creation (one provider payout); provider rejection releases funds; `payout.failed` returns funds; **provider pays but both its response and its webhook are lost**: the payout stays reserved, the reconciler resubmits with the same key (still one provider payout), polls the status and settles it exactly once; insufficient funds does not consume the key. Every test checks the ledger stays balanced. |
| `internal/httpapi/server_test.go` | End to end over HTTP: a webhook delivered twice credits once (and the duplicate/processed metrics reflect it); a forged signature gets `401`; transfers require `Idempotency-Key`, return `201` then `200` with `Idempotent-Replayed: true`; `/healthz`, `/readyz` and `/metrics` respond, with routes labelled by pattern. |

`cmd/seed` adds one more safety net: after seeding it checks that every transaction is balanced and every cached balance matches the ledger, and exits with an error otherwise.

### CI

GitHub Actions runs on every push and pull request:

1. `go vet` and a `gofmt` check;
2. the full test suite against a PostgreSQL service container, with the race detector;
3. validation of the Kubernetes manifests with `kustomize` + `kubeconform`.

## Configuration and Make targets

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable` | PostgreSQL connection string |
| `WEBHOOK_SECRET` | — (required) | Secret used to verify webhook signatures |
| `WEBHOOK_PROVIDER` | `acmepay` | Provider name in `/v1/webhooks/{provider}` |
| `PORT` | `8080` | HTTP port |
| `PAYOUT_PROVIDER_URL` | — (payouts disabled) | Base URL of the payout provider |
| `PAYOUT_STALE_AFTER` | `2m` | How long a submitted payout waits for a webhook before the reconciler polls the provider |

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
cmd/fakeprovider/     simulated payout provider with failure injection
internal/ledger/      domain: accounts, double-entry postings, idempotent transfers
internal/payout/      payouts: reserve-then-call, provider client, reconciler
internal/fakeprovider/ simulated provider (idempotent, async webhooks)
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
- [x] Payouts through an external provider with reconciliation (crash-safe, never paid twice)
- [ ] Publish ledger events to a message broker (transactional outbox → SQS/Kafka)
- [ ] OpenTelemetry tracing and centralized logs (Loki)
- [ ] Horizontal Pod Autoscaler based on request rate
- [ ] Expiration (TTL) for idempotency keys
- [ ] Versioned migrations (e.g. golang-migrate) instead of idempotent scripts
- [ ] OpenAPI specification
- [ ] Refund event (`payment.refunded`) and reversal transactions
- [ ] API client authentication
