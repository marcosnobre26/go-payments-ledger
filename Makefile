TEST_DATABASE_URL ?= postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable

.PHONY: up down run test test-integration lint

up:                ## start Postgres + API with Docker Compose
	docker compose up --build

down:
	docker compose down -v

run:               ## run the API locally (needs a database)
	WEBHOOK_SECRET=dev-secret go run ./cmd/api

test:              ## unit tests (integration tests are skipped)
	go test ./...

test-integration:  ## all tests against Postgres (docker compose up db)
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -p 1 ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

# ---- Kubernetes (kind) ----
CLUSTER ?= ledger
IMAGE   ?= go-payments-ledger:dev

.PHONY: k8s-up k8s-redeploy k8s-forward k8s-status k8s-down load

k8s-up:            ## create a local cluster, build the image and deploy everything
	kind get clusters | grep -qx $(CLUSTER) || kind create cluster --config deploy/kind-config.yaml
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	kubectl apply -k deploy/k8s
	kubectl -n ledger rollout status statefulset/postgres --timeout=180s
	kubectl -n ledger rollout status deployment/api --timeout=180s
	kubectl -n ledger rollout status deployment/payout-provider --timeout=180s
	kubectl -n ledger rollout status deployment/prometheus --timeout=180s
	kubectl -n ledger rollout status deployment/grafana --timeout=180s

k8s-redeploy:      ## rebuild the image and roll out the new version (zero downtime)
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	kubectl -n ledger rollout restart deployment/api deployment/payout-provider
	kubectl -n ledger rollout status deployment/api
	kubectl -n ledger rollout status deployment/payout-provider

k8s-forward:       ## API :8080, provider :8081, Prometheus :9090, Grafana :3000 (Ctrl+C stops)
	kubectl -n ledger port-forward svc/api 8080:80 & \
	kubectl -n ledger port-forward svc/payout-provider 8081:80 & \
	kubectl -n ledger port-forward svc/prometheus 9090:9090 & \
	kubectl -n ledger port-forward svc/grafana 3000:3000 & \
	wait

k8s-status:
	kubectl -n ledger get pods,svc,pvc

k8s-down:          ## delete the local cluster
	kind delete cluster --name $(CLUSTER)

load:              ## generate realistic traffic for 5 minutes
	go run ./cmd/loadgen -duration 5m

# ---- Seeds ----
SEED_ARGS ?=

.PHONY: seed k8s-seed

seed:              ## seed the DB at DATABASE_URL (default localhost:5432). SEED_ARGS=-reset wipes it first
	go run ./cmd/seed $(SEED_ARGS)

k8s-seed:          ## rebuild the image and run the seed as a Kubernetes Job
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	kubectl -n ledger delete job seed --ignore-not-found
	kubectl apply -f deploy/k8s/jobs/seed-job.yaml
	kubectl -n ledger wait --for=condition=complete job/seed --timeout=120s || (kubectl -n ledger logs job/seed; exit 1)
	kubectl -n ledger logs job/seed
