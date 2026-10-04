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
