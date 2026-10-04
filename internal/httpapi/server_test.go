package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/httpapi"
	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/testutil"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

var secret = []byte("test-secret")

type env struct {
	srv    *httptest.Server
	ledger *ledger.Service
	worker *webhook.Worker
}

func setup(t *testing.T) env {
	db := testutil.DB(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l := ledger.NewService(db)
	api := httpapi.New(l, webhook.NewStore(db), map[string][]byte{"acmepay": secret}, log)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return env{srv: srv, ledger: l, worker: webhook.NewWorker(db, l, log)}
}

func sendWebhook(t *testing.T, e env, sign []byte, eventID, accountID string, amount int64) *http.Response {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"id": eventID, "type": "payment.succeeded",
		"data": map[string]any{"account_id": accountID, "amount": amount, "currency": "BRL"},
	})
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/webhooks/acmepay", bytes.NewReader(body))
	req.Header.Set(webhook.SignatureHeader, webhook.SignatureHeaderValue(sign, time.Now().Unix(), body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func drain(t *testing.T, w *webhook.Worker) {
	t.Helper()
	for {
		processed, err := w.ProcessNext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			return
		}
	}
}

func TestWebhookDeliveredTwiceCreditsOnce(t *testing.T) {
	e := setup(t)
	acc, err := e.ledger.CreateAccount(context.Background(), "BRL")
	if err != nil {
		t.Fatal(err)
	}
	eventID := fmt.Sprintf("evt_%d", time.Now().UnixNano())

	first := sendWebhook(t, e, secret, eventID, acc.ID, 5_000)
	second := sendWebhook(t, e, secret, eventID, acc.ID, 5_000)
	if first.StatusCode != http.StatusAccepted || second.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d / %d, want 202 / 202", first.StatusCode, second.StatusCode)
	}
	var body struct{ Duplicate bool }
	_ = json.NewDecoder(second.Body).Decode(&body)
	if !body.Duplicate {
		t.Fatal("second delivery should be flagged as duplicate")
	}

	drain(t, e.worker)

	got, _ := e.ledger.GetAccount(context.Background(), acc.ID)
	if got.Balance != 5_000 {
		t.Fatalf("balance = %d, want 5000 (credited once)", got.Balance)
	}
}

func TestWebhookWithInvalidSignatureIsRejected(t *testing.T) {
	e := setup(t)
	resp := sendWebhook(t, e, []byte("attacker-secret"), "evt_forged", "00000000-0000-0000-0000-000000000000", 1)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestTransferEndpointRequiresIdempotencyKeyAndReplays(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	from, _ := e.ledger.CreateAccount(ctx, "BRL")
	to, _ := e.ledger.CreateAccount(ctx, "BRL")
	if err := e.ledger.Deposit(ctx, from.ID, 1_000, "BRL", "seed:"+from.ID); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":250,"currency":"BRL"}`, from.ID, to.ID)

	post := func(key string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/transfers", bytes.NewBufferString(body))
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	if resp := post(""); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("without key: status = %d, want 400", resp.StatusCode)
	}
	k := fmt.Sprintf("key-%d", time.Now().UnixNano())
	if resp := post(k); resp.StatusCode != http.StatusCreated {
		t.Fatalf("first: status = %d, want 201", resp.StatusCode)
	}
	resp := post(k)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry: status = %d replayed=%q, want 200/true", resp.StatusCode, resp.Header.Get("Idempotent-Replayed"))
	}
	got, _ := e.ledger.GetAccount(ctx, from.ID)
	if got.Balance != 750 {
		t.Fatalf("balance = %d, want 750", got.Balance)
	}
}
