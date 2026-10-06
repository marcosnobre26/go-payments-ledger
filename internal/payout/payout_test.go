package payout_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/fakeprovider"
	"github.com/marcosnobre26/go-payments-ledger/internal/httpapi"
	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
	"github.com/marcosnobre26/go-payments-ledger/internal/payout"
	"github.com/marcosnobre26/go-payments-ledger/internal/testutil"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

var secret = []byte("test-secret")

type env struct {
	db       *sql.DB
	ledger   *ledger.Service
	payouts  *payout.Service
	worker   *webhook.Worker
	provider *fakeprovider.Server
}

func setup(t *testing.T, webhooks bool) env {
	t.Helper()
	db := testutil.DB(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	l := ledger.NewService(db)

	apiSrv := httptest.NewUnstartedServer(nil)
	opts := fakeprovider.Options{Secret: secret, WebhookDelay: 20 * time.Millisecond, Logger: log}
	if webhooks {
		opts.WebhookURL = "http://" + apiSrv.Listener.Addr().String() + "/v1/webhooks/acmepay"
	}
	prov := fakeprovider.New(opts)
	provSrv := httptest.NewServer(prov.Handler())
	t.Cleanup(provSrv.Close)

	p := payout.NewService(db, payout.NewHTTPProvider(provSrv.URL), m, log)
	p.StaleAfter = time.Hour
	w := webhook.NewWorker(db, l, m, log).WithPayouts(p)

	apiSrv.Config.Handler = httpapi.New(httpapi.Deps{
		Ledger: l, Webhooks: webhook.NewStore(db), WebhookSecrets: map[string][]byte{"acmepay": secret},
		Payouts: p, Metrics: m, Logger: log,
	}).Handler()
	apiSrv.Start()
	t.Cleanup(apiSrv.Close)

	return env{db: db, ledger: l, payouts: p, worker: w, provider: prov}
}

func fundedAccount(t *testing.T, e env, amount int64) ledger.Account {
	t.Helper()
	acc, err := e.ledger.CreateAccount(context.Background(), "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ledger.Deposit(context.Background(), acc.ID, amount, "BRL", "test:"+acc.ID); err != nil {
		t.Fatal(err)
	}
	return acc
}

func balance(t *testing.T, e env, id string) int64 {
	t.Helper()
	acc, err := e.ledger.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return acc.Balance
}

func key(t *testing.T) string { return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()) }

func waitForWebhook(t *testing.T, e env) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	for {
		processed, err := e.worker.ProcessNext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			return
		}
	}
}

func status(t *testing.T, e env, id string) string {
	t.Helper()
	p, err := e.payouts.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p.Status
}

func makeDue(t *testing.T, e env, id string) {
	t.Helper()
	if _, err := e.db.Exec(`UPDATE payouts SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func assertLedgerBalanced(t *testing.T, e env) {
	t.Helper()
	var unbalanced int
	if err := e.db.QueryRow(`
		SELECT count(*) FROM (SELECT transaction_id FROM ledger_entries
		GROUP BY transaction_id HAVING SUM(amount) <> 0) t`).Scan(&unbalanced); err != nil {
		t.Fatal(err)
	}
	if unbalanced != 0 {
		t.Fatalf("%d unbalanced transactions", unbalanced)
	}
}

func TestPayoutReservesThenSettlesFromWebhook(t *testing.T) {
	e := setup(t, true)
	acc := fundedAccount(t, e, 10_000)

	p, replayed, err := e.payouts.Create(context.Background(), key(t), payout.CreateInput{
		AccountID: acc.ID, Amount: 3_000, Currency: "BRL", Destination: "pix:marcos@example.com",
	})
	if err != nil || replayed {
		t.Fatalf("create: err=%v replayed=%v", err, replayed)
	}
	if p.Status != payout.StatusSubmitted || p.ProviderRef == nil {
		t.Fatalf("status = %s, provider_ref = %v; want submitted with a provider reference", p.Status, p.ProviderRef)
	}
	if got := balance(t, e, acc.ID); got != 7_000 {
		t.Fatalf("balance after reservation = %d, want 7000", got)
	}

	waitForWebhook(t, e)

	if s := status(t, e, p.ID); s != payout.StatusPaid {
		t.Fatalf("status after webhook = %s, want paid", s)
	}
	if got := balance(t, e, acc.ID); got != 7_000 {
		t.Fatalf("balance after settlement = %d, want 7000", got)
	}
	assertLedgerBalanced(t, e)
}

func TestPayoutCreateIsIdempotent(t *testing.T) {
	e := setup(t, false)
	acc := fundedAccount(t, e, 10_000)
	in := payout.CreateInput{AccountID: acc.ID, Amount: 2_000, Currency: "BRL", Destination: "pix:a@b.c"}
	k := key(t)

	first, _, err := e.payouts.Create(context.Background(), k, in)
	if err != nil {
		t.Fatal(err)
	}
	second, replayed, err := e.payouts.Create(context.Background(), k, in)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed || second.ID != first.ID {
		t.Fatalf("expected replay of %s, got %s (replayed=%v)", first.ID, second.ID, replayed)
	}
	if got := balance(t, e, acc.ID); got != 8_000 {
		t.Fatalf("funds reserved twice: balance = %d", got)
	}
	if n := e.provider.Payouts(); n != 1 {
		t.Fatalf("provider created %d payouts, want 1", n)
	}

	in.Amount = 9_999
	if _, _, err := e.payouts.Create(context.Background(), k, in); !errors.Is(err, ledger.ErrIdempotencyKeyReused) {
		t.Fatalf("got %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestPayoutRejectedByProviderReleasesFunds(t *testing.T) {
	e := setup(t, false)
	acc := fundedAccount(t, e, 5_000)

	p, _, err := e.payouts.Create(context.Background(), key(t), payout.CreateInput{
		AccountID: acc.ID, Amount: 1_500, Currency: "BRL", Destination: "invalid-iban",
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != payout.StatusFailed {
		t.Fatalf("status = %s, want failed", p.Status)
	}
	if got := balance(t, e, acc.ID); got != 5_000 {
		t.Fatalf("balance = %d, want 5000 (reservation released)", got)
	}
	assertLedgerBalanced(t, e)
}

func TestPayoutFailedWebhookReversesReservation(t *testing.T) {
	e := setup(t, true)
	acc := fundedAccount(t, e, 5_000)

	p, _, err := e.payouts.Create(context.Background(), key(t), payout.CreateInput{
		AccountID: acc.ID, Amount: 1_000, Currency: "BRL", Destination: "reject-closed-account",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForWebhook(t, e)

	if s := status(t, e, p.ID); s != payout.StatusFailed {
		t.Fatalf("status = %s, want failed", s)
	}
	if got := balance(t, e, acc.ID); got != 5_000 {
		t.Fatalf("balance = %d, want 5000 (funds returned)", got)
	}
	assertLedgerBalanced(t, e)
}

func TestPayoutChargedButResponseAndWebhookLost(t *testing.T) {
	e := setup(t, false)
	acc := fundedAccount(t, e, 10_000)
	e.provider.FailNextAfterRecord(1)

	p, _, err := e.payouts.Create(context.Background(), key(t), payout.CreateInput{
		AccountID: acc.ID, Amount: 4_000, Currency: "BRL", Destination: "pix:x@y.z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != payout.StatusPending || p.LastError == nil {
		t.Fatalf("status = %s, want pending with the provider error recorded", p.Status)
	}
	if n := e.provider.Payouts(); n != 1 {
		t.Fatalf("provider should already hold the payout, has %d", n)
	}
	if got := balance(t, e, acc.ID); got != 6_000 {
		t.Fatalf("funds must stay reserved while the outcome is unknown: balance = %d", got)
	}

	makeDue(t, e, p.ID)
	if _, err := e.payouts.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := e.provider.Payouts(); n != 1 {
		t.Fatalf("resubmission created a second payout at the provider (%d)", n)
	}
	if s := status(t, e, p.ID); s != payout.StatusSubmitted {
		t.Fatalf("status after resubmit = %s, want submitted", s)
	}

	time.Sleep(100 * time.Millisecond)
	makeDue(t, e, p.ID)
	if _, err := e.payouts.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := status(t, e, p.ID); s != payout.StatusPaid {
		t.Fatalf("status after polling = %s, want paid", s)
	}
	if got := balance(t, e, acc.ID); got != 6_000 {
		t.Fatalf("balance = %d, want 6000 (paid exactly once)", got)
	}
	assertLedgerBalanced(t, e)
}

func TestPayoutWithInsufficientFundsDoesNotConsumeKey(t *testing.T) {
	e := setup(t, false)
	acc := fundedAccount(t, e, 1_000)
	k := key(t)
	in := payout.CreateInput{AccountID: acc.ID, Amount: 5_000, Currency: "BRL", Destination: "pix:a@b.c"}

	if _, _, err := e.payouts.Create(context.Background(), k, in); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("got %v, want ErrInsufficientFunds", err)
	}
	if n := e.provider.Payouts(); n != 0 {
		t.Fatalf("provider must not be called, got %d payouts", n)
	}

	if err := e.ledger.Deposit(context.Background(), acc.ID, 10_000, "BRL", "topup:"+acc.ID); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := e.payouts.Create(context.Background(), k, in); err != nil || replayed {
		t.Fatalf("retry with same key: err=%v replayed=%v", err, replayed)
	}
}
