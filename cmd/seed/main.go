package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/database"
	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "seed failed:", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := flag.String("dsn", envOr("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"), "PostgreSQL connection string")
	brlAccounts := flag.Int("accounts", 20, "number of BRL accounts")
	usdAccounts := flag.Int("usd-accounts", 3, "number of USD accounts")
	transfers := flag.Int("transfers", 150, "number of transfers to attempt")
	randSeed := flag.Int64("rand-seed", time.Now().UnixNano(), "random seed (fix it to reproduce the same data)")
	reset := flag.Bool("reset", false, "DELETE ALL DATA before seeding")
	flag.Parse()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(*randSeed))
	runID := fmt.Sprintf("%x", rng.Int63())[:8]

	db, err := database.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := database.Migrate(ctx, db); err != nil {
		return err
	}

	if *reset {
		fmt.Println("reset: truncating all tables")
		if _, err := db.ExecContext(ctx, `
			TRUNCATE payouts, idempotency_keys, ledger_entries, transactions, webhook_events, accounts
			RESTART IDENTITY CASCADE`); err != nil {
			return fmt.Errorf("reset: %w", err)
		}
	}

	svc := ledger.NewService(db)
	store := webhook.NewStore(db)
	worker := webhook.NewWorker(db, svc, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	brl, err := createAccounts(ctx, svc, "BRL", *brlAccounts)
	if err != nil {
		return err
	}
	usd, err := createAccounts(ctx, svc, "USD", *usdAccounts)
	if err != nil {
		return err
	}
	fmt.Printf("accounts: %d BRL, %d USD\n", len(brl), len(usd))

	stats := map[string]int{}
	saveEvent := func(eventID, accountID string, amount int64, currency string) error {
		var ev webhook.Event
		ev.ID, ev.Type = eventID, webhook.EventPaymentSucceeded
		ev.Data.AccountID, ev.Data.Amount, ev.Data.Currency = accountID, amount, currency
		raw, _ := json.Marshal(ev)
		dup, err := store.Save(ctx, "acmepay", ev, raw)
		if dup {
			stats["webhooks duplicated (ignored)"]++
		} else {
			stats["webhooks stored"]++
		}
		return err
	}

	for i, id := range append(append([]string{}, brl...), usd...) {
		currency := "BRL"
		if i >= len(brl) {
			currency = "USD"
		}
		eventID := fmt.Sprintf("evt_seed_%s_%03d", runID, i)
		amount := int64(50_000 + rng.Intn(450_000))
		if err := saveEvent(eventID, id, amount, currency); err != nil {
			return err
		}

		if i%5 == 0 {
			if err := saveEvent(eventID, id, amount, currency); err != nil {
				return err
			}
		}
	}

	if err := saveEvent("evt_seed_"+runID+"_orphan", "00000000-0000-0000-0000-000000000000", 10_000, "BRL"); err != nil {
		return err
	}

	for {
		processed, err := worker.ProcessNext(ctx)
		if err != nil {
			return err
		}
		if !processed {
			break
		}
	}

	for i := 0; i < *transfers; i++ {
		from, to := pickTwo(rng, brl)
		amount := int64(500 + rng.Intn(60_000))
		if rng.Intn(100) < 8 {
			amount = 100_000_000
		}
		key := fmt.Sprintf("seed-%s-%04d", runID, i)
		in := ledger.TransferInput{FromAccountID: from, ToAccountID: to, Amount: amount, Currency: "BRL"}

		_, _, err := svc.Transfer(ctx, key, in)
		switch {
		case err == nil:
			stats["transfers created"]++
		case errors.Is(err, ledger.ErrInsufficientFunds):
			stats["transfers rejected (insufficient funds)"]++
			continue
		default:
			return fmt.Errorf("transfer %s: %w", key, err)
		}

		if rng.Intn(100) < 10 {
			if _, replayed, err := svc.Transfer(ctx, key, in); err == nil && replayed {
				stats["transfers replayed (idempotent)"]++
			}
		}
	}

	for _, k := range []string{
		"webhooks stored", "webhooks duplicated (ignored)",
		"transfers created", "transfers replayed (idempotent)", "transfers rejected (insufficient funds)",
	} {
		fmt.Printf("  %-42s %d\n", k, stats[k])
	}
	return printIntegrity(ctx, db)
}

func createAccounts(ctx context.Context, svc *ledger.Service, currency string, n int) ([]string, error) {
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		acc, err := svc.CreateAccount(ctx, currency)
		if err != nil {
			return nil, err
		}
		ids = append(ids, acc.ID)
	}
	return ids, nil
}

func pickTwo(rng *rand.Rand, ids []string) (string, string) {
	a := rng.Intn(len(ids))
	b := rng.Intn(len(ids) - 1)
	if b >= a {
		b++
	}
	return ids[a], ids[b]
}

func printIntegrity(ctx context.Context, db *sql.DB) error {
	var unbalanced, driftedBalances int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT transaction_id FROM ledger_entries
			GROUP BY transaction_id HAVING SUM(amount) <> 0
		) t`).Scan(&unbalanced); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT a.id FROM accounts a
			LEFT JOIN ledger_entries e ON e.account_id = a.id
			GROUP BY a.id, a.balance
			HAVING a.balance <> COALESCE(SUM(e.amount), 0)
		) t`).Scan(&driftedBalances); err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, `SELECT status, count(*) FROM webhook_events GROUP BY status ORDER BY status`)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Print("webhook_events by status:")
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return err
		}
		fmt.Printf(" %s=%d", status, n)
	}
	fmt.Println()

	fmt.Printf("integrity: unbalanced transactions=%d, balances out of sync=%d\n", unbalanced, driftedBalances)
	if unbalanced != 0 || driftedBalances != 0 {
		return errors.New("integrity check failed")
	}
	return rows.Err()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
