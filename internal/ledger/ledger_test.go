package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/testutil"
)

func newFundedAccount(t *testing.T, svc *ledger.Service, amount int64) ledger.Account {
	t.Helper()
	ctx := context.Background()
	acc, err := svc.CreateAccount(ctx, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if amount > 0 {
		ref := fmt.Sprintf("test:%s", acc.ID)
		if err := svc.Deposit(ctx, acc.ID, amount, "BRL", ref); err != nil {
			t.Fatal(err)
		}
	}
	return acc
}

func balance(t *testing.T, svc *ledger.Service, id string) int64 {
	t.Helper()
	acc, err := svc.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return acc.Balance
}

func key(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestTransferMovesMoney(t *testing.T) {
	svc := ledger.NewService(testutil.DB(t))
	from, to := newFundedAccount(t, svc, 10_000), newFundedAccount(t, svc, 0)

	tr, replayed, err := svc.Transfer(context.Background(), key(t), ledger.TransferInput{
		FromAccountID: from.ID, ToAccountID: to.ID, Amount: 2_550, Currency: "BRL",
	})
	if err != nil || replayed {
		t.Fatalf("transfer: err=%v replayed=%v", err, replayed)
	}
	if tr.Amount != 2_550 {
		t.Fatalf("amount = %d", tr.Amount)
	}
	if got := balance(t, svc, from.ID); got != 7_450 {
		t.Fatalf("from balance = %d, want 7450", got)
	}
	if got := balance(t, svc, to.ID); got != 2_550 {
		t.Fatalf("to balance = %d, want 2550", got)
	}
}

func TestTransferRejectsInsufficientFunds(t *testing.T) {
	svc := ledger.NewService(testutil.DB(t))
	from, to := newFundedAccount(t, svc, 100), newFundedAccount(t, svc, 0)

	_, _, err := svc.Transfer(context.Background(), key(t), ledger.TransferInput{
		FromAccountID: from.ID, ToAccountID: to.ID, Amount: 101, Currency: "BRL",
	})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("got %v, want ErrInsufficientFunds", err)
	}
	if got := balance(t, svc, from.ID); got != 100 {
		t.Fatalf("balance changed to %d", got)
	}
}

func TestTransferIsIdempotent(t *testing.T) {
	svc := ledger.NewService(testutil.DB(t))
	from, to := newFundedAccount(t, svc, 1_000), newFundedAccount(t, svc, 0)
	in := ledger.TransferInput{FromAccountID: from.ID, ToAccountID: to.ID, Amount: 300, Currency: "BRL"}
	k := key(t)

	first, _, err := svc.Transfer(context.Background(), k, in)
	if err != nil {
		t.Fatal(err)
	}
	second, replayed, err := svc.Transfer(context.Background(), k, in)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed || second.ID != first.ID {
		t.Fatalf("expected replay of %s, got %s (replayed=%v)", first.ID, second.ID, replayed)
	}
	if got := balance(t, svc, from.ID); got != 700 {
		t.Fatalf("money moved twice: balance = %d", got)
	}

	in.Amount = 999
	if _, _, err := svc.Transfer(context.Background(), k, in); !errors.Is(err, ledger.ErrIdempotencyKeyReused) {
		t.Fatalf("got %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestConcurrentTransfersNeverOverdraw(t *testing.T) {
	svc := ledger.NewService(testutil.DB(t))
	from, to := newFundedAccount(t, svc, 1_000), newFundedAccount(t, svc, 0)

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, insufficient := 0, 0
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := svc.Transfer(context.Background(), fmt.Sprintf("%s-%d", key(t), i), ledger.TransferInput{
				FromAccountID: from.ID, ToAccountID: to.ID, Amount: 100, Currency: "BRL",
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ledger.ErrInsufficientFunds):
				insufficient++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if ok != 10 || insufficient != 15 {
		t.Fatalf("ok=%d insufficient=%d, want 10/15", ok, insufficient)
	}
	if got := balance(t, svc, from.ID); got != 0 {
		t.Fatalf("from balance = %d, want 0", got)
	}
	if got := balance(t, svc, to.ID); got != 1_000 {
		t.Fatalf("to balance = %d, want 1000", got)
	}
}

func TestTransferValidation(t *testing.T) {
	svc := ledger.NewService(testutil.DB(t))
	a, b := newFundedAccount(t, svc, 500), newFundedAccount(t, svc, 0)
	usd, err := svc.CreateAccount(context.Background(), "USD")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		in   ledger.TransferInput
		want error
	}{
		{"zero amount", ledger.TransferInput{a.ID, b.ID, 0, "BRL"}, ledger.ErrInvalidAmount},
		{"negative amount", ledger.TransferInput{a.ID, b.ID, -5, "BRL"}, ledger.ErrInvalidAmount},
		{"same account", ledger.TransferInput{a.ID, a.ID, 10, "BRL"}, ledger.ErrSameAccount},
		{"bad currency", ledger.TransferInput{a.ID, b.ID, 10, "real"}, ledger.ErrInvalidCurrency},
		{"currency mismatch", ledger.TransferInput{a.ID, usd.ID, 10, "BRL"}, ledger.ErrCurrencyMismatch},
		{"unknown account", ledger.TransferInput{a.ID, "00000000-0000-0000-0000-000000000000", 10, "BRL"}, ledger.ErrAccountNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := svc.Transfer(context.Background(), key(t), tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
