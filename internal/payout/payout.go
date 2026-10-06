package payout

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
)

const (
	StatusPending   = "pending"
	StatusSubmitted = "submitted"
	StatusPaid      = "paid"
	StatusFailed    = "failed"
)

type permanentError struct{ msg string }

func (e *permanentError) Error() string   { return e.msg }
func (e *permanentError) Permanent() bool { return true }

var (
	ErrNotFound           error = &permanentError{"payout not found"}
	ErrStateConflict      error = &permanentError{"payout already finalized with a different outcome"}
	ErrInvalidDestination       = errors.New("destination is required (max 128 characters)")
)

type Payout struct {
	ID          string    `json:"id"`
	AccountID   string    `json:"account_id"`
	Amount      int64     `json:"amount"`
	Currency    string    `json:"currency"`
	Destination string    `json:"destination"`
	Status      string    `json:"status"`
	ProviderRef *string   `json:"provider_ref,omitempty"`
	Attempts    int       `json:"attempts"`
	LastError   *string   `json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CreateInput struct {
	AccountID   string `json:"account_id"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	Destination string `json:"destination"`
}

func (in CreateInput) validate() error {
	switch {
	case !ledger.ValidCurrency(in.Currency):
		return ledger.ErrInvalidCurrency
	case in.Amount <= 0:
		return ledger.ErrInvalidAmount
	case !ledger.ValidID(in.AccountID):
		return ledger.ErrAccountNotFound
	case in.Destination == "" || len(in.Destination) > 128:
		return ErrInvalidDestination
	}
	return nil
}

func (in CreateInput) hash() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s", in.AccountID, in.Amount, in.Currency, in.Destination)))
	return hex.EncodeToString(sum[:])
}

type Service struct {
	db       *sql.DB
	provider Provider
	metrics  *metrics.Metrics
	log      *slog.Logger

	StaleAfter time.Duration

	Lease time.Duration
}

func NewService(db *sql.DB, p Provider, m *metrics.Metrics, log *slog.Logger) *Service {
	return &Service{db: db, provider: p, metrics: m, log: log, StaleAfter: 2 * time.Minute, Lease: 30 * time.Second}
}

func (s *Service) Create(ctx context.Context, idempotencyKey string, in CreateInput) (Payout, bool, error) {
	if err := in.validate(); err != nil {
		return Payout{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Payout{}, false, err
	}
	defer tx.Rollback() //nolint:errcheck

	var exists bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1 AND NOT is_system)`, in.AccountID,
	).Scan(&exists); err != nil {
		return Payout{}, false, err
	}
	if !exists {
		return Payout{}, false, ledger.ErrAccountNotFound
	}

	var id string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO payouts (account_id, amount, currency, destination, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`,
		in.AccountID, in.Amount, in.Currency, in.Destination, idempotencyKey, in.hash(),
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		var storedHash, existingID string
		if err := tx.QueryRowContext(ctx,
			`SELECT request_hash, id FROM payouts WHERE idempotency_key = $1`, idempotencyKey,
		).Scan(&storedHash, &existingID); err != nil {
			return Payout{}, false, err
		}
		if storedHash != in.hash() {
			return Payout{}, false, ledger.ErrIdempotencyKeyReused
		}
		s.metrics.Payouts.Inc("replayed")
		p, err := s.Get(ctx, existingID)
		return p, true, err
	}
	if err != nil {
		return Payout{}, false, err
	}

	clearing, err := ledger.SystemAccountTx(ctx, tx, in.Currency, ledger.RoleClearing)
	if err != nil {
		return Payout{}, false, err
	}

	if _, err := ledger.PostTx(ctx, tx, "payout_reserve", "payout:"+id+":reserve",
		in.AccountID, clearing, in.Amount, in.Currency); err != nil {
		return Payout{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Payout{}, false, err
	}
	s.metrics.Payouts.Inc("reserved")

	s.submit(context.WithoutCancel(ctx), id)

	p, err := s.Get(ctx, id)
	return p, false, err
}

func (s *Service) Get(ctx context.Context, id string) (Payout, error) {
	if !ledger.ValidID(id) {
		return Payout{}, ErrNotFound
	}
	var p Payout
	err := s.db.QueryRowContext(ctx, `
		SELECT id, account_id, amount, currency, destination, status, provider_ref,
		       attempts, last_error, created_at, updated_at
		FROM payouts WHERE id = $1`, id,
	).Scan(&p.ID, &p.AccountID, &p.Amount, &p.Currency, &p.Destination, &p.Status, &p.ProviderRef,
		&p.Attempts, &p.LastError, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Payout{}, ErrNotFound
	}
	return p, err
}

func (s *Service) submit(ctx context.Context, id string) {
	p, err := s.Get(ctx, id)
	if err != nil || p.Status != StatusPending {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := s.provider.Submit(callCtx, SubmitRequest{
		IdempotencyKey: p.ID, Reference: p.ID,
		Amount: p.Amount, Currency: p.Currency, Destination: p.Destination,
	})
	log := s.log.With("payout_id", p.ID, "attempt", p.Attempts+1)

	switch {
	case err == nil:
		s.recordAccepted(ctx, p.ID, res)
		log.Info("payout submitted", "provider_ref", res.ProviderRef, "provider_status", res.Status)

	case errors.Is(err, ErrRejected):

		err := s.inTx(ctx, func(tx *sql.Tx) error { return s.MarkFailedTx(ctx, tx, p.ID, "", err.Error()) })
		if err != nil {
			log.Error("release rejected payout", "error", err)
			return
		}
		s.metrics.Payouts.Inc("rejected")
		log.Warn("payout rejected by provider")

	default:

		if _, dbErr := s.db.ExecContext(ctx, `
			UPDATE payouts
			SET attempts = attempts + 1, last_error = $2, updated_at = now(),
			    next_attempt_at = now() + make_interval(secs => $3)
			WHERE id = $1 AND status = 'pending'`, p.ID, err.Error(), backoff(p.Attempts+1).Seconds()); dbErr != nil {
			log.Error("record submit error", "error", dbErr)
		}
		s.metrics.Payouts.Inc("submit_error")
		log.Warn("payout submit failed, will retry", "error", err)
	}
}

func (s *Service) recordAccepted(ctx context.Context, id string, res Result) {
	switch res.Status {
	case ProviderPaid:
		_ = s.inTx(ctx, func(tx *sql.Tx) error { return s.MarkPaidTx(ctx, tx, id, res.ProviderRef) })
		return
	case ProviderFailed:
		_ = s.inTx(ctx, func(tx *sql.Tx) error { return s.MarkFailedTx(ctx, tx, id, res.ProviderRef, res.FailureReason) })
		return
	}

	res2, err := s.db.ExecContext(ctx, `
		UPDATE payouts
		SET status = 'submitted', provider_ref = $2, attempts = attempts + 1, last_error = NULL,
		    next_attempt_at = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND status = 'pending'`, id, res.ProviderRef, s.StaleAfter.Seconds())
	if err != nil {
		s.log.Error("record submitted payout", "payout_id", id, "error", err)
		return
	}
	if n, _ := res2.RowsAffected(); n == 1 {
		s.metrics.Payouts.Inc("submitted")
	}
}

func (s *Service) MarkPaidTx(ctx context.Context, tx *sql.Tx, id, providerRef string) error {
	p, err := lockTx(ctx, tx, id)
	if err != nil {
		return err
	}
	switch p.Status {
	case StatusPaid:
		return nil
	case StatusFailed:
		return ErrStateConflict
	}
	clearing, err := ledger.SystemAccountTx(ctx, tx, p.Currency, ledger.RoleClearing)
	if err != nil {
		return err
	}
	settlement, err := ledger.SystemAccountTx(ctx, tx, p.Currency, ledger.RoleSettlement)
	if err != nil {
		return err
	}
	if _, err := ledger.PostTx(ctx, tx, "payout_settle", "payout:"+id+":settle",
		clearing, settlement, p.Amount, p.Currency); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE payouts SET status = 'paid', provider_ref = COALESCE(provider_ref, NULLIF($2, '')),
		       last_error = NULL, updated_at = now()
		WHERE id = $1`, id, providerRef); err != nil {
		return err
	}
	s.metrics.Payouts.Inc("paid")
	return nil
}

func (s *Service) MarkFailedTx(ctx context.Context, tx *sql.Tx, id, providerRef, reason string) error {
	p, err := lockTx(ctx, tx, id)
	if err != nil {
		return err
	}
	switch p.Status {
	case StatusFailed:
		return nil
	case StatusPaid:
		return ErrStateConflict
	}
	clearing, err := ledger.SystemAccountTx(ctx, tx, p.Currency, ledger.RoleClearing)
	if err != nil {
		return err
	}
	if _, err := ledger.PostTx(ctx, tx, "payout_reverse", "payout:"+id+":reverse",
		clearing, p.AccountID, p.Amount, p.Currency); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE payouts SET status = 'failed', provider_ref = COALESCE(provider_ref, NULLIF($2, '')),
		       last_error = NULLIF($3, ''), updated_at = now()
		WHERE id = $1`, id, providerRef, reason); err != nil {
		return err
	}
	s.metrics.Payouts.Inc("failed")
	return nil
}

func lockTx(ctx context.Context, tx *sql.Tx, id string) (Payout, error) {
	if !ledger.ValidID(id) {
		return Payout{}, ErrNotFound
	}
	var p Payout
	err := tx.QueryRowContext(ctx,
		`SELECT id, account_id, amount, currency, status FROM payouts WHERE id = $1 FOR UPDATE`, id,
	).Scan(&p.ID, &p.AccountID, &p.Amount, &p.Currency, &p.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return Payout{}, ErrNotFound
	}
	return p, err
}

func (s *Service) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func backoff(attempt int) time.Duration {
	if attempt > 10 {
		return 5 * time.Minute
	}
	d := time.Duration(1<<attempt) * time.Second
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}
