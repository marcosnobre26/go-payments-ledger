package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
)

// Event is the payload sent by the payment provider.
type Event struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		AccountID string `json:"account_id"`
		Amount    int64  `json:"amount"`
		Currency  string `json:"currency"`
	} `json:"data"`
}

const EventPaymentSucceeded = "payment.succeeded"

var errMalformedPayload = errors.New("malformed event payload")

// Store persists incoming events. Receiving and processing are separated so
// the endpoint answers the provider in milliseconds, regardless of how long
// processing takes or whether it fails.
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Save stores the event once. It returns duplicate=true when the provider
// redelivers an event that was already received.
func (s *Store) Save(ctx context.Context, provider string, ev Event, raw []byte) (duplicate bool, err error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_events (provider, event_id, event_type, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (provider, event_id) DO NOTHING`,
		provider, ev.ID, ev.Type, raw)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 0, err
}

// Worker polls pending events and applies them to the ledger.
type Worker struct {
	db          *sql.DB
	ledger      *ledger.Service
	log         *slog.Logger
	interval    time.Duration
	maxAttempts int
}

func NewWorker(db *sql.DB, l *ledger.Service, log *slog.Logger) *Worker {
	return &Worker{db: db, ledger: l, log: log, interval: time.Second, maxAttempts: 8}
}

// Run processes events until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		// Drain everything that is due, then wait for the next tick.
		for {
			processed, err := w.ProcessNext(ctx)
			if err != nil && ctx.Err() == nil {
				w.log.Error("webhook worker", "error", err)
			}
			if !processed || ctx.Err() != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ProcessNext handles one due event. FOR UPDATE SKIP LOCKED lets several
// worker replicas run in parallel without picking the same event.
func (w *Worker) ProcessNext(ctx context.Context) (processed bool, err error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck

	var (
		id       int64
		provider string
		eventID  string
		payload  []byte
		attempts int
	)
	err = tx.QueryRowContext(ctx, `
		SELECT id, provider, event_id, payload, attempts
		FROM webhook_events
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
	).Scan(&id, &provider, &eventID, &payload, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// A savepoint lets us undo a failed ledger posting while still recording
	// the failed attempt in the same transaction.
	if _, err := tx.ExecContext(ctx, `SAVEPOINT apply_event`); err != nil {
		return false, err
	}
	applyErr := w.apply(ctx, tx, provider, eventID, payload)

	log := w.log.With("event_id", eventID, "provider", provider, "attempt", attempts+1)
	switch {
	case applyErr == nil:
		_, err = tx.ExecContext(ctx, `
			UPDATE webhook_events
			SET status = 'processed', attempts = attempts + 1, processed_at = now(), last_error = NULL
			WHERE id = $1`, id)
		log.Info("webhook processed")

	case ledger.IsPermanent(applyErr) || errors.Is(applyErr, errMalformedPayload) || attempts+1 >= w.maxAttempts:
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT apply_event`); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE webhook_events
			SET status = 'failed', attempts = attempts + 1, last_error = $2
			WHERE id = $1`, id, applyErr.Error())
		log.Warn("webhook failed permanently", "error", applyErr)

	default:
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT apply_event`); err != nil {
			return false, err
		}
		delay := backoff(attempts + 1)
		_, err = tx.ExecContext(ctx, `
			UPDATE webhook_events
			SET attempts = attempts + 1, last_error = $2,
			    next_attempt_at = now() + make_interval(secs => $3)
			WHERE id = $1`, id, applyErr.Error(), delay.Seconds())
		log.Warn("webhook will be retried", "error", applyErr, "retry_in", delay.String())
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (w *Worker) apply(ctx context.Context, tx *sql.Tx, provider, eventID string, payload []byte) error {
	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		return fmt.Errorf("%w: %v", errMalformedPayload, err)
	}
	switch ev.Type {
	case EventPaymentSucceeded:
		reference := provider + ":" + eventID
		return w.ledger.DepositTx(ctx, tx, ev.Data.AccountID, ev.Data.Amount, ev.Data.Currency, reference)
	default:
		// Unknown event types are acknowledged and ignored, so providers can
		// add new events without breaking the integration.
		return nil
	}
}

// backoff returns an exponential delay: 2s, 4s, 8s... capped at 10 minutes.
func backoff(attempt int) time.Duration {
	d := time.Duration(1<<attempt) * time.Second
	if d > 10*time.Minute {
		return 10 * time.Minute
	}
	return d
}
