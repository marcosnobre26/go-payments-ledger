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
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
)

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

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

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

type Worker struct {
	db          *sql.DB
	ledger      *ledger.Service
	metrics     *metrics.Metrics
	log         *slog.Logger
	interval    time.Duration
	maxAttempts int
}

func NewWorker(db *sql.DB, l *ledger.Service, m *metrics.Metrics, log *slog.Logger) *Worker {
	return &Worker{db: db, ledger: l, metrics: m, log: log, interval: time.Second, maxAttempts: 8}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {

		for {
			processed, err := w.ProcessNext(ctx)
			if err != nil && ctx.Err() == nil {
				w.log.Error("webhook worker", "error", err)
			}
			if !processed || ctx.Err() != nil {
				break
			}
		}
		w.updateBacklog(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) ProcessNext(ctx context.Context) (processed bool, err error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

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

	if _, err := tx.ExecContext(ctx, `SAVEPOINT apply_event`); err != nil {
		return false, err
	}
	applyErr := w.apply(ctx, tx, provider, eventID, payload)

	log := w.log.With("event_id", eventID, "provider", provider, "attempt", attempts+1)
	var result string
	switch {
	case applyErr == nil:
		result = "processed"
		_, err = tx.ExecContext(ctx, `
			UPDATE webhook_events
			SET status = 'processed', attempts = attempts + 1, processed_at = now(), last_error = NULL
			WHERE id = $1`, id)
		log.Info("webhook processed")

	case ledger.IsPermanent(applyErr) || errors.Is(applyErr, errMalformedPayload) || attempts+1 >= w.maxAttempts:
		result = "failed"
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT apply_event`); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE webhook_events
			SET status = 'failed', attempts = attempts + 1, last_error = $2
			WHERE id = $1`, id, applyErr.Error())
		log.Warn("webhook failed permanently", "error", applyErr)

	default:
		result = "retry"
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
	if err := tx.Commit(); err != nil {
		return false, err
	}
	
	w.metrics.WebhooksProcessed.Inc(result)
	return true, nil
}

func (w *Worker) updateBacklog(ctx context.Context) {
	var pending int64
	if err := w.db.QueryRowContext(ctx,
		`SELECT count(*) FROM webhook_events WHERE status = 'pending'`,
	).Scan(&pending); err == nil {
		w.metrics.WebhooksPending.Set(float64(pending))
	}
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
		return nil
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(1<<attempt) * time.Second
	if d > 10*time.Minute {
		return 10 * time.Minute
	}
	return d
}
