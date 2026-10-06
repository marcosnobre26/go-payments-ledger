package payout

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (s *Service) RunReconciler(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := s.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("payout reconciler", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) ReconcileOnce(ctx context.Context) (int, error) {
	n := 0
	for ctx.Err() == nil {
		id, status, providerRef, ok, err := s.claimDue(ctx)
		if err != nil {
			return n, err
		}
		if !ok {
			break
		}
		n++
		switch status {
		case StatusPending:
			s.metrics.Payouts.Inc("resubmitted")
			s.submit(ctx, id)
		case StatusSubmitted:
			s.pollProvider(ctx, id, providerRef)
		}
	}
	s.updateInFlight(ctx)
	return n, nil
}

func (s *Service) claimDue(ctx context.Context) (id, status, providerRef string, ok bool, err error) {
	err = s.db.QueryRowContext(ctx, `
		UPDATE payouts SET next_attempt_at = now() + make_interval(secs => $1)
		WHERE id = (
			SELECT id FROM payouts
			WHERE status IN ('pending', 'submitted') AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, status, COALESCE(provider_ref, '')`, s.Lease.Seconds(),
	).Scan(&id, &status, &providerRef)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", false, nil
	}
	return id, status, providerRef, err == nil, err
}

func (s *Service) pollProvider(ctx context.Context, id, providerRef string) {
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := s.provider.Status(callCtx, providerRef)
	log := s.log.With("payout_id", id, "provider_ref", providerRef)
	if err != nil {
		log.Warn("provider status check failed; lease will expire and retry", "error", err)
		return
	}
	switch res.Status {
	case ProviderPaid:
		err = s.inTx(ctx, func(tx *sql.Tx) error { return s.MarkPaidTx(ctx, tx, id, providerRef) })
	case ProviderFailed:
		err = s.inTx(ctx, func(tx *sql.Tx) error { return s.MarkFailedTx(ctx, tx, id, providerRef, res.FailureReason) })
	default:
		_, err = s.db.ExecContext(ctx,
			`UPDATE payouts SET next_attempt_at = now() + make_interval(secs => $2) WHERE id = $1`,
			id, s.StaleAfter.Seconds())
	}
	if err != nil {
		log.Error("apply provider status", "error", err)
		return
	}
	log.Info("payout reconciled with provider", "provider_status", res.Status)
}

func (s *Service) updateInFlight(ctx context.Context) {
	var n int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM payouts WHERE status IN ('pending', 'submitted')`,
	).Scan(&n); err == nil {
		s.metrics.PayoutsInFlight.Set(float64(n))
	}
}
