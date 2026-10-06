package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/database"
	"github.com/marcosnobre26/go-payments-ledger/internal/httpapi"
	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
	"github.com/marcosnobre26/go-payments-ledger/internal/payout"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable")
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		return errors.New("WEBHOOK_SECRET is required")
	}
	provider := env("WEBHOOK_PROVIDER", "acmepay")

	db, err := database.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := database.Migrate(ctx, db); err != nil {
		return err
	}

	ledgerSvc := ledger.NewService(db)
	m := metrics.New()
	worker := webhook.NewWorker(db, ledgerSvc, m, log)

	var payoutSvc *payout.Service
	if providerURL := os.Getenv("PAYOUT_PROVIDER_URL"); providerURL != "" {
		payoutSvc = payout.NewService(db, payout.NewHTTPProvider(providerURL), m, log)
		if d, err := time.ParseDuration(os.Getenv("PAYOUT_STALE_AFTER")); err == nil {
			payoutSvc.StaleAfter = d
		}
		worker.WithPayouts(payoutSvc)
		log.Info("payouts enabled", "provider_url", providerURL, "stale_after", payoutSvc.StaleAfter.String())
	}

	api := httpapi.New(httpapi.Deps{
		Ledger:         ledgerSvc,
		Webhooks:       webhook.NewStore(db),
		WebhookSecrets: map[string][]byte{provider: []byte(secret)},
		Payouts:        payoutSvc,
		Metrics:        m,
		Logger:         log,
		Ready:          db.PingContext,
	})

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Run(ctx)
	}()
	if payoutSvc != nil {
		go payoutSvc.RunReconciler(ctx, 5*time.Second)
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-workerDone
	return err
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
