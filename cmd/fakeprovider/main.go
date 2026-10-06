package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/fakeprovider"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := fakeprovider.New(fakeprovider.Options{
		WebhookURL:          os.Getenv("API_WEBHOOK_URL"),
		Secret:              []byte(env("WEBHOOK_SECRET", "dev-secret")),
		WebhookDelay:        duration("WEBHOOK_DELAY", 3*time.Second),
		FailAfterRecordRate: rate("FAIL_AFTER_RECORD_RATE", 0.1),
		DropWebhookRate:     rate("DROP_WEBHOOK_RATE", 0.1),
		Logger:              log,
	})

	httpSrv := &http.Server{Addr: ":" + env("PORT", "8081"), Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Info("fake payout provider listening", "addr", httpSrv.Addr, "webhook_url", os.Getenv("API_WEBHOOK_URL"))
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return fallback
}

func rate(key string, fallback float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil && f >= 0 && f <= 1 {
		return f
	}
	return fallback
}
