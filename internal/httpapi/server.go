package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/metrics"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type Server struct {
	ledger         *ledger.Service
	webhooks       *webhook.Store
	webhookSecrets map[string][]byte
	metrics        *metrics.Metrics
	ready          func(context.Context) error
	log            *slog.Logger
	now            func() time.Time
}

type Deps struct {
	Ledger         *ledger.Service
	Webhooks       *webhook.Store
	WebhookSecrets map[string][]byte
	Metrics        *metrics.Metrics
	Logger         *slog.Logger
	Ready          func(context.Context) error
}

func New(d Deps) *Server {
	if d.Ready == nil {
		d.Ready = func(context.Context) error { return nil }
	}
	return &Server{
		ledger: d.Ledger, webhooks: d.Webhooks, webhookSecrets: d.WebhookSecrets,
		metrics: d.Metrics, ready: d.Ready, log: d.Logger, now: time.Now,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	handle := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, s.instrument(pattern, h))
	}

	handle("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	handle("GET /readyz", s.readyz)
	handle("POST /v1/accounts", s.createAccount)
	handle("GET /v1/accounts/{id}", s.getAccount)
	handle("GET /v1/accounts/{id}/entries", s.listEntries)
	handle("POST /v1/transfers", s.createTransfer)
	handle("POST /v1/webhooks/{provider}", s.receiveWebhook)
	mux.Handle("GET /metrics", s.metrics.Handler())

	return s.recoverer(s.requestLogger(requestID(mux)))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "not_ready", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Currency string `json:"currency"`
	}
	if !decode(w, r, &body) {
		return
	}
	account, err := s.ledger.CreateAccount(r.Context(), body.Currency)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, account)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	account, err := s.ledger.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	entries, err := s.ledger.ListEntries(r.Context(), r.PathValue("id"), 50)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": entries})
}

func (s *Server) createTransfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		s.metrics.Transfers.Inc("missing_idempotency_key")
		writeProblem(w, http.StatusBadRequest, "missing_idempotency_key",
			"the Idempotency-Key header is required (max 255 characters)")
		return
	}
	var in ledger.TransferInput
	if !decode(w, r, &in) {
		s.metrics.Transfers.Inc("invalid_json")
		return
	}

	transfer, replayed, err := s.ledger.Transfer(r.Context(), key, in)
	if err != nil {
		s.metrics.Transfers.Inc(errorCode(err))
		s.writeError(w, r, err)
		return
	}
	if replayed {
		s.metrics.Transfers.Inc("replayed")
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, transfer)
		return
	}
	s.metrics.Transfers.Inc("created")
	writeJSON(w, http.StatusCreated, transfer)
}

func (s *Server) receiveWebhook(w http.ResponseWriter, r *http.Request) {
	provider := r.PathValue("provider")
	secret, ok := s.webhookSecrets[provider]
	if !ok {
		writeProblem(w, http.StatusNotFound, "unknown_provider", "unknown webhook provider")
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeProblem(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large")
		return
	}
	if err := webhook.Verify(secret, r.Header.Get(webhook.SignatureHeader), raw, s.now(), 5*time.Minute); err != nil {
		s.log.Warn("webhook rejected", "provider", provider, "reason", err.Error())
		s.metrics.WebhooksRejected.Inc(provider, "invalid_signature")
		writeProblem(w, http.StatusUnauthorized, "invalid_signature", err.Error())
		return
	}

	var ev webhook.Event
	if err := json.Unmarshal(raw, &ev); err != nil || ev.ID == "" || ev.Type == "" {
		s.metrics.WebhooksRejected.Inc(provider, "invalid_event")
		writeProblem(w, http.StatusBadRequest, "invalid_event", "event must be JSON with id and type")
		return
	}

	duplicate, err := s.webhooks.Save(r.Context(), provider, ev, raw)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.metrics.WebhooksReceived.Inc(provider, strconv.FormatBool(duplicate))
	writeJSON(w, http.StatusAccepted, map[string]any{"received": true, "duplicate": duplicate})
}

type problem struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeProblem(w http.ResponseWriter, status int, code, message string) {
	var p problem
	p.Error.Code, p.Error.Message = code, message
	writeJSON(w, status, p)
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		return "account_not_found"
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return "insufficient_funds"
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		return "currency_mismatch"
	case errors.Is(err, ledger.ErrIdempotencyKeyReused):
		return "idempotency_key_reused"
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrSameAccount):
		return "validation_error"
	default:
		return "internal_error"
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	code := errorCode(err)
	switch code {
	case "account_not_found":
		writeProblem(w, http.StatusNotFound, code, err.Error())
	case "insufficient_funds", "currency_mismatch", "idempotency_key_reused":
		writeProblem(w, http.StatusUnprocessableEntity, code, err.Error())
	case "validation_error":
		writeProblem(w, http.StatusBadRequest, code, err.Error())
	default:
		s.log.Error("internal error", "error", err, "request_id", r.Header.Get("X-Request-ID"))
		writeProblem(w, http.StatusInternalServerError, code, "internal server error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
			r.Header.Set("X-Request-ID", id)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(pattern string, next http.Handler) http.Handler {
	method, route, _ := strings.Cut(pattern, " ")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.metrics.HTTPRequests.Inc(method, route, strconv.Itoa(rec.status))
		s.metrics.HTTPDuration.Observe(time.Since(start).Seconds(), method, route)
	})
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", r.Header.Get("X-Request-ID"))
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic recovered", "panic", v)
				writeProblem(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
