// Package httpapi exposes the ledger and webhook receiver over HTTP.
package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/ledger"
	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type Server struct {
	ledger         *ledger.Service
	webhooks       *webhook.Store
	webhookSecrets map[string][]byte // provider name -> signing secret
	log            *slog.Logger
	now            func() time.Time
}

func New(l *ledger.Service, w *webhook.Store, webhookSecrets map[string][]byte, log *slog.Logger) *Server {
	return &Server{ledger: l, webhooks: w, webhookSecrets: webhookSecrets, log: log, now: time.Now}
}

// Handler returns the router wrapped in middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/accounts", s.createAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", s.listEntries)
	mux.HandleFunc("POST /v1/transfers", s.createTransfer)
	mux.HandleFunc("POST /v1/webhooks/{provider}", s.receiveWebhook)

	return s.recoverer(s.requestLogger(requestID(mux)))
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
		writeProblem(w, http.StatusBadRequest, "missing_idempotency_key",
			"the Idempotency-Key header is required (max 255 characters)")
		return
	}
	var in ledger.TransferInput
	if !decode(w, r, &in) {
		return
	}

	transfer, replayed, err := s.ledger.Transfer(r.Context(), key, in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, transfer)
		return
	}
	writeJSON(w, http.StatusCreated, transfer)
}

func (s *Server) receiveWebhook(w http.ResponseWriter, r *http.Request) {
	provider := r.PathValue("provider")
	secret, ok := s.webhookSecrets[provider]
	if !ok {
		writeProblem(w, http.StatusNotFound, "unknown_provider", "unknown webhook provider")
		return
	}

	// The signature is computed over the exact raw bytes, so read the body
	// before any JSON decoding.
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeProblem(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large")
		return
	}
	if err := webhook.Verify(secret, r.Header.Get(webhook.SignatureHeader), raw, s.now(), 5*time.Minute); err != nil {
		s.log.Warn("webhook rejected", "provider", provider, "reason", err.Error())
		writeProblem(w, http.StatusUnauthorized, "invalid_signature", err.Error())
		return
	}

	var ev webhook.Event
	if err := json.Unmarshal(raw, &ev); err != nil || ev.ID == "" || ev.Type == "" {
		writeProblem(w, http.StatusBadRequest, "invalid_event", "event must be JSON with id and type")
		return
	}

	duplicate, err := s.webhooks.Save(r.Context(), provider, ev, raw)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	// Duplicates are acknowledged with success so the provider stops retrying.
	writeJSON(w, http.StatusAccepted, map[string]any{"received": true, "duplicate": duplicate})
}

// ---- errors and JSON helpers ----

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

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		writeProblem(w, http.StatusNotFound, "account_not_found", err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		writeProblem(w, http.StatusUnprocessableEntity, "insufficient_funds", err.Error())
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		writeProblem(w, http.StatusUnprocessableEntity, "currency_mismatch", err.Error())
	case errors.Is(err, ledger.ErrIdempotencyKeyReused):
		writeProblem(w, http.StatusUnprocessableEntity, "idempotency_key_reused", err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrSameAccount):
		writeProblem(w, http.StatusBadRequest, "validation_error", err.Error())
	default:
		s.log.Error("internal error", "error", err, "request_id", r.Header.Get("X-Request-ID"))
		writeProblem(w, http.StatusInternalServerError, "internal_error", "internal server error")
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

// ---- middleware ----

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
