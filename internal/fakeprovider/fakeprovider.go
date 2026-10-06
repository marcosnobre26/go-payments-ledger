package fakeprovider

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	mrand "math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

type Options struct {
	WebhookURL          string
	Secret              []byte
	WebhookDelay        time.Duration
	FailAfterRecordRate float64
	DropWebhookRate     float64
	Logger              *slog.Logger
}

type payout struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason,omitempty"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	Destination   string `json:"destination"`
	Reference     string `json:"reference"`
}

type Server struct {
	opts   Options
	client *http.Client

	mu        sync.Mutex
	byKey     map[string]*payout
	byID      map[string]*payout
	failNext  int
	dropNext  int
	submitted int
}

func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Server{
		opts:   opts,
		client: &http.Client{Timeout: 5 * time.Second},
		byKey:  map[string]*payout{},
		byID:   map[string]*payout{},
	}
}

func (s *Server) FailNextAfterRecord(n int) {
	s.mu.Lock()
	s.failNext = n
	s.mu.Unlock()
}

func (s *Server) DropNextWebhooks(n int) {
	s.mu.Lock()
	s.dropNext = n
	s.mu.Unlock()
}

func (s *Server) Payouts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payouts", s.create)
	mux.HandleFunc("GET /v1/payouts/{id}", s.get)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]int{"payouts_created": len(s.byID), "submit_calls": s.submitted})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header is required"})
		return
	}
	var in struct {
		Amount      int64  `json:"amount"`
		Currency    string `json:"currency"`
		Destination string `json:"destination"`
		Reference   string `json:"reference"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payout request"})
		return
	}
	if strings.HasPrefix(in.Destination, "invalid") {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "invalid destination"})
		return
	}

	s.mu.Lock()
	s.submitted++
	if existing, ok := s.byKey[key]; ok {

		p := *existing
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, p)
		return
	}
	p := &payout{
		ID: "po_" + randomHex(8), Status: "processing",
		Amount: in.Amount, Currency: in.Currency, Destination: in.Destination, Reference: in.Reference,
	}
	s.byKey[key], s.byID[p.ID] = p, p
	failAfterRecord := false
	if s.failNext > 0 {
		s.failNext--
		failAfterRecord = true
	} else if mrand.Float64() < s.opts.FailAfterRecordRate {
		failAfterRecord = true
	}
	snapshot := *p
	s.mu.Unlock()

	time.AfterFunc(s.opts.WebhookDelay, func() { s.finalize(p.ID) })

	if failAfterRecord {
		s.opts.Logger.Warn("injected failure: payout recorded, responding 500", "payout", p.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusCreated, snapshot)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	p, ok := s.byID[r.PathValue("id")]
	var snapshot payout
	if ok {
		snapshot = *p
	}
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "payout not found"})
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) finalize(id string) {
	s.mu.Lock()
	p := s.byID[id]
	if strings.HasPrefix(p.Destination, "reject") {
		p.Status, p.FailureReason = "failed", "destination account closed"
	} else {
		p.Status = "paid"
	}
	drop := false
	if s.dropNext > 0 {
		s.dropNext--
		drop = true
	} else if mrand.Float64() < s.opts.DropWebhookRate {
		drop = true
	}
	snapshot := *p
	s.mu.Unlock()

	if drop || s.opts.WebhookURL == "" {
		s.opts.Logger.Warn("webhook not delivered (simulated loss)", "payout", id, "status", snapshot.Status)
		return
	}
	s.sendWebhook(snapshot)
}

func (s *Server) sendWebhook(p payout) {
	body, _ := json.Marshal(map[string]any{
		"id":   "evt_" + p.ID + "_" + p.Status,
		"type": "payout." + p.Status,
		"data": map[string]any{
			"payout_id":          p.Reference,
			"provider_payout_id": p.ID,
			"failure_reason":     p.FailureReason,
		},
	})
	for attempt := 0; attempt < 5; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, s.opts.WebhookURL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(webhook.SignatureHeader, webhook.SignatureHeaderValue(s.opts.Secret, time.Now().Unix(), body))
		resp, err := s.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
		}
		time.Sleep(time.Duration(1<<attempt) * time.Second)
	}
	s.opts.Logger.Error("webhook delivery gave up", "payout", p.ID)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
