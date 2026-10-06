package payout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrRejected = errors.New("payout rejected by provider")

const (
	ProviderProcessing = "processing"
	ProviderPaid       = "paid"
	ProviderFailed     = "failed"
)

type SubmitRequest struct {
	IdempotencyKey string
	Reference      string
	Amount         int64
	Currency       string
	Destination    string
}

type Result struct {
	ProviderRef   string `json:"id"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason,omitempty"`
}

type Provider interface {
	Submit(ctx context.Context, req SubmitRequest) (Result, error)
	Status(ctx context.Context, providerRef string) (Result, error)
}

type HTTPProvider struct {
	baseURL string
	client  *http.Client
}

func NewHTTPProvider(baseURL string) *HTTPProvider {
	return &HTTPProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (p *HTTPProvider) Submit(ctx context.Context, req SubmitRequest) (Result, error) {
	body, _ := json.Marshal(map[string]any{
		"amount": req.Amount, "currency": req.Currency,
		"destination": req.Destination, "reference": req.Reference,
	})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/payouts", bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)
	return p.do(httpReq)
}

func (p *HTTPProvider) Status(ctx context.Context, providerRef string) (Result, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/payouts/"+url.PathEscape(providerRef), nil)
	if err != nil {
		return Result{}, err
	}
	return p.do(httpReq)
}

func (p *HTTPProvider) do(req *http.Request) (Result, error) {
	resp, err := p.client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("provider unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
		var r Result
		if err := json.Unmarshal(raw, &r); err != nil || r.ProviderRef == "" {
			return Result{}, fmt.Errorf("provider returned an invalid body: %s", raw)
		}
		return r, nil
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity:
		return Result{}, fmt.Errorf("%w: %s", ErrRejected, strings.TrimSpace(string(raw)))
	default:

		return Result{}, fmt.Errorf("provider returned %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
}
