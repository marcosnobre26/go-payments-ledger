package webhook

import (
	"errors"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	secret := []byte("test-secret")
	body := []byte(`{"id":"evt_1","type":"payment.succeeded"}`)
	now := time.Unix(1_700_000_000, 0)
	valid := SignatureHeaderValue(secret, now.Unix(), body)

	tests := []struct {
		name   string
		secret []byte
		header string
		body   []byte
		now    time.Time
		want   error
	}{
		{"valid signature", secret, valid, body, now, nil},
		{"tampered body", secret, valid, []byte(`{"id":"evt_1","type":"payment.refunded"}`), now, ErrInvalidSignature},
		{"wrong secret", []byte("other"), valid, body, now, ErrInvalidSignature},
		{"replayed after tolerance", secret, valid, body, now.Add(6 * time.Minute), ErrTimestampOutOfRange},
		{"timestamp from the future", secret, valid, body, now.Add(-6 * time.Minute), ErrTimestampOutOfRange},
		{"missing header", secret, "", body, now, ErrMissingSignature},
		{"malformed header", secret, "garbage", body, now, ErrMalformedSignature},
		{"missing v1", secret, "t=1700000000", body, now, ErrMalformedSignature},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.secret, tc.header, tc.body, tc.now, 5*time.Minute)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBackoffIsCapped(t *testing.T) {
	if got := backoff(1); got != 2*time.Second {
		t.Fatalf("backoff(1) = %v, want 2s", got)
	}
	if got := backoff(30); got != 10*time.Minute {
		t.Fatalf("backoff(30) = %v, want 10m cap", got)
	}
}
