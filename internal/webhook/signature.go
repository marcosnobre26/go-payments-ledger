// Package webhook receives, verifies, deduplicates and asynchronously
// processes payment-provider webhooks.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader carries "t=<unix seconds>,v1=<hex hmac>", the same scheme
// used by providers such as Stripe.
const SignatureHeader = "X-Signature"

var (
	ErrMissingSignature    = errors.New("missing signature header")
	ErrMalformedSignature  = errors.New("malformed signature header")
	ErrTimestampOutOfRange = errors.New("signature timestamp outside tolerance")
	ErrInvalidSignature    = errors.New("invalid signature")
)

// Sign computes HMAC-SHA256 over "<timestamp>.<body>". Including the
// timestamp in the signed payload prevents replaying an old valid request.
func Sign(secret []byte, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignatureHeaderValue builds the header a provider would send.
func SignatureHeaderValue(secret []byte, timestamp int64, body []byte) string {
	return fmt.Sprintf("t=%d,v1=%s", timestamp, Sign(secret, timestamp, body))
}

// Verify checks the header against the raw request body.
func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	if header == "" {
		return ErrMissingSignature
	}

	var timestamp int64
	var signature string
	for _, part := range strings.Split(header, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrMalformedSignature
		}
		switch key {
		case "t":
			ts, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ErrMalformedSignature
			}
			timestamp = ts
		case "v1":
			signature = value
		}
	}
	if timestamp == 0 || signature == "" {
		return ErrMalformedSignature
	}

	age := now.Sub(time.Unix(timestamp, 0))
	if age > tolerance || age < -tolerance {
		return ErrTimestampOutOfRange
	}

	expected := Sign(secret, timestamp, body)
	// Constant-time comparison avoids leaking information through timing.
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrInvalidSignature
	}
	return nil
}
