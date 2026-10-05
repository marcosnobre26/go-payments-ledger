package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

func main() {
	url := flag.String("url", "http://localhost:8080/v1/webhooks/acmepay", "webhook endpoint")
	secret := flag.String("secret", envOr("WEBHOOK_SECRET", "dev-secret"), "signing secret")
	account := flag.String("account", "", "account id to credit (required)")
	amount := flag.Int64("amount", 1000, "amount in minor units (cents)")
	currency := flag.String("currency", "BRL", "ISO 4217 currency")
	eventID := flag.String("event", "", "event id (random if empty; reuse it to test deduplication)")
	flag.Parse()

	if *account == "" {
		fmt.Fprintln(os.Stderr, "-account is required")
		os.Exit(2)
	}
	if *eventID == "" {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		*eventID = "evt_" + hex.EncodeToString(b)
	}

	var ev webhook.Event
	ev.ID, ev.Type = *eventID, webhook.EventPaymentSucceeded
	ev.Data.AccountID, ev.Data.Amount, ev.Data.Currency = *account, *amount, *currency
	body, _ := json.Marshal(ev)

	req, _ := http.NewRequest(http.MethodPost, *url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.SignatureHeader, webhook.SignatureHeaderValue([]byte(*secret), time.Now().Unix(), body))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("event %s -> %s %s", *eventID, resp.Status, out)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
