package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"

	"github.com/marcosnobre26/go-payments-ledger/internal/webhook"
)

type client struct {
	base     string
	provider string
	secret   []byte
	http     *http.Client

	mu     sync.Mutex
	counts map[string]int
}

func main() {
	base := flag.String("base", "http://localhost:8080", "API base URL")
	secret := flag.String("secret", envOr("WEBHOOK_SECRET", "dev-secret"), "webhook signing secret")
	provider := flag.String("provider", "acmepay", "webhook provider name")
	accounts := flag.Int("accounts", 10, "number of accounts to create")
	workers := flag.Int("workers", 4, "concurrent workers")
	duration := flag.Duration("duration", time.Minute, "how long to generate traffic")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	c := &client{
		base: *base, provider: *provider, secret: []byte(*secret),
		http: &http.Client{Timeout: 5 * time.Second}, counts: map[string]int{},
	}

	ids := make([]string, 0, *accounts)
	for i := 0; i < *accounts; i++ {
		id, err := c.createAccount()
		if err != nil {
			fmt.Fprintln(os.Stderr, "create account:", err)
			os.Exit(1)
		}
		ids = append(ids, id)
		c.deposit(id, int64(50_000+mrand.Intn(50_000)), randomID("evt"))
	}
	fmt.Printf("created and funded %d accounts; generating traffic for %s (Ctrl+C to stop)\n", len(ids), *duration)
	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				c.randomAction(ids)
				time.Sleep(time.Duration(50+mrand.Intn(150)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	c.printSummary()
}

func (c *client) randomAction(ids []string) {
	from, to := ids[mrand.Intn(len(ids))], ids[mrand.Intn(len(ids))]
	for to == from {
		to = ids[mrand.Intn(len(ids))]
	}
	switch p := mrand.Intn(100); {
	case p < 42:
		c.transfer(randomID("key"), from, to, int64(100+mrand.Intn(5_000)))
	case p < 50:
		key := randomID("key")
		c.transfer(key, from, to, 1_000)
		c.transfer(key, from, to, 1_000)
	case p < 56:
		c.transfer(randomID("key"), from, to, 10_000_000)
	case p < 72:
		c.deposit(to, int64(1_000+mrand.Intn(20_000)), randomID("evt"))
	case p < 80:
		ev := randomID("evt")
		c.deposit(to, 5_000, ev)
		c.deposit(to, 5_000, ev)
	case p < 90:
		c.payout(randomID("payout"), from, int64(500+mrand.Intn(3_000)), "pix:"+randomID("user")+"@example.com")
	case p < 93:
		c.payout(randomID("payout"), from, 1_000, "reject-closed-account")
	case p < 95:
		key := randomID("payout")
		c.payout(key, from, 1_000, "pix:retry@example.com")
		c.payout(key, from, 1_000, "pix:retry@example.com")
	default:
		c.forgedDeposit(to)
	}
}

func (c *client) payout(key, account string, amount int64, destination string) {
	body, _ := json.Marshal(map[string]any{
		"account_id": account, "amount": amount, "currency": "BRL", "destination": destination,
	})
	req, _ := http.NewRequest(http.MethodPost, c.base+"/v1/payouts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	c.do("payout", req)
}

func (c *client) createAccount() (string, error) {
	resp, err := c.http.Post(c.base+"/v1/accounts", "application/json", bytes.NewBufferString(`{"currency":"BRL"}`))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.ID == "" {
		return "", fmt.Errorf("unexpected response (status %d)", resp.StatusCode)
	}
	return out.ID, nil
}

func (c *client) transfer(key, from, to string, amount int64) {
	body, _ := json.Marshal(map[string]any{
		"from_account_id": from, "to_account_id": to, "amount": amount, "currency": "BRL",
	})
	req, _ := http.NewRequest(http.MethodPost, c.base+"/v1/transfers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	c.do("transfer", req)
}

func (c *client) deposit(account string, amount int64, eventID string) {
	c.sendWebhook(account, amount, eventID, c.secret, "webhook")
}

func (c *client) forgedDeposit(account string) {
	c.sendWebhook(account, 999_999, randomID("evt"), []byte("attacker-secret"), "webhook-forged")
}

func (c *client) sendWebhook(account string, amount int64, eventID string, secret []byte, label string) {
	var ev webhook.Event
	ev.ID, ev.Type = eventID, webhook.EventPaymentSucceeded
	ev.Data.AccountID, ev.Data.Amount, ev.Data.Currency = account, amount, "BRL"
	body, _ := json.Marshal(ev)
	req, _ := http.NewRequest(http.MethodPost, c.base+"/v1/webhooks/"+c.provider, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.SignatureHeader, webhook.SignatureHeaderValue(secret, time.Now().Unix(), body))
	c.do(label, req)
}

func (c *client) do(label string, req *http.Request) {
	resp, err := c.http.Do(req)
	result := "error"
	if err == nil {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		result = resp.Status
	}
	c.mu.Lock()
	c.counts[label+" -> "+result]++
	c.mu.Unlock()
}

func (c *client) printSummary() {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.counts))
	for k := range c.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("\nsummary:")
	for _, k := range keys {
		fmt.Printf("  %-45s %d\n", k, c.counts[k])
	}
}

func randomID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
