package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

var (
	ErrAccountNotFound      = errors.New("account not found")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrCurrencyMismatch     = errors.New("currency mismatch")
	ErrInvalidAmount        = errors.New("amount must be a positive integer in minor units")
	ErrInvalidCurrency      = errors.New("currency must be a 3-letter ISO 4217 code")
	ErrSameAccount          = errors.New("source and destination accounts must differ")
	ErrIdempotencyKeyReused = errors.New("idempotency key already used with a different request")
	ErrTransactionNotFound  = errors.New("transaction not found")
)

var (
	currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

func IsPermanent(err error) bool {
	for _, target := range []error{
		ErrAccountNotFound, ErrInsufficientFunds, ErrCurrencyMismatch,
		ErrInvalidAmount, ErrInvalidCurrency, ErrSameAccount,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

type Account struct {
	ID        string    `json:"id"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

type Entry struct {
	TransactionID string    `json:"transaction_id"`
	Kind          string    `json:"kind"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

type Transfer struct {
	ID            string    `json:"id"`
	FromAccountID string    `json:"from_account_id"`
	ToAccountID   string    `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

type TransferInput struct {
	FromAccountID string `json:"from_account_id"`
	ToAccountID   string `json:"to_account_id"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
}

func (in TransferInput) validate() error {
	if !currencyRe.MatchString(in.Currency) {
		return ErrInvalidCurrency
	}
	if in.Amount <= 0 {
		return ErrInvalidAmount
	}
	if !uuidRe.MatchString(in.FromAccountID) || !uuidRe.MatchString(in.ToAccountID) {
		return ErrAccountNotFound
	}
	if in.FromAccountID == in.ToAccountID {
		return ErrSameAccount
	}
	return nil
}

func (in TransferInput) hash() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s", in.FromAccountID, in.ToAccountID, in.Amount, in.Currency)))
	return hex.EncodeToString(sum[:])
}

type Service struct {
	db *sql.DB
}

func NewService(db *sql.DB) *Service {
	return &Service{db: db}
}

func (s *Service) CreateAccount(ctx context.Context, currency string) (Account, error) {
	if !currencyRe.MatchString(currency) {
		return Account{}, ErrInvalidCurrency
	}
	var a Account
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO accounts (currency) VALUES ($1) RETURNING id, currency, balance, created_at`,
		currency,
	).Scan(&a.ID, &a.Currency, &a.Balance, &a.CreatedAt)
	return a, err
}

func (s *Service) GetAccount(ctx context.Context, id string) (Account, error) {
	if !uuidRe.MatchString(id) {
		return Account{}, ErrAccountNotFound
	}
	var a Account
	err := s.db.QueryRowContext(ctx,
		`SELECT id, currency, balance, created_at FROM accounts WHERE id = $1 AND NOT is_system`, id,
	).Scan(&a.ID, &a.Currency, &a.Balance, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	return a, err
}

func (s *Service) ListEntries(ctx context.Context, accountID string, limit int) ([]Entry, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.transaction_id, t.kind, e.amount, e.currency, e.created_at
		FROM ledger_entries e
		JOIN transactions t ON t.id = e.transaction_id
		WHERE e.account_id = $1
		ORDER BY e.id DESC
		LIMIT $2`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := []Entry{}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.TransactionID, &e.Kind, &e.Amount, &e.Currency, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *Service) Transfer(ctx context.Context, idempotencyKey string, in TransferInput) (t Transfer, replayed bool, err error) {
	if err := in.validate(); err != nil {
		return Transfer{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Transfer{}, false, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		idempotencyKey, in.hash())
	if err != nil {
		return Transfer{}, false, err
	}
	if inserted, _ := res.RowsAffected(); inserted == 0 {
		var storedHash string
		var txID sql.NullString
		if err := tx.QueryRowContext(ctx,
			`SELECT request_hash, transaction_id FROM idempotency_keys WHERE key = $1`, idempotencyKey,
		).Scan(&storedHash, &txID); err != nil {
			return Transfer{}, false, err
		}
		if storedHash != in.hash() || !txID.Valid {
			return Transfer{}, false, ErrIdempotencyKeyReused
		}
		t, err := s.getTransfer(ctx, tx, txID.String)
		return t, true, err
	}

	txID, createdAt, err := post(ctx, tx, "transfer", nil, in.FromAccountID, in.ToAccountID, in.Amount, in.Currency)
	if err != nil {
		return Transfer{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE idempotency_keys SET transaction_id = $2 WHERE key = $1`, idempotencyKey, txID,
	); err != nil {
		return Transfer{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Transfer{}, false, err
	}

	return Transfer{
		ID: txID, FromAccountID: in.FromAccountID, ToAccountID: in.ToAccountID,
		Amount: in.Amount, Currency: in.Currency, CreatedAt: createdAt,
	}, false, nil
}

func (s *Service) Deposit(ctx context.Context, accountID string, amount int64, currency, reference string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.DepositTx(ctx, tx, accountID, amount, currency, reference); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) DepositTx(ctx context.Context, tx *sql.Tx, accountID string, amount int64, currency, reference string) error {
	if !currencyRe.MatchString(currency) {
		return ErrInvalidCurrency
	}
	if !uuidRe.MatchString(accountID) {
		return ErrAccountNotFound
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO accounts (currency, is_system) VALUES ($1, true)
		ON CONFLICT (currency) WHERE is_system DO NOTHING`, currency); err != nil {
		return err
	}
	var settlementID string
	if err := tx.QueryRowContext(ctx,
		`SELECT id FROM accounts WHERE is_system AND currency = $1`, currency,
	).Scan(&settlementID); err != nil {
		return err
	}

	_, _, err := post(ctx, tx, "deposit", &reference, settlementID, accountID, amount, currency)
	return err
}

func post(ctx context.Context, tx *sql.Tx, kind string, reference *string, from, to string, amount int64, currency string) (string, time.Time, error) {
	ids := []string{from, to}
	sort.Strings(ids)

	type locked struct {
		currency string
		balance  int64
		system   bool
	}
	accounts := make(map[string]locked, 2)
	for _, id := range ids {
		var l locked
		err := tx.QueryRowContext(ctx,
			`SELECT currency, balance, is_system FROM accounts WHERE id = $1 FOR UPDATE`, id,
		).Scan(&l.currency, &l.balance, &l.system)
		if errors.Is(err, sql.ErrNoRows) {
			return "", time.Time{}, ErrAccountNotFound
		}
		if err != nil {
			return "", time.Time{}, err
		}
		accounts[id] = l
	}

	src, dst := accounts[from], accounts[to]
	if src.currency != currency || dst.currency != currency {
		return "", time.Time{}, ErrCurrencyMismatch
	}
	if !src.system && src.balance < amount {
		return "", time.Time{}, ErrInsufficientFunds
	}

	var txID string
	var createdAt time.Time
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO transactions (kind, reference) VALUES ($1, $2) RETURNING id, created_at`, kind, reference,
	).Scan(&txID, &createdAt); err != nil {
		return "", time.Time{}, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, amount, currency)
		VALUES ($1, $2, $3, $5), ($1, $4, -$3::BIGINT, $5)`,
		txID, to, amount, from, currency); err != nil {
		return "", time.Time{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET balance = balance - $2 WHERE id = $1`, from, amount); err != nil {
		return "", time.Time{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, to, amount); err != nil {
		return "", time.Time{}, err
	}
	return txID, createdAt, nil
}

func (s *Service) getTransfer(ctx context.Context, tx *sql.Tx, id string) (Transfer, error) {
	var t Transfer
	err := tx.QueryRowContext(ctx, `
		SELECT t.id, t.created_at,
		       MAX(CASE WHEN e.amount < 0 THEN e.account_id::text END),
		       MAX(CASE WHEN e.amount > 0 THEN e.account_id::text END),
		       MAX(CASE WHEN e.amount > 0 THEN e.amount END),
		       MAX(e.currency)
		FROM transactions t
		JOIN ledger_entries e ON e.transaction_id = t.id
		WHERE t.id = $1 AND t.kind = 'transfer'
		GROUP BY t.id, t.created_at`, id,
	).Scan(&t.ID, &t.CreatedAt, &t.FromAccountID, &t.ToAccountID, &t.Amount, &t.Currency)
	if errors.Is(err, sql.ErrNoRows) {
		return Transfer{}, ErrTransactionNotFound
	}
	return t, err
}
