// Package testutil provides a migrated PostgreSQL connection for integration tests.
package testutil

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/marcosnobre26/go-payments-ledger/internal/database"
)

// DB returns a connection to TEST_DATABASE_URL, or skips the test when the
// variable is not set (so `go test ./...` works without a database).
func DB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	db, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
