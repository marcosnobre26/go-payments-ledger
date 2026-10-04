// Package database opens the PostgreSQL connection and applies migrations.
package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

//go:embed migrations/*.sql
var migrations embed.FS

// Open connects to PostgreSQL, retrying for a while so the service can start
// before the database container is fully ready.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	var pingErr error
	for attempt := 0; attempt < 10; attempt++ {
		if pingErr = db.PingContext(ctx); pingErr == nil {
			return db, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	db.Close()
	return nil, fmt.Errorf("ping database: %w", pingErr)
}

// Migrate applies the embedded SQL files in name order. The files are written
// to be idempotent (IF NOT EXISTS), so running them on every start is safe.
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, name := range files {
		script, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(script)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}
