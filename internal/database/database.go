// Package database opens the PostgreSQL connection and applies migrations.
package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	_ "github.com/lib/pq"
	"io/fs"
	"sort"
	"time"
)

var migrations embed.FS

func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	var pingErr error
	for attempt := 0; attempt < 30; attempt++ {
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

const migrationLockID = 7_426_001

func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID)

	for _, name := range files {
		script, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, string(script)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}
