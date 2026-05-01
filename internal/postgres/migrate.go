package postgres

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // register postgres driver
	_ "github.com/golang-migrate/migrate/v4/source/file"       // register file source
)

// Migrate applies all up migrations from sourceURL against dsn. Idempotent
// when the database is already at head. ErrNoChange is treated as success
// because operators expect "migrate up" to be a fence, not a guarantee that
// something happened.
//
// CLAUDE.md is explicit that the ingest service does not run migrations on
// startup; this is a tool for the cmd/migrate binary and integration tests.
func Migrate(sourceURL, dsn string) error {
	m, err := migrate.New(sourceURL, dsn)
	if err != nil {
		return fmt.Errorf("open migrator: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back N steps; pass 0 to roll back everything. Used by
// integration tests and operational rollback only.
func MigrateDown(sourceURL, dsn string, steps int) error {
	m, err := migrate.New(sourceURL, dsn)
	if err != nil {
		return fmt.Errorf("open migrator: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()
	if steps == 0 {
		if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("migrate down: %w", err)
		}
		return nil
	}
	if err := m.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate steps -%d: %w", steps, err)
	}
	return nil
}

// Version returns the current schema version and a "dirty" flag indicating
// that a previous migration crashed mid-run. Returns (0, false, nil) for a
// fresh database.
func Version(sourceURL, dsn string) (uint, bool, error) {
	m, err := migrate.New(sourceURL, dsn)
	if err != nil {
		return 0, false, fmt.Errorf("open migrator: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()
	v, dirty, err := m.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read version: %w", err)
	}
	return v, dirty, nil
}
