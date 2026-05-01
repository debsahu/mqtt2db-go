// Command migrate is the operational entry point for schema migrations.
//
// CLAUDE.md is explicit that the ingest service does not run migrations on
// startup — they are a separate operator-driven step. This binary reads
// the same Postgres DSN as the service and applies migrations from a
// configurable source path (defaults to the bundled ./migrations).
//
// Usage:
//
//	migrate up
//	migrate down [N]
//	migrate version
//
// DSN comes from --dsn or MQTT2DB_POSTGRES_DSN. Source from --source
// (defaults to file://./migrations).
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/version"
)

const (
	defaultSource = "file://./migrations"
	envDSN        = "MQTT2DB_POSTGRES_DSN"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dsn    string
		source string
		showV  bool
	)
	flag.StringVar(&dsn, "dsn", os.Getenv(envDSN), "Postgres DSN (or set "+envDSN+")")
	flag.StringVar(&source, "source", defaultSource, "migrations source URL (e.g. file://./migrations)")
	flag.BoolVar(&showV, "version", false, "print binary version and exit")
	flag.Usage = usage
	flag.Parse()

	if showV {
		fmt.Printf("migrate %s (commit %s, built %s)\n", version.Version, version.Commit, version.BuildDate)
		return nil
	}

	args := flag.Args()
	if len(args) == 0 {
		usage()
		return errors.New("missing subcommand")
	}
	if dsn == "" {
		return fmt.Errorf("dsn required (use --dsn or %s)", envDSN)
	}

	switch cmd := args[0]; cmd {
	case "up":
		if err := postgres.Migrate(source, dsn); err != nil {
			return err
		}
		fmt.Println("up: ok")
		return nil

	case "down":
		steps := 1
		if len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n < 0 {
				return fmt.Errorf("down: steps must be a non-negative integer (0 = all), got %q", args[1])
			}
			steps = n
		}
		if err := postgres.MigrateDown(source, dsn, steps); err != nil {
			return err
		}
		fmt.Printf("down: ok (steps=%d)\n", steps)
		return nil

	case "version":
		v, dirty, err := postgres.Version(source, dsn)
		if err != nil {
			return err
		}
		fmt.Printf("version=%d dirty=%t\n", v, dirty)
		return nil

	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `migrate — schema management for mqtt2db-go

Usage:
  migrate [flags] up                  apply all pending up migrations
  migrate [flags] down [N]            roll back N migrations (default 1, 0 = all)
  migrate [flags] version             print current schema version

Flags:
`)
	flag.PrintDefaults()
}
