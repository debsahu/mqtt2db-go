// Package version exposes build-time identity for the binary.
// Values are injected via -ldflags at build time; defaults indicate a
// non-release build (e.g. `go run`).
package version

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)
