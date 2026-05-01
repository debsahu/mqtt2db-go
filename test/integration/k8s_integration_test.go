//go:build integration && k8s

// Kubernetes integration: stands up a kind cluster, installs the
// mqtt2db-go Helm chart, and asserts the StatefulSet rolls out cleanly.
//
// This is gated behind the `k8s` build tag in addition to `integration`
// because it requires `kind` and `helm` on the host PATH and adds a
// non-trivial slice of test time. CI can run this on dedicated jobs.
//
//	go test -tags='integration k8s' -timeout=15m ./test/integration/...
//	make test-k8s
package integration

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireBin fails the test fast if a CLI dep is missing.
func requireBin(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not on PATH; skipping k8s test", name)
	}
}

func runCmd(t *testing.T, ctx context.Context, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func helmDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "..", "..", "deploy", "helm")
}

func TestK8s_HelmInstall_StatefulSetReady(t *testing.T) {
	requireBin(t, "kind")
	requireBin(t, "helm")
	requireBin(t, "kubectl")

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	cluster := fmt.Sprintf("mqtt2db-go-test-%d", time.Now().Unix())
	t.Logf("creating kind cluster %s", cluster)

	if out, err := runCmd(t, ctx, "kind", "create", "cluster", "--name", cluster, "--wait", "120s"); err != nil {
		t.Fatalf("kind create: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, _ = runCmd(t, ctx, "kind", "delete", "cluster", "--name", cluster)
	})

	// Bootstrap a no-op secret with placeholder values so the chart
	// installs even though we have no real broker / DB. We're testing
	// the rollout, not end-to-end traffic — that's what e2e does.
	if out, err := runCmd(t, ctx, "kubectl", "create", "secret", "generic", "mqtt2db-go-secret",
		"--from-literal=MQTT2DB_MQTT_USERNAME=ingest",
		"--from-literal=MQTT2DB_MQTT_PASSWORD=ingest",
		"--from-literal=MQTT2DB_POSTGRES_DSN=postgres://ingest:ingest@nonexistent:5432/telemetry",
	); err != nil {
		t.Fatalf("create secret: %v\n%s", err, out)
	}

	// Build the mqtt2db-go image into the kind cluster's local registry.
	imgTag := fmt.Sprintf("mqtt2db-go:%s", cluster)
	if out, err := runCmd(t, ctx, "docker", "build", "-t", imgTag, "."); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
	if out, err := runCmd(t, ctx, "kind", "load", "docker-image", imgTag, "--name", cluster); err != nil {
		t.Fatalf("kind load: %v\n%s", err, out)
	}

	args := []string{
		"install", "mqtt2db-go", helmDir(t),
		"--set", "replicaCount=1",
		"--set", "image.repository=mqtt2db-go",
		"--set", "image.tag=" + cluster,
		"--set", "image.pullPolicy=Never",
		"--set", "config.mqtt.brokers={tcp://nonexistent:1883}",
		"--set", "config.postgres.dsn=postgres://ingest:ingest@nonexistent:5432/telemetry",
		"--set", "config.dead_letter.s3.bucket=mqtt2db-go-dlq",
		"--set", "secret.existingName=mqtt2db-go-secret",
	}
	if out, err := runCmd(t, ctx, "helm", args...); err != nil {
		t.Fatalf("helm install: %v\n%s", err, out)
	}

	// Wait for the StatefulSet to scale to 1 ready replica.
	rolloutCtx, rolloutCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer rolloutCancel()
	if out, err := runCmd(t, rolloutCtx, "kubectl", "rollout", "status", "statefulset/mqtt2db-go", "--timeout=300s"); err != nil {
		t.Logf("rollout output:\n%s", out)
		t.Fatalf("rollout did not complete: %v", err)
	}

	// Sanity assertion: pod has the expected hostname-derived client ID
	// in its env (or at least is running).
	out, err := runCmd(t, ctx, "kubectl", "get", "pod", "mqtt2db-go-0", "-o", "jsonpath={.status.phase}")
	require.NoError(t, err)
	assert.True(t, strings.Contains(out, "Running"), "pod phase=%s", out)
}
