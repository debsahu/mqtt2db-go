//go:build integration

// Subscriber integration test: spins up Comqtt (built from
// deploy/comqtt/Dockerfile) via testcontainers, connects the subscriber,
// publishes via a one-shot paho client, and asserts the ring received
// the message. ADR 0004 captures why we build Comqtt from source
// instead of substituting another broker.
package integration

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/subscriber"
)

// comqttContext returns the absolute path to deploy/comqtt so
// testcontainers can build the broker image. Resolved from this test
// file's location so the test is CWD-agnostic.
func comqttContext(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "..", "..", "deploy", "comqtt")
}

// ringRecorder satisfies subscriber.Ring and just collects what the
// subscriber routes to it.
type ringRecorder struct {
	mu   sync.Mutex
	msgs []postgres.Message
}

func (r *ringRecorder) Enqueue(m postgres.Message) (buffer.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return buffer.StateNormal, nil
}
func (r *ringRecorder) State() buffer.State { return buffer.StateNormal }
func (r *ringRecorder) MarkSpilled()        {}

func (r *ringRecorder) Snapshot() []postgres.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]postgres.Message(nil), r.msgs...)
}

// startBroker builds Comqtt from deploy/comqtt/Dockerfile and runs a
// single-node instance. Returns the tcp:// broker URL. The build is
// cached after the first run, so subsequent tests in the same Docker
// daemon are fast.
func startBroker(tb testing.TB, ctx context.Context) string {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    comqttContext(tb),
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts: []string{"1883/tcp"},
		Cmd:          []string{"--tcp=:1883", "--ws=:1882", "--http=:8080"},
		WaitingFor:   wait.ForListeningPort("1883/tcp").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})

	host, err := c.Host(ctx)
	require.NoError(tb, err)
	mappedPort, err := c.MappedPort(ctx, "1883/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("tcp://%s:%s", host, mappedPort.Port())
}

func publishOne(tb testing.TB, broker, topic string, payload []byte) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bu, err := url.Parse(broker)
	require.NoError(tb, err)
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:     []*url.URL{bu},
		ConnectTimeout: 5 * time.Second,
		KeepAlive:      10,
		ClientConfig:   paho.ClientConfig{ClientID: "pub-" + uuid.NewString()},
	})
	require.NoError(tb, err)
	require.NoError(tb, cm.AwaitConnection(ctx))
	_, err = cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: payload})
	require.NoError(tb, err)
	_ = cm.Disconnect(ctx)
}

func TestSubscriber_RealComqtt_DeliversThroughSharedSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	broker := startBroker(t, ctx)

	ring := &ringRecorder{}
	reg := metrics.NewRegistry()
	subMetrics := metrics.NewSubscriberMetrics(reg)

	cfg := config.MQTTConfig{
		Brokers:             []string{broker},
		ClientIDPrefix:      "mqtt2db-go-itest",
		SharedSubscription:  "$share/ingest/t/+/d/+/evt/#",
		Keepalive:           config.Duration(10 * time.Second),
		ConnectTimeout:      config.Duration(5 * time.Second),
		MaxReconnectBackoff: config.Duration(10 * time.Second),
		ProtocolVersion:     5,
	}
	sub, err := subscriber.New(cfg, ring, nil, subMetrics, nil)
	require.NoError(t, err)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	go func() { _ = sub.Start(subCtx) }()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(subMetrics.Connected) == 1
	}, 30*time.Second, 100*time.Millisecond, "subscriber never reported connected")

	dev := uuid.New()
	topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
	publishOne(t, broker, topic, []byte(`{"hello":"comqtt"}`))

	require.Eventually(t, func() bool {
		return len(ring.Snapshot()) == 1
	}, 15*time.Second, 100*time.Millisecond, "ring never observed the published message")

	got := ring.Snapshot()[0]
	assert.Equal(t, "acme", got.TenantID)
	assert.Equal(t, dev, got.DeviceUUID)
	assert.Equal(t, []byte(`{"hello":"comqtt"}`), got.Payload)
	assert.Equal(t, float64(1), testutil.ToFloat64(subMetrics.Acked))
}

func TestSubscriber_RealComqtt_SharedSubLoadBalancesAcrossReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	broker := startBroker(t, ctx)

	const replicas = 3
	rings := make([]*ringRecorder, replicas)
	subs := make([]*subscriber.Subscriber, replicas)
	subMetrics := make([]*metrics.SubscriberMetrics, replicas)

	for i := 0; i < replicas; i++ {
		t.Setenv("HOSTNAME", fmt.Sprintf("replica-%d", i))
		rings[i] = &ringRecorder{}
		reg := metrics.NewRegistry()
		subMetrics[i] = metrics.NewSubscriberMetrics(reg)
		s, err := subscriber.New(config.MQTTConfig{
			Brokers:             []string{broker},
			ClientIDPrefix:      "mqtt2db-go-itest",
			SharedSubscription:  "$share/ingest/t/+/d/+/evt/#",
			Keepalive:           config.Duration(10 * time.Second),
			ConnectTimeout:      config.Duration(5 * time.Second),
			MaxReconnectBackoff: config.Duration(10 * time.Second),
			ProtocolVersion:     5,
		}, rings[i], nil, subMetrics[i], nil)
		require.NoError(t, err)
		subs[i] = s
	}

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	for _, s := range subs {
		s := s
		go func() { _ = s.Start(subCtx) }()
	}

	for i := range subMetrics {
		i := i
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(subMetrics[i].Connected) == 1
		}, 30*time.Second, 100*time.Millisecond, "replica %d never connected", i)
	}

	const total = 60
	for n := 0; n < total; n++ {
		dev := uuid.New()
		topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
		publishOne(t, broker, topic, []byte(fmt.Sprintf(`{"seq":%d}`, n)))
	}

	require.Eventually(t, func() bool {
		var got int
		for _, r := range rings {
			got += len(r.Snapshot())
		}
		return got == total
	}, 30*time.Second, 100*time.Millisecond,
		"sum of ring sizes across replicas never reached %d", total)

	// Every replica must have received >=1 message; the shared sub is a
	// load balancer, not a fan-out. We don't assert exactly even
	// distribution because Comqtt's strategy is implementation-defined.
	var totalSeen atomic.Int64
	for i, r := range rings {
		n := len(r.Snapshot())
		totalSeen.Add(int64(n))
		assert.Greater(t, n, 0, "replica %d received nothing — load balancing broken?", i)
	}
	assert.Equal(t, int64(total), totalSeen.Load())
}
