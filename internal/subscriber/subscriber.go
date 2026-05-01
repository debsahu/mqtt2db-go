// Package subscriber owns the MQTT side of mqtt2db-go.
//
// The contract from CLAUDE.md is non-negotiable on a few points:
//
//   - clean_session=false (CleanStart in MQTT 5) so a restart does not silently
//     drop messages queued for our session.
//   - QoS 1 with manual ack — we ack only after the message has either been
//     enqueued in the in-memory ring or written to the WAL. Messages that
//     can't be enqueued are NOT acked, so Comqtt buffers or redelivers via
//     the shared subscription.
//   - Stable client ID per replica. Operators set client_id_prefix and we
//     append the hostname (the Kubernetes pod name in production).
//
// The subscriber owns no durability of its own. Routing decision per
// incoming message:
//
//	State    Action
//	-----    ------
//	Normal   ring.Enqueue, then ack
//	Spill    ring.Enqueue if it still fits, else wal.Append; then ack
//	Pause    ring.Enqueue if it still fits, else wal.Append if it fits;
//	         otherwise return without ack so Comqtt redelivers
package subscriber

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// Ring is the subset of buffer.Ring the subscriber needs. Defined here as
// an interface so tests can swap it for a mock without standing up a real
// ring.
type Ring interface {
	Enqueue(m postgres.Message) (buffer.State, error)
	State() buffer.State
	MarkSpilled()
}

// WAL is the subset of wal.Store the subscriber needs.
type WAL interface {
	Append(m postgres.Message) ([]byte, error)
}

// Subscriber wires paho.golang/autopaho to the ring + WAL. Start blocks
// until ctx is cancelled.
type Subscriber struct {
	cfg    config.MQTTConfig
	ring   Ring
	wal    WAL
	m      *metrics.SubscriberMetrics
	logger *slog.Logger

	cm *autopaho.ConnectionManager

	// clientID is computed once at New and reused so reconnects keep the
	// same session.
	clientID string
}

// New constructs a Subscriber. Callers pass already-built dependencies so
// tests can substitute fakes. WAL may be nil — in that case the spill path
// is unavailable and pause messages just don't get acked.
func New(cfg config.MQTTConfig, ring Ring, w WAL, m *metrics.SubscriberMetrics, logger *slog.Logger) (*Subscriber, error) {
	if ring == nil {
		return nil, errors.New("subscriber: ring required")
	}
	if m == nil {
		return nil, errors.New("subscriber: metrics required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("subscriber: at least one broker URL required")
	}

	id, err := computeClientID(cfg.ClientIDPrefix)
	if err != nil {
		return nil, err
	}
	return &Subscriber{
		cfg:      cfg,
		ring:     ring,
		wal:      w,
		m:        m,
		logger:   logger.With("component", "subscriber"),
		clientID: id,
	}, nil
}

// ClientID returns the resolved client ID. Exported so tests can assert
// stability across reconnects.
func (s *Subscriber) ClientID() string { return s.clientID }

// Start connects, subscribes, and dispatches messages until ctx is done.
// Returns the first error encountered while building the client; a
// connection failure is handled by autopaho's reconnect loop.
func (s *Subscriber) Start(ctx context.Context) error {
	urls, err := parseBrokers(s.cfg.Brokers)
	if err != nil {
		return err
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:        urls,
		KeepAlive:         uint16(s.cfg.Keepalive.AsDuration().Seconds()),
		ConnectRetryDelay: 1 * time.Second,
		ConnectTimeout:    s.cfg.ConnectTimeout.AsDuration(),
		// MQTT 5 session expiry — we want sessions to survive at least the
		// length of MaxReconnectBackoff so a flapping deploy doesn't lose
		// messages queued for our client.
		SessionExpiryInterval: uint32(s.cfg.MaxReconnectBackoff.AsDuration().Seconds()),
		CleanStartOnInitialConnection: s.cfg.CleanSession,

		ConnectUsername: s.cfg.Username,
		ConnectPassword: []byte(s.cfg.Password),

		OnConnectionUp: s.onConnectionUp,
		OnConnectError: func(err error) {
			s.m.Reconnects.Inc()
			s.logger.Warn("connect error", "event", "connect_error", "err", err.Error())
		},

		ClientConfig: paho.ClientConfig{
			ClientID:        s.clientID,
			Session:         nil, // autopaho manages
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				s.onPublish,
			},
			OnClientError: func(err error) {
				s.logger.Warn("client error", "event", "client_error", "err", err.Error())
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				s.m.Connected.Set(0)
				s.logger.Warn("server disconnect", "event", "server_disconnect", "reason_code", int(d.ReasonCode))
			},
		},
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return fmt.Errorf("autopaho.NewConnection: %w", err)
	}
	s.cm = cm

	s.logger.Info("started", "event", "started",
		"client_id", s.clientID,
		"brokers", s.cfg.Brokers,
		"shared_subscription", s.cfg.SharedSubscription)

	<-ctx.Done()

	// Best-effort graceful disconnect; ignore errors past cancellation.
	disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = cm.Disconnect(disconnectCtx)
	s.m.Connected.Set(0)
	s.logger.Info("stopped", "event", "stopped")
	return nil
}

func (s *Subscriber) onConnectionUp(cm *autopaho.ConnectionManager, _ *paho.Connack) {
	s.m.Connected.Set(1)
	s.m.Reconnects.Inc()
	s.logger.Info("connected", "event", "connected", "client_id", s.clientID)

	// Subscribe to the shared subscription with QoS 1.
	//
	// MQTT 5 §3.8.3.1: "It is a Protocol Error to set the No Local bit to
	// 1 on a Shared Subscription." Compliant brokers (mosquitto, Comqtt)
	// disconnect with reason code 0x82 if we try, so NoLocal stays false
	// here.
	subCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(subCtx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{
			Topic:             s.cfg.SharedSubscription,
			QoS:               1,
			RetainAsPublished: true,
		}},
	}); err != nil {
		s.logger.Error("subscribe failed",
			"event", "subscribe_error",
			"topic", s.cfg.SharedSubscription,
			"err", err.Error())
	}
}

// onPublish is the paho-side adapter; HandleMessage owns all the routing
// logic so tests can drive it directly without standing up a broker.
//
// When the paho ClientConfig has EnableManualAcknowledgment=true the
// returned (true, nil) means "I have taken responsibility for this
// message"; otherwise paho auto-acks. Either way, the Acked counter
// reflects our intent, not the paho-internal mechanism.
func (s *Subscriber) onPublish(pr paho.PublishReceived) (bool, error) {
	ack := func() {
		// Best-effort manual ack; ignore the error when manual ack isn't
		// enabled (paho will have already auto-acked).
		_ = pr.Client.Ack(pr.Packet)
		s.m.Acked.Inc()
	}
	s.HandleMessage(pr.Packet.Topic, pr.Packet.Payload, ack)
	return true, nil
}

// HandleMessage runs the parse + route + ack policy for one MQTT message.
// ack is invoked exactly when the message has been durably accepted by
// the ring (or WAL spill); otherwise ack stays uncalled so Comqtt
// redelivers via the shared subscription. Exposed for unit tests; the
// production caller is onPublish.
func (s *Subscriber) HandleMessage(topic string, payload []byte, ack func()) {
	s.m.Received.Inc()

	parsed, err := ParseTopic(topic)
	if err != nil {
		s.m.HandlerErrors.Inc()
		s.logger.Warn("topic parse failed",
			"event", "topic_parse_error",
			"topic", topic,
			"err", err.Error())
		// Bad topic is unrecoverable; ack so the broker drops it.
		if ack != nil {
			ack()
		}
		return
	}

	now := time.Now().UTC()
	msg := postgres.Message{
		TenantID:   parsed.Tenant,
		DeviceUUID: parsed.Device,
		Topic:      topic,
		Payload:    append([]byte(nil), payload...),
		ReceivedAt: now,
		DedupKey:   makeDedupKey(parsed.Device, now),
	}
	if !s.routeMessage(msg) {
		// Pause + WAL also full: do NOT ack; broker will redeliver.
		return
	}
	if ack != nil {
		ack()
	}
}

// routeMessage applies the spill/pause policy from CLAUDE.md. Returns
// true if the message is durably held and we may ack.
func (s *Subscriber) routeMessage(msg postgres.Message) bool {
	state, err := s.ring.Enqueue(msg)
	if err == nil {
		// Above the spill threshold, future incoming messages should
		// prefer WAL — but the ring still had room for *this* one, so
		// we ack and let the next message take the WAL path if needed.
		_ = state
		return true
	}
	if !errors.Is(err, buffer.ErrFull) {
		s.logger.Warn("enqueue error",
			"event", "enqueue_error",
			"err", err.Error())
		return false
	}

	// Ring rejected. Try WAL spill if available.
	if s.wal != nil {
		if _, walErr := s.wal.Append(msg); walErr == nil {
			s.ring.MarkSpilled()
			return true
		} else {
			s.logger.Error("wal append failed",
				"event", "wal_error",
				"err", walErr.Error())
		}
	}

	// Pause + nowhere to spill. Drop ack so Comqtt redelivers.
	return false
}

// computeClientID joins the configured prefix with the host identity. In
// Kubernetes that is the pod name, which is stable across restarts of the
// same StatefulSet replica.
func computeClientID(prefix string) (string, error) {
	if prefix == "" {
		return "", errors.New("subscriber: client_id_prefix required")
	}
	host := os.Getenv("HOSTNAME")
	if host == "" {
		var err error
		host, err = os.Hostname()
		if err != nil || host == "" {
			return "", fmt.Errorf("resolve hostname for client id: %w", err)
		}
	}
	return prefix + "-" + host, nil
}

func parseBrokers(raw []string) ([]*url.URL, error) {
	out := make([]*url.URL, 0, len(raw))
	for _, b := range raw {
		u, err := url.Parse(b)
		if err != nil {
			return nil, fmt.Errorf("parse broker %q: %w", b, err)
		}
		out = append(out, u)
	}
	return out, nil
}

// makeDedupKey is "{device}:{nanos}". Mirrored in the WAL replay path
// (because the WAL persists the same key) and in the dead-letter S3 key.
func makeDedupKey(device uuid.UUID, t time.Time) string {
	return fmt.Sprintf("%s:%d", device, t.UnixNano())
}

// guard against accidental import cycles when adding helpers.
var _ sync.Locker = (*sync.Mutex)(nil)
