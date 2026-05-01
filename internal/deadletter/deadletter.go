// Package deadletter persists messages that exhausted retries against
// PostgreSQL. Each message is written as a single JSON object to the
// configured S3-compatible bucket (RustFS in dev, AWS S3 in production).
//
// Key shape:
//
//	dead-letter/{date}/{tenant}/{device}/{nanos}-{seq}.json
//
// Date provides cheap retention windows; tenant + device let an operator
// scope a recovery scan to a single fleet.
//
// The contract from CLAUDE.md is "do not silently drop". The flusher
// hands a message here only after MaxRetries+1 failed PG inserts.
package deadletter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// S3API narrows the dependency on the AWS SDK to just what we use, which
// makes mocking trivial in tests.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// Sink writes failed messages to an S3-compatible bucket.
type Sink struct {
	cfg config.S3Config
	api S3API
	m   *metrics.DeadLetterMetrics
	seq atomic.Uint64
}

// NewSink wires an AWS SDK v2 S3 client. Static creds are honored if the
// caller set them; otherwise the AWS default credential chain runs (env,
// config file, IRSA, instance metadata).
func NewSink(ctx context.Context, cfg config.S3Config, m *metrics.DeadLetterMetrics) (*Sink, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("deadletter: bucket required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if m == nil {
		return nil, errors.New("deadletter: metrics required")
	}

	loadOpts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		loadOpts = append(loadOpts, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awscfgValue, err := awscfg.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			endpoint := cfg.Endpoint
			if !strings.Contains(endpoint, "://") {
				if cfg.UseSSL {
					endpoint = "https://" + endpoint
				} else {
					endpoint = "http://" + endpoint
				}
			}
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = cfg.UsePathStyle
		})
	} else if cfg.UsePathStyle {
		clientOpts = append(clientOpts, func(o *s3.Options) { o.UsePathStyle = true })
	}

	api := s3.NewFromConfig(awscfgValue, clientOpts...)
	return NewSinkWithAPI(cfg, api, m), nil
}

// NewSinkWithAPI is the testing seam: wraps a hand-built S3API.
func NewSinkWithAPI(cfg config.S3Config, api S3API, m *metrics.DeadLetterMetrics) *Sink {
	return &Sink{cfg: cfg, api: api, m: m}
}

// HealthCheck pings the bucket so /readyz can fail fast if S3 is
// unreachable.
func (s *Sink) HealthCheck(ctx context.Context) error {
	_, err := s.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.cfg.Bucket)})
	return err
}

// envelope is the on-disk shape. Mirrors postgres.Message + a freeform
// "error" string (populated from the flusher's last attempt).
type envelope struct {
	Timestamp  time.Time `json:"timestamp"`
	TenantID   string    `json:"tenant_id"`
	DeviceUUID string    `json:"device_uuid"`
	Topic      string    `json:"topic"`
	Payload    []byte    `json:"payload"` // JSON marshals as base64
	ReceivedAt time.Time `json:"received_at"`
	DedupKey   string    `json:"dedup_key"`
	LastError  string    `json:"last_error,omitempty"`
}

// Write serialises msg with optional lastErr and uploads to S3.
func (s *Sink) Write(ctx context.Context, msg postgres.Message, lastErr error) error {
	now := time.Now().UTC()
	env := envelope{
		Timestamp:  now,
		TenantID:   msg.TenantID,
		DeviceUUID: msg.DeviceUUID.String(),
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		ReceivedAt: msg.ReceivedAt,
		DedupKey:   msg.DedupKey,
	}
	if lastErr != nil {
		env.LastError = lastErr.Error()
	}
	body, err := json.Marshal(env)
	if err != nil {
		s.m.Errors.Inc()
		return fmt.Errorf("marshal envelope: %w", err)
	}

	key := s.makeKey(msg, now)

	start := time.Now()
	_, err = s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	s.m.Latency.Observe(time.Since(start).Seconds())
	if err != nil {
		s.m.Errors.Inc()
		return fmt.Errorf("put %s: %w", key, err)
	}
	s.m.Written.Inc()
	return nil
}

func (s *Sink) makeKey(msg postgres.Message, now time.Time) string {
	tenant := msg.TenantID
	if tenant == "" {
		tenant = "unknown"
	}
	device := msg.DeviceUUID.String()
	if device == "00000000-0000-0000-0000-000000000000" {
		device = "unknown"
	}
	seq := s.seq.Add(1)
	return fmt.Sprintf("dead-letter/%s/%s/%s/%d-%d.json",
		now.UTC().Format("2006-01-02"), tenant, device, msg.ReceivedAt.UnixNano(), seq)
}
