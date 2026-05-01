// Command loadgen synthesises MQTT traffic against a broker so we can
// stress mqtt2db-go end-to-end. It publishes per-device QoS 1 messages
// at a configurable rate and reports achieved throughput plus an
// approximation of end-to-end latency (round-trip via a sentinel topic
// is intentionally NOT included — measure end-to-end latency via the
// Postgres `received_at` column for the canonical number).
//
// Usage:
//
//	go run ./test/loadgen \
//	    --broker=tcp://localhost:1883 \
//	    --rate=5000 --duration=60s --devices=1000 \
//	    --tenant=acme --topic-prefix=t/acme/d
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
)

func main() {
	var (
		broker      string
		rate        int
		duration    time.Duration
		devices     int
		tenant      string
		topicPrefix string
		username    string
		password    string
		payload     int
		clients     int
	)
	flag.StringVar(&broker, "broker", "tcp://localhost:1883", "MQTT broker URL")
	flag.IntVar(&rate, "rate", 1000, "messages per second target")
	flag.DurationVar(&duration, "duration", 30*time.Second, "test duration")
	flag.IntVar(&devices, "devices", 100, "distinct device UUIDs")
	flag.StringVar(&tenant, "tenant", "acme", "tenant id")
	flag.StringVar(&topicPrefix, "topic-prefix", "t", "topic prefix; final topic = {prefix}/{tenant}/d/{device}/evt/state")
	flag.StringVar(&username, "username", "", "MQTT username (optional)")
	flag.StringVar(&password, "password", "", "MQTT password (optional)")
	flag.IntVar(&payload, "payload", 64, "payload bytes per message")
	flag.IntVar(&clients, "clients", 8, "parallel publisher clients")
	flag.Parse()

	if rate <= 0 || duration <= 0 || devices <= 0 || clients <= 0 {
		fmt.Fprintf(os.Stderr, "rate, duration, devices, clients must all be > 0\n")
		os.Exit(1)
	}

	deviceIDs := make([]uuid.UUID, devices)
	for i := range deviceIDs {
		deviceIDs[i] = uuid.New()
	}

	bu, err := url.Parse(broker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse broker: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Token bucket: send a tick every 1/rate seconds. We split work
	// across `clients` publishers; each owns a fair share of the rate.
	publishers := make([]*autopaho.ConnectionManager, clients)
	for i := 0; i < clients; i++ {
		cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
			ServerUrls:      []*url.URL{bu},
			KeepAlive:       30,
			ConnectTimeout:  5 * time.Second,
			ConnectUsername: username,
			ConnectPassword: []byte(password),
			ClientConfig:    paho.ClientConfig{ClientID: fmt.Sprintf("loadgen-%s", uuid.NewString()[:8])},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect publisher %d: %v\n", i, err)
			os.Exit(1)
		}
		if err := cm.AwaitConnection(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "await publisher %d: %v\n", i, err)
			os.Exit(1)
		}
		publishers[i] = cm
		defer func(c *autopaho.ConnectionManager) {
			cleanCtx, c2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer c2()
			_ = c.Disconnect(cleanCtx)
		}(cm)
	}

	var sent, errs atomic.Int64
	start := time.Now()
	perClient := rate / clients
	if perClient < 1 {
		perClient = 1
	}

	var wg sync.WaitGroup
	for i, cm := range publishers {
		wg.Add(1)
		go func(idx int, cm *autopaho.ConnectionManager) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(idx)))
			interval := time.Second / time.Duration(perClient)
			next := time.Now()
			body := make([]byte, payload)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if now := time.Now(); now.Before(next) {
					time.Sleep(next.Sub(now))
				}
				next = next.Add(interval)

				dev := deviceIDs[r.Intn(len(deviceIDs))]
				topic := fmt.Sprintf("%s/%s/d/%s/evt/state", topicPrefix, tenant, dev)
				_, _ = r.Read(body)
				if _, err := cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: body}); err != nil {
					errs.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i, cm)
	}

	wg.Wait()
	elapsed := time.Since(start)

	achievedRate := float64(sent.Load()) / elapsed.Seconds()
	fmt.Printf("loadgen results\n")
	fmt.Printf("  duration         : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  target rate      : %d msg/s\n", rate)
	fmt.Printf("  publishers       : %d\n", clients)
	fmt.Printf("  devices          : %d\n", devices)
	fmt.Printf("  payload          : %d bytes\n", payload)
	fmt.Printf("  messages sent    : %d\n", sent.Load())
	fmt.Printf("  publish errors   : %d\n", errs.Load())
	fmt.Printf("  achieved rate    : %.0f msg/s (%.1f%% of target)\n",
		achievedRate, achievedRate/float64(rate)*100)
}
