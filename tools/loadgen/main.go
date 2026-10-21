// Command loadgen measures throughput and end-to-end latency of a running
// webevent server. It starts its own signature-verifying receiver, registers
// an endpoint pointing at it, publishes events as fast as allowed, and
// reports how long events took from publish to arrival.
//
// The server must accept private URLs for this (WEBEVENT_ALLOW_PRIVATE_NETWORKS=true).
//
//	go run ./tools/loadgen -events 5000 -concurrency 32
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GarineSaiAjay/webevent/pkg/client"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

func main() {
	server := flag.String("server", envOr("WEBEVENT_SERVER", "http://localhost:8080"), "server URL")
	apiKey := flag.String("api-key", os.Getenv("WEBEVENT_API_KEY"), "API key")
	events := flag.Int("events", 2000, "number of events to publish")
	concurrency := flag.Int("concurrency", 16, "concurrent publishers")
	receiverDelay := flag.Duration("receiver-delay", 0, "artificial receiver latency")
	timeout := flag.Duration("timeout", 5*time.Minute, "give up waiting for deliveries after this long")
	flag.Parse()
	if *apiKey == "" {
		log.Fatal("-api-key or WEBEVENT_API_KEY is required")
	}

	// Receiver: records the arrival time of each event id.
	var (
		mu        sync.Mutex
		verifier  *webhook.Verifier
		published = map[string]time.Time{}
		arrived   = map[string]time.Time{}
		dups, bad atomic.Int64
	)
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		_ = http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			v := verifier
			mu.Unlock()
			if v == nil || v.Verify(r.Header, body) != nil {
				bad.Add(1)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			time.Sleep(*receiverDelay)
			var env struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(body, &env)
			mu.Lock()
			if _, seen := arrived[env.ID]; seen {
				dups.Add(1)
			} else {
				arrived[env.ID] = time.Now()
			}
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
	}()

	ctx := context.Background()
	c := client.New(*server, *apiKey)
	maxInFlight := 1000
	ep, err := c.CreateEndpoint(ctx, client.CreateEndpointInput{
		URL: "http://" + ln.Addr().String() + "/", EventTypes: []string{"loadgen.event"}, MaxInFlight: &maxInFlight,
		Description: "loadgen (temporary)",
	})
	if err != nil {
		log.Fatalf("create endpoint: %v", err)
	}
	defer func() { _ = c.DeleteEndpoint(context.Background(), ep.ID) }()
	v, err := webhook.NewVerifier(ep.Secret)
	if err != nil {
		log.Print(err)
		return
	}
	mu.Lock()
	verifier = v
	mu.Unlock()

	// Publish.
	fmt.Printf("publishing %d events with %d publishers to %s\n", *events, *concurrency, *server)
	var next atomic.Int64
	var publishLat []time.Duration
	var plMu sync.Mutex
	var failures atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > int64(*events) {
					return
				}
				t0 := time.Now()
				res, err := c.CreateEvent(ctx, client.CreateEventInput{EventType: "loadgen.event", Payload: map[string]int64{"i": i}})
				if err != nil {
					failures.Add(1)
					continue
				}
				d := time.Since(t0)
				mu.Lock()
				published[res.ID] = t0
				mu.Unlock()
				plMu.Lock()
				publishLat = append(publishLat, d)
				plMu.Unlock()
			}
		}()
	}
	wg.Wait()
	publishDur := time.Since(start)

	// Wait for delivery.
	want := *events - int(failures.Load())
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(arrived)
		mu.Unlock()
		if n >= want {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	total := time.Since(start)

	mu.Lock()
	var e2e []time.Duration
	for id, t0 := range published {
		if t1, ok := arrived[id]; ok {
			e2e = append(e2e, t1.Sub(t0))
		}
	}
	delivered := len(arrived)
	mu.Unlock()

	fmt.Printf("\npublished:  %d ok, %d failed in %v (%.0f events/s)\n", len(publishLat), failures.Load(), publishDur.Round(time.Millisecond),
		float64(len(publishLat))/publishDur.Seconds())
	fmt.Printf("publish latency:     %s\n", percentiles(publishLat))
	fmt.Printf("delivered:  %d/%d in %v (%.0f deliveries/s overall)\n", delivered, want, total.Round(time.Millisecond), float64(delivered)/total.Seconds())
	fmt.Printf("end-to-end latency:  %s\n", percentiles(e2e))
	fmt.Printf("duplicates: %d   bad signatures: %d\n", dups.Load(), bad.Load())
	if delivered < want {
		fmt.Println("NOT ALL EVENTS WERE DELIVERED")
	}
}

func percentiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "n/a"
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	p := func(q float64) time.Duration { return ds[int(q*float64(len(ds)-1))].Round(100 * time.Microsecond) }
	return fmt.Sprintf("p50=%v p95=%v p99=%v max=%v", p(0.50), p(0.95), p(0.99), ds[len(ds)-1].Round(100*time.Microsecond))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
