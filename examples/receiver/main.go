// Command receiver is an example webhook consumer. It verifies signatures,
// drops duplicate deliveries, and can simulate an unreliable server so you
// can watch retries, dead letters and replays happen.
//
//	go run ./examples/receiver -secret whsec_... -addr :4000 -fail-rate 0.3
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

func main() {
	addr := flag.String("addr", ":4000", "listen address")
	secret := flag.String("secret", os.Getenv("WEBHOOK_SECRET"), "signing secret(s), comma-separated (default $WEBHOOK_SECRET)")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests to answer with 500 (0..1)")
	latency := flag.Duration("latency", 0, "artificial delay before answering")
	flag.Parse()

	if *secret == "" {
		log.Fatal("a signing secret is required: -secret whsec_... or WEBHOOK_SECRET")
	}
	verifier, err := webhook.NewVerifier(strings.Split(*secret, ",")...)
	if err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	seen := map[string]bool{} // webhook-id -> processed (use a database in real life)

	http.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		// Always read the raw body first: verification needs the exact bytes.
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		if err := verifier.Verify(r.Header, body); err != nil {
			log.Printf("REJECTED %s: %v", r.Header.Get(webhook.HeaderID), err)
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		time.Sleep(*latency)
		if rand.Float64() < *failRate {
			log.Printf("SIMULATED FAILURE for %s", r.Header.Get(webhook.HeaderID))
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}

		msgID := r.Header.Get(webhook.HeaderID)
		mu.Lock()
		duplicate := seen[msgID]
		seen[msgID] = true
		mu.Unlock()
		if duplicate {
			// Delivery is at-least-once: acknowledge, but do not process twice.
			log.Printf("DUPLICATE %s (ignored)", msgID)
			w.WriteHeader(http.StatusOK)
			return
		}

		var env struct {
			ID   string          `json:"id"`
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		_ = json.Unmarshal(body, &env)
		log.Printf("RECEIVED %s %s %s", env.ID, env.Type, env.Data)
		w.WriteHeader(http.StatusNoContent)
	})

	fmt.Printf("receiver listening on %s (fail-rate %.0f%%)\n", *addr, *failRate*100)
	srv := &http.Server{Addr: *addr, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
