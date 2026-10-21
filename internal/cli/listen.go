package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/GarineSaiAjay/webevent/pkg/client"
)

func newListenCmd(a *app) *cobra.Command {
	var forwardTo, events string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "listen --forward-to URL",
		Short: "Receive webhooks on your machine without exposing it to the internet",
		Long: `listen opens a temporary endpoint on the server and forwards every
delivery for it to a local URL, with the real signature headers, then reports
the local response back. Retries, dead letters and signatures behave exactly
as for a real endpoint. Press Ctrl-C to stop; the session is deleted.`,
		Example: `  webevent listen --forward-to http://localhost:3000/webhooks
  webevent listen --forward-to http://localhost:3000/webhooks --events order.paid,order.refunded`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			sess, err := c.StartListen(ctx, splitList(events))
			if err != nil {
				return err
			}
			defer func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = c.StopListen(stopCtx, sess.SessionID)
				fmt.Fprintln(a.env.Stdout, "listen session closed")
			}()
			fmt.Fprintf(a.env.Stdout, "Ready! Forwarding %v to %s\nSigning secret for this session: %s\n\n",
				sess.EventTypes, forwardTo, sess.Secret)

			local := &http.Client{Timeout: timeout}
			for ctx.Err() == nil {
				list, err := c.PollListen(ctx, sess.SessionID, 20*time.Second)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					if client.IsNotFound(err) {
						return errors.New("listen session expired on the server")
					}
					fmt.Fprintln(a.env.Stderr, "poll failed, retrying:", err)
					sleep(ctx, 2*time.Second)
					continue
				}
				results := make([]client.ListenResult, 0, len(list))
				for _, d := range list {
					r := forward(ctx, local, forwardTo, d)
					results = append(results, r)
					code := fmt.Sprint(r.StatusCode)
					if r.StatusCode == 0 {
						code = "ERR"
					}
					fmt.Fprintf(a.env.Stdout, "%s  --> %-24s %s  [%s] %dms %s\n",
						time.Now().Format("15:04:05"), d.EventType, d.EventID, code, r.DurationMS, r.Error)
				}
				if len(results) > 0 {
					if err := c.ReportListen(context.WithoutCancel(ctx), sess.SessionID, results); err != nil {
						fmt.Fprintln(a.env.Stderr, "report failed (deliveries will be retried):", err)
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&forwardTo, "forward-to", "", "local URL to forward deliveries to (required)")
	cmd.Flags().StringVar(&events, "events", "*", "comma-separated event types to receive")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "timeout for the local request")
	_ = cmd.MarkFlagRequired("forward-to")
	return cmd
}

// forward replays one delivery against the local server.
func forward(ctx context.Context, hc *http.Client, url string, d client.ListenDelivery) client.ListenResult {
	res := client.ListenResult{DeliveryID: d.DeliveryID, LeaseToken: d.LeaseToken}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(d.Body)))
	if err != nil {
		res.Error = err.Error()
		return res
	}
	for k, v := range d.Headers {
		req.Header.Set(k, v)
	}
	resp, err := hc.Do(req)
	res.DurationMS = int(time.Since(start).Milliseconds())
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	res.StatusCode = resp.StatusCode
	res.ResponseBody = string(body)
	return res
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
