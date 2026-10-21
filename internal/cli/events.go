package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/GarineSaiAjay/webevent/pkg/client"
)

func newEventsCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{Use: "events", Aliases: []string{"event", "ev"}, Short: "Publish and inspect events"}
	cmd.AddCommand(eventsTriggerCmd(a), eventsListCmd(a), eventsGetCmd(a))
	return cmd
}

// readPayload resolves --data: inline JSON, @file, or - for stdin.
func readPayload(a *app, data string) (json.RawMessage, error) {
	var raw []byte
	var err error
	switch {
	case data == "-":
		raw, err = io.ReadAll(a.env.Stdin)
	case strings.HasPrefix(data, "@"):
		raw, err = os.ReadFile(strings.TrimPrefix(data, "@"))
	default:
		raw = []byte(data)
	}
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	if !json.Valid(raw) {
		return nil, errors.New("payload is not valid JSON")
	}
	return raw, nil
}

func eventsTriggerCmd(a *app) *cobra.Command {
	var data, key string
	cmd := &cobra.Command{
		Use:   "trigger <event-type> [--data JSON|@file|-]",
		Short: "Publish an event",
		Example: `  webevent events trigger user.created --data '{"id": 42}'
  webevent events trigger invoice.paid --data @invoice.json --idempotency-key inv-981
  cat payload.json | webevent events trigger order.shipped --data -`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			payload, err := readPayload(a, data)
			if err != nil {
				return err
			}
			res, err := c.CreateEvent(cmd.Context(), client.CreateEventInput{EventType: args[0], Payload: payload, IdempotencyKey: key})
			if err != nil {
				return err
			}
			return a.render(res, func(w io.Writer) {
				verb := "created"
				if res.Replayed {
					verb = "already existed (idempotent replay)"
				}
				fmt.Fprintf(w, "event %s %s, %s queued\n", res.ID, verb, plural(res.DeliveryCount, "delivery", "deliveries"))
			})
		},
	}
	cmd.Flags().StringVarP(&data, "data", "d", "{}", "JSON payload, @file to read a file, or - for stdin")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "makes retries of this command safe")
	return cmd
}

func eventsListCmd(a *app) *cobra.Command {
	var opts client.ListOptions
	var eventType string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recent events",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			page, err := c.ListEvents(cmd.Context(), eventType, opts)
			if err != nil {
				return err
			}
			return a.render(page, func(w io.Writer) {
				row(w, "ID", "TYPE", "CREATED", "PAYLOAD")
				for _, ev := range page.Data {
					row(w, ev.ID, ev.EventType, ago(ev.CreatedAt), truncate(string(ev.Payload), 60))
				}
				nextPageHint(w, page.NextCursor)
			})
		},
	}
	cmd.Flags().StringVar(&eventType, "type", "", "only events of this type")
	cmd.Flags().IntVar(&opts.Limit, "limit", 20, "page size (1-100)")
	cmd.Flags().StringVar(&opts.Cursor, "cursor", "", "cursor from a previous page")
	return cmd
}

func eventsGetCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "get <event-id>",
		Short: "Show an event and the state of its deliveries",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			ev, err := c.GetEvent(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			dl, err := c.ListDeliveries(cmd.Context(), client.DeliveryFilter{EventID: ev.ID}, client.ListOptions{Limit: 100})
			if err != nil {
				return err
			}
			out := struct {
				*client.Event
				Deliveries []client.Delivery `json:"deliveries"`
			}{ev, dl.Data}
			return a.render(out, func(w io.Writer) {
				row(w, "ID:", ev.ID)
				row(w, "Type:", ev.EventType)
				row(w, "Created:", ev.CreatedAt.Local().Format(time.RFC3339))
				var pretty bytes.Buffer
				if json.Indent(&pretty, ev.Payload, "", "  ") == nil {
					fmt.Fprintf(w, "Payload:\n%s\n", pretty.String())
				}
				fmt.Fprintln(w, "\nDeliveries:")
				row(w, "ID", "ENDPOINT", "STATUS", "ATTEMPTS", "LAST CODE")
				for _, d := range dl.Data {
					row(w, d.ID, d.EndpointID, d.Status, d.AttemptCount, intOr(d.LastStatusCode, "-"))
				}
			})
		},
	}
}

// plural returns "1 delivery" / "2 deliveries" style counts.
func plural(n int, singular, pluralForm string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %s", n, pluralForm)
}
