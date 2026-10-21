package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/GarineSaiAjay/webevent/pkg/client"
)

func newEndpointsCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{Use: "endpoints", Aliases: []string{"endpoint", "ep"}, Short: "Manage webhook endpoints"}
	cmd.AddCommand(
		endpointsCreateCmd(a), endpointsListCmd(a), endpointsGetCmd(a), endpointsUpdateCmd(a),
		endpointsDeleteCmd(a), endpointsSecretCmd(a), endpointsRotateCmd(a),
		endpointsStatusCmd(a, "pause", "paused", "Stop sending to an endpoint (deliveries queue up)"),
		endpointsStatusCmd(a, "resume", "active", "Resume an endpoint that was paused or automatically disabled"),
		endpointsReplayCmd(a),
	)
	return cmd
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func printEndpoint(w io.Writer, ep *client.Endpoint) {
	row(w, "ID:", ep.ID)
	row(w, "URL:", ep.URL)
	row(w, "Description:", ep.Description)
	row(w, "Events:", strings.Join(ep.EventTypes, ", "))
	row(w, "Status:", ep.Status)
	if ep.DisabledReason != nil {
		row(w, "Disabled reason:", *ep.DisabledReason)
	}
	row(w, "Rate limit:", intOr(ep.RateLimitPerSec, "unlimited")+" /s")
	row(w, "Max in flight:", ep.MaxInFlight)
	row(w, "Consecutive failures:", ep.ConsecutiveFailures)
	row(w, "Created:", ep.CreatedAt.Local().Format(time.RFC3339))
	if ep.Secret != "" {
		row(w, "Signing secret:", ep.Secret)
	}
}

func endpointsCreateCmd(a *app) *cobra.Command {
	var url, events, desc string
	var rate, inFlight int
	cmd := &cobra.Command{
		Use:   "create --url URL [--events a,b]",
		Short: "Register an endpoint (prints its signing secret)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			in := client.CreateEndpointInput{URL: url, Description: desc, EventTypes: splitList(events)}
			if cmd.Flags().Changed("rate-limit") {
				in.RateLimitPerSec = &rate
			}
			if cmd.Flags().Changed("max-in-flight") {
				in.MaxInFlight = &inFlight
			}
			ep, err := c.CreateEndpoint(cmd.Context(), in)
			if err != nil {
				return err
			}
			return a.render(ep, func(w io.Writer) {
				printEndpoint(w, ep)
				fmt.Fprintln(w, "\nStore the signing secret in your receiver to verify webhooks.")
			})
		},
	}
	cmd.Flags().StringVar(&url, "url", "", "URL that receives webhooks (required)")
	cmd.Flags().StringVar(&events, "events", "*", `comma-separated event types to subscribe to ("*" = all)`)
	cmd.Flags().StringVar(&desc, "description", "", "free-form description")
	cmd.Flags().IntVar(&rate, "rate-limit", 0, "max deliveries per second (0 = unlimited)")
	cmd.Flags().IntVar(&inFlight, "max-in-flight", 10, "max concurrent requests to this endpoint")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}

func endpointsListCmd(a *app) *cobra.Command {
	var opts client.ListOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List endpoints",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			page, err := c.ListEndpoints(cmd.Context(), opts)
			if err != nil {
				return err
			}
			return a.render(page, func(w io.Writer) {
				row(w, "ID", "STATUS", "URL", "EVENTS", "RATE", "FAILURES", "CREATED")
				for _, ep := range page.Data {
					row(w, ep.ID, ep.Status, truncate(ep.URL, 50), truncate(strings.Join(ep.EventTypes, ","), 30),
						intOr(ep.RateLimitPerSec, "-"), ep.ConsecutiveFailures, ago(ep.CreatedAt))
				}
				nextPageHint(w, page.NextCursor)
			})
		},
	}
	cmd.Flags().IntVar(&opts.Limit, "limit", 20, "page size (1-100)")
	cmd.Flags().StringVar(&opts.Cursor, "cursor", "", "cursor from a previous page")
	return cmd
}

func endpointsGetCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "get <endpoint-id>",
		Short: "Show one endpoint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			ep, err := c.GetEndpoint(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return a.render(ep, func(w io.Writer) { printEndpoint(w, ep) })
		},
	}
}

func endpointsUpdateCmd(a *app) *cobra.Command {
	var url, events, desc string
	var rate, inFlight int
	cmd := &cobra.Command{
		Use:   "update <endpoint-id> [flags]",
		Short: "Change an endpoint (only the flags you pass are changed)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			var in client.UpdateEndpointInput
			f := cmd.Flags()
			if f.Changed("url") {
				in.URL = &url
			}
			if f.Changed("description") {
				in.Description = &desc
			}
			if f.Changed("events") {
				list := splitList(events)
				in.EventTypes = &list
			}
			if f.Changed("rate-limit") {
				in.RateLimitPerSec = &rate
			}
			if f.Changed("max-in-flight") {
				in.MaxInFlight = &inFlight
			}
			ep, err := c.UpdateEndpoint(cmd.Context(), args[0], in)
			if err != nil {
				return err
			}
			return a.render(ep, func(w io.Writer) { printEndpoint(w, ep) })
		},
	}
	cmd.Flags().StringVar(&url, "url", "", "new URL")
	cmd.Flags().StringVar(&events, "events", "", "new comma-separated event types")
	cmd.Flags().StringVar(&desc, "description", "", "new description")
	cmd.Flags().IntVar(&rate, "rate-limit", 0, "deliveries per second (0 removes the limit)")
	cmd.Flags().IntVar(&inFlight, "max-in-flight", 0, "max concurrent requests")
	return cmd
}

func endpointsStatusCmd(a *app, use, status, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use + " <endpoint-id>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			ep, err := c.UpdateEndpoint(cmd.Context(), args[0], client.UpdateEndpointInput{Status: &status})
			if err != nil {
				return err
			}
			return a.render(ep, func(w io.Writer) { row(w, ep.ID, "is now", ep.Status) })
		},
	}
}

func endpointsDeleteCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <endpoint-id>",
		Short: "Delete an endpoint and cancel its pending deliveries",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			if err := c.DeleteEndpoint(cmd.Context(), args[0]); err != nil {
				return err
			}
			return a.render(map[string]string{"deleted": args[0]}, func(w io.Writer) { row(w, "deleted", args[0]) })
		},
	}
}

func endpointsSecretCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "secret <endpoint-id>",
		Short: "Print an endpoint's signing secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			secret, err := c.GetEndpointSecret(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return a.render(map[string]string{"secret": secret}, func(w io.Writer) { fmt.Fprintln(w, secret) })
		},
	}
}

func endpointsRotateCmd(a *app) *cobra.Command {
	var grace time.Duration
	cmd := &cobra.Command{
		Use:   "rotate-secret <endpoint-id>",
		Short: "Issue a new signing secret (the old one stays valid for --grace)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			res, err := c.RotateEndpointSecret(cmd.Context(), args[0], grace)
			if err != nil {
				return err
			}
			return a.render(res, func(w io.Writer) {
				row(w, "New secret:", res.Secret)
				row(w, "Old secret valid until:", res.PreviousSecretValidUntil.Local().Format(time.RFC3339))
			})
		},
	}
	cmd.Flags().DurationVar(&grace, "grace", 24*time.Hour, "how long the old secret keeps working (0 revokes it now)")
	return cmd
}

func endpointsReplayCmd(a *app) *cobra.Command {
	var since, until string
	cmd := &cobra.Command{
		Use:   "replay <endpoint-id>",
		Short: "Re-send all dead deliveries of an endpoint (default: last 7 days)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			s, err := parseTimeFlag(since)
			if err != nil {
				return fmt.Errorf("--since: %w", err)
			}
			u, err := parseTimeFlag(until)
			if err != nil {
				return fmt.Errorf("--until: %w", err)
			}
			res, err := c.ReplayEndpoint(cmd.Context(), args[0], s, u)
			if err != nil {
				return err
			}
			return a.render(res, func(w io.Writer) {
				fmt.Fprintf(w, "re-queued %s (dead between %s and %s)\n", plural(res.Replayed, "dead delivery", "dead deliveries"),
					res.Since.Local().Format(time.RFC3339), res.Until.Local().Format(time.RFC3339))
			})
		},
	}
	cmd.Flags().StringVar(&since, "since", "", "RFC3339 time or a duration ago, e.g. 2h")
	cmd.Flags().StringVar(&until, "until", "", "RFC3339 time or a duration ago")
	return cmd
}

// parseTimeFlag accepts "" (zero), an RFC3339 timestamp, or a duration
// meaning "that long ago" (e.g. "90m").
func parseTimeFlag(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return time.Now().Add(-d), nil
	}
	return time.Parse(time.RFC3339, v)
}
