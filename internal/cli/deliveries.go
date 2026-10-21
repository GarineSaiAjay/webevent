package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/GarineSaiAjay/webevent/pkg/client"
)

func newDeliveriesCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{Use: "deliveries", Aliases: []string{"delivery", "dl"}, Short: "Inspect and replay deliveries"}
	cmd.AddCommand(deliveriesListCmd(a), deliveriesGetCmd(a), deliveriesReplayCmd(a))
	return cmd
}

func deliveriesListCmd(a *app) *cobra.Command {
	var f client.DeliveryFilter
	var opts client.ListOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List deliveries (e.g. --status dead for the dead-letter queue)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			page, err := c.ListDeliveries(cmd.Context(), f, opts)
			if err != nil {
				return err
			}
			return a.render(page, func(w io.Writer) {
				row(w, "ID", "EVENT", "ENDPOINT", "STATUS", "ATTEMPTS", "LAST CODE", "NEXT ATTEMPT", "LAST ERROR")
				for _, d := range page.Data {
					next := "-"
					if d.Status == "pending" {
						next = ago(d.NextAttemptAt)
					}
					row(w, d.ID, d.EventID, d.EndpointID, d.Status, d.AttemptCount, intOr(d.LastStatusCode, "-"), next,
						truncate(strOr(d.LastError, "-"), 40))
				}
				nextPageHint(w, page.NextCursor)
			})
		},
	}
	cmd.Flags().StringVar(&f.Status, "status", "", "pending, delivering, succeeded, dead or cancelled")
	cmd.Flags().StringVar(&f.EventID, "event", "", "only deliveries of this event")
	cmd.Flags().StringVar(&f.EndpointID, "endpoint", "", "only deliveries to this endpoint")
	cmd.Flags().IntVar(&opts.Limit, "limit", 20, "page size (1-100)")
	cmd.Flags().StringVar(&opts.Cursor, "cursor", "", "cursor from a previous page")
	return cmd
}

func deliveriesGetCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "get <delivery-id>",
		Short: "Show a delivery with every attempt",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			d, err := c.GetDelivery(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return a.render(d, func(w io.Writer) {
				row(w, "ID:", d.ID)
				row(w, "Event:", d.EventID)
				row(w, "Endpoint:", d.EndpointID)
				row(w, "Status:", d.Status)
				row(w, "Attempts:", d.AttemptCount)
				row(w, "Replays:", d.ReplayCount)
				if d.Status == "pending" {
					row(w, "Next attempt:", d.NextAttemptAt.Local().Format(time.RFC3339))
				}
				fmt.Fprintln(w, "\nAttempts:")
				row(w, "#", "TIME", "CODE", "DURATION", "ERROR")
				for _, at := range d.Attempts {
					row(w, at.AttemptNumber, at.CreatedAt.Local().Format(time.RFC3339), intOr(at.StatusCode, "-"),
						fmt.Sprintf("%dms", at.DurationMS), truncate(strOr(at.Error, "-"), 70))
				}
			})
		},
	}
}

func deliveriesReplayCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "replay <delivery-id>",
		Short: "Re-send a dead (or succeeded) delivery",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			d, err := c.ReplayDelivery(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return a.render(d, func(w io.Writer) { fmt.Fprintf(w, "delivery %s re-queued (replay #%d)\n", d.ID, d.ReplayCount) })
		},
	}
}

func newLogsCmd(a *app) *cobra.Command {
	var f client.AttemptFilter
	var limit int
	var follow bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show delivery attempts; --follow streams new ones as they happen",
		Example: `  webevent logs --endpoint ep_01j...
  webevent logs --follow
  webevent logs --event evt_01j... -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.client()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			page, err := c.ListAttempts(ctx, f, client.ListOptions{Limit: limit})
			if err != nil {
				return err
			}
			// The browse API returns newest first; print oldest first, like a log.
			for i, j := 0, len(page.Data)-1; i < j; i, j = i+1, j-1 {
				page.Data[i], page.Data[j] = page.Data[j], page.Data[i]
			}
			if !follow {
				return a.render(page.Data, func(w io.Writer) { printAttempts(w, page.Data, true) })
			}
			return a.follow(ctx, c, f, page.Data, interval)
		},
	}
	cmd.Flags().StringVar(&f.EndpointID, "endpoint", "", "only attempts for this endpoint")
	cmd.Flags().StringVar(&f.EventID, "event", "", "only attempts for this event")
	cmd.Flags().StringVar(&f.DeliveryID, "delivery", "", "only attempts for this delivery")
	cmd.Flags().IntVar(&limit, "limit", 20, "how many recent attempts to show first (1-100)")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep polling and print new attempts")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "polling interval for --follow")
	return cmd
}

// follow prints the initial attempts, then polls for newer ones until the
// command is interrupted (Ctrl-C cancels the context).
func (a *app) follow(ctx context.Context, c *client.Client, f client.AttemptFilter, initial []client.Attempt, interval time.Duration) error {
	a.printStream(initial, true)
	if len(initial) > 0 {
		f.After = initial[len(initial)-1].ID
	} else {
		f.After = "0" // every id sorts after "0"
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	header := len(initial) == 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for {
			page, err := c.ListAttempts(ctx, f, client.ListOptions{Limit: 100})
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				fmt.Fprintln(a.env.Stderr, "poll failed:", err)
				break
			}
			a.printStream(page.Data, header)
			header = header && len(page.Data) == 0
			f.After = page.NextCursor
			if len(page.Data) < 100 {
				break
			}
		}
	}
}

// printStream writes attempts immediately (JSON lines with -o json).
func (a *app) printStream(list []client.Attempt, header bool) {
	if a.output == "json" {
		for _, at := range list {
			_ = a.render(at, nil)
		}
		return
	}
	_ = a.render(nil, func(w io.Writer) { printAttempts(w, list, header) })
}

// printAttempts uses fixed-width columns (not a tabwriter) so that lines
// printed by successive --follow polls stay aligned with the header.
func printAttempts(w io.Writer, list []client.Attempt, header bool) {
	const format = "%-12s  %-30s  %3s  %-24s  %-30s  %4s  %8s  %s\n"
	if header {
		fmt.Fprintf(w, format, "TIME", "DELIVERY", "#", "EVENT TYPE", "ENDPOINT", "CODE", "DURATION", "ERROR")
	}
	for _, at := range list {
		fmt.Fprintf(w, format, at.CreatedAt.Local().Format("15:04:05.000"), at.DeliveryID, fmt.Sprint(at.AttemptNumber),
			truncate(at.EventType, 24), at.EndpointID, intOr(at.StatusCode, "-"), fmt.Sprintf("%dms", at.DurationMS),
			truncate(strOr(at.Error, ""), 60))
	}
}
