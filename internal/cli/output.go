package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// render prints v as indented JSON (-o json) or calls table to print a
// human-readable view.
func (a *app) render(v any, table func(w io.Writer)) error {
	if a.output == "json" {
		enc := json.NewEncoder(a.env.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	tw := tabwriter.NewWriter(a.env.Stdout, 0, 2, 2, ' ', 0)
	table(tw)
	return tw.Flush()
}

func row(w io.Writer, cols ...any) {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprint(c)
	}
	fmt.Fprintln(w, strings.Join(parts, "\t"))
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	switch {
	case d < 0:
		return "in " + (-d).Round(time.Second).String()
	case d < time.Minute:
		return d.Round(time.Second).String() + " ago"
	case d < time.Hour:
		return d.Round(time.Minute).String() + " ago"
	case d < 48*time.Hour:
		return d.Round(time.Hour).String() + " ago"
	default:
		return t.Local().Format("2006-01-02 15:04")
	}
}

func intOr(p *int, def string) string {
	if p == nil {
		return def
	}
	return fmt.Sprint(*p)
}

func strOr(p *string, def string) string {
	if p == nil || *p == "" {
		return def
	}
	return *p
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func nextPageHint(w io.Writer, cursor string) {
	if cursor != "" {
		fmt.Fprintf(w, "\nmore results: --cursor %s\n", cursor)
	}
}
