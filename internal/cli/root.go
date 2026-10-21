// Package cli implements the `webevent` command-line client. It is a thin
// layer over pkg/client: every command maps to one or two API calls.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/GarineSaiAjay/webevent/pkg/client"
)

// Env abstracts the process environment so tests can run commands in
// isolation (fake stdin/stdout, env vars and config location).
type Env struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Getenv func(string) string
}

// OSEnv is the real process environment.
func OSEnv() Env {
	return Env{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv}
}

// app carries resolved settings shared by all commands.
type app struct {
	env     Env
	server  string
	apiKey  string
	output  string // "table" or "json"
	version string
}

func (a *app) client() (*client.Client, error) {
	if a.apiKey == "" {
		return nil, errors.New("no API key: pass --api-key, set WEBEVENT_API_KEY, or run `webevent config set api-key <key>`")
	}
	c := client.New(a.server, a.apiKey)
	c.UserAgent = "webevent-cli/" + a.version
	return c, nil
}

// NewRootCmd builds the command tree.
func NewRootCmd(env Env, version string) *cobra.Command {
	a := &app{env: env, version: version}
	var flagServer, flagKey string

	root := &cobra.Command{
		Use:           "webevent",
		Short:         "Command-line client for the webevent webhook delivery service",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Precedence: flag > environment > config file > default.
			cfg, err := loadConfig(env)
			if err != nil {
				return err
			}
			a.server = firstNonEmpty(flagServer, env.Getenv("WEBEVENT_SERVER"), cfg.Server, "http://localhost:8080")
			a.apiKey = firstNonEmpty(flagKey, env.Getenv("WEBEVENT_API_KEY"), cfg.APIKey)
			if a.output != "table" && a.output != "json" {
				return fmt.Errorf("--output must be table or json, got %q", a.output)
			}
			return nil
		},
	}
	root.SetIn(env.Stdin)
	root.SetOut(env.Stdout)
	root.SetErr(env.Stderr)
	root.PersistentFlags().StringVar(&flagServer, "server", "", "server URL (env WEBEVENT_SERVER, default http://localhost:8080)")
	root.PersistentFlags().StringVar(&flagKey, "api-key", "", "API key (env WEBEVENT_API_KEY)")
	root.PersistentFlags().StringVarP(&a.output, "output", "o", "table", "output format: table or json")

	root.AddCommand(
		newConfigCmd(a),
		newEndpointsCmd(a),
		newEventsCmd(a),
		newDeliveriesCmd(a),
		newLogsCmd(a),
		newListenCmd(a),
		newHealthCmd(a),
	)
	return root
}

// Execute runs the CLI and returns the process exit code.
func Execute(ctx context.Context, env Env, version string, args []string) int {
	root := NewRootCmd(env, version)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(env.Stderr, "Error:", err)
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && apiErr.RequestID != "" {
			fmt.Fprintln(env.Stderr, "Request ID:", apiErr.RequestID)
		}
		return 1
	}
	return 0
}

func newHealthCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check that the server is reachable",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := client.New(a.server, a.apiKey)
			v, err := c.Health(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Fprintf(a.env.Stdout, "ok: %s (server version %s)\n", a.server, v)
			return nil
		},
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
