package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// fileConfig is persisted at $XDG_CONFIG_HOME/webevent/config.json (or the
// OS equivalent); WEBEVENT_CONFIG overrides the path.
type fileConfig struct {
	Server string `json:"server,omitempty"`
	APIKey string `json:"api_key,omitempty"`
}

func configPath(env Env) (string, error) {
	if p := env.Getenv("WEBEVENT_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "webevent", "config.json"), nil
}

func loadConfig(env Env) (fileConfig, error) {
	var cfg fileConfig
	p, err := configPath(env)
	if err != nil {
		return cfg, nil // no config dir: just use flags/env
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", p, err)
	}
	return cfg, nil
}

func saveConfig(env Env, cfg fileConfig) (string, error) {
	p, err := configPath(env)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return "", err
	}
	data, _ := json.MarshalIndent(cfg, "", "  ")
	// 0600: the file holds an API key.
	return p, os.WriteFile(p, append(data, '\n'), 0o600)
}

func newConfigCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Manage CLI settings (server URL, API key)"}

	set := &cobra.Command{
		Use:       "set <server|api-key> <value>",
		Short:     "Save a setting to the config file",
		Args:      cobra.ExactArgs(2),
		ValidArgs: []string{"server", "api-key"},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig(a.env)
			if err != nil {
				return err
			}
			switch args[0] {
			case "server":
				cfg.Server = args[1]
			case "api-key":
				cfg.APIKey = args[1]
			default:
				return fmt.Errorf("unknown setting %q (want server or api-key)", args[0])
			}
			p, err := saveConfig(a.env, cfg)
			if err != nil {
				return err
			}
			fmt.Fprintf(a.env.Stdout, "saved %s to %s\n", args[0], p)
			return nil
		},
	}

	show := &cobra.Command{
		Use:   "show",
		Short: "Show the effective settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, _ := configPath(a.env)
			fmt.Fprintf(a.env.Stdout, "config file: %s\nserver:      %s\napi key:     %s\n", p, a.server, mask(a.apiKey))
			return nil
		},
	}
	cmd.AddCommand(set, show)
	return cmd
}

// mask shows only enough of a key to recognise it.
func mask(k string) string {
	switch {
	case k == "":
		return "(not set)"
	case len(k) <= 8:
		return "****"
	default:
		return k[:4] + "…" + k[len(k)-4:]
	}
}
