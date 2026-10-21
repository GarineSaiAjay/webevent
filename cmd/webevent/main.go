// Command webevent is the command-line client for the webhook delivery service.
//
//	webevent config set server http://localhost:8080
//	webevent config set api-key <key>
//	webevent endpoints create --url https://example.com/hooks --events user.created
//	webevent events trigger user.created --data '{"id": 42}'
//	webevent logs --follow
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/GarineSaiAjay/webevent/internal/cli"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx, cli.OSEnv(), version, os.Args[1:])
	stop()
	os.Exit(code)
}
