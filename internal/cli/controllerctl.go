package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// controller: serve | key show | etcd show. Local admin surface on the
// same host as the Controller — `serve` is the foreground entry point
// (cmd/controller/main.go is the production systemd-run path; this
// subcommand is for local debugging/dev), `key show` and `etcd show`
// read local files/state rather than calling the HTTP API. See mvp.md,
// "The Controller itself is the one systemd unit that never becomes a
// container."
func newControllerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "controller", Short: "Local Controller admin: run in the foreground, inspect the age key and etcd"}

	cmd.AddCommand(&cobra.Command{
		Use:   "serve",
		Short: "Run the Controller in the foreground (see cmd/controller for the systemd-managed entry point)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "controller serve: run `go run ./cmd/controller` directly, or `groundplane-controller.service` in production")
			return nil
		},
	})

	key := &cobra.Command{Use: "key", Short: "The root-only controller age key"}
	key.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the controller age key's path and fingerprint (never its value)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/host") // TODO: a dedicated /host/age-key endpoint once infra/age is wired
		},
	})
	cmd.AddCommand(key)

	etcd := &cobra.Command{Use: "etcd", Short: "The bootstrap etcd store"}
	etcd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show etcd endpoint status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/host")
		},
	})
	cmd.AddCommand(etcd)

	return cmd
}
