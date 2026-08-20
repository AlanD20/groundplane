package cli

import (
	"github.com/spf13/cobra"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// controller: serve | key show | etcd show. Local admin surface on the
// same host as the Controller — `serve` is the foreground entry point
// (cmd/controller/main.go is the production systemd-run path; this
// subcommand is for local debugging/dev), `key show` and `etcd show`
// read local files/state rather than calling the HTTP API. See mvp.md,
// "The Controller itself is the one systemd unit that never becomes a
// container."
func newControllerCmd(runController ControllerRunner) *cobra.Command {
	cmd := &cobra.Command{Use: "controller", Short: "Local Controller admin: run in the foreground, inspect the age key and etcd"}

	cmd.AddCommand(&cobra.Command{
		Use:   "serve",
		Short: "Run the Controller in the foreground (see cmd/controller for the systemd-managed entry point)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if runController == nil {
				return errs.New(errs.CodeInternal, "controller runner is not configured")
			}
			return runController(cmd.Context())
		},
	})

	key := &cobra.Command{Use: "key", Short: "The root-only controller age key"}
	key.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the controller age key's path and fingerprint (never its value)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errs.New(errs.CodeNotImplemented, "controller key diagnostics are not implemented")
		},
	})
	cmd.AddCommand(key)

	etcd := &cobra.Command{Use: "etcd", Short: "The bootstrap etcd store"}
	etcd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show etcd endpoint status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errs.New(errs.CodeNotImplemented, "controller etcd diagnostics are not implemented")
		},
	})
	cmd.AddCommand(etcd)

	return cmd
}
