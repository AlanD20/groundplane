package cli

import "github.com/spf13/cobra"

// core: show; core component coredns|controller show|config|update
// (agent is read-only here — see `groundplane agent` for the
// operational surface). See mvp.md, "Core (locked) — the
// groundplane-infra add-on".
func newCoreCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "core", Short: "The platform's own containers (groundplane-infra) and their settings"}

	cmd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Overview: components, bootstrap chain, agents",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/core")
		},
	})

	component := &cobra.Command{Use: "component", Short: "Per-component settings (coredns, controller; agent is read-only)"}

	component.AddCommand(&cobra.Command{
		Use:   "coredns",
		Short: "CoreDNS component",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/core/components/coredns")
		},
	})
	component.AddCommand(&cobra.Command{
		Use:   "controller",
		Short: "Controller component (the one systemd unit that never becomes a container)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/core/components/controller")
		},
	})

	component.AddCommand(&cobra.Command{
		Use:   "config <kind>",
		Short: "Show or set a component's config (kind: coredns | controller)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/core/components/"+target(fromContext(cmd), args[0])+"/config")
		},
	})
	component.AddCommand(&cobra.Command{
		Use:   "update <kind>",
		Short: "Update a component (controller: staged-binary swap; coredns: pinned-tag recreate)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/core/components/"+target(fromContext(cmd), args[0])+"/update", nil)
		},
	})

	cmd.AddCommand(component)
	return cmd
}
