package cli

import "github.com/spf13/cobra"

// host: show. Includes etcd status — etcd is host-level, not a Core
// component. See mvp.md, "etcd is host-level, not a Core component
// (locked)".
func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "host", Short: "The host this Controller runs on"}
	cmd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show host status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/host")
		},
	})
	return cmd
}
