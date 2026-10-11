package cli

import "github.com/spf13/cobra"

func newComponentTunnelCheckCmd() *cobra.Command {
	var origin, hostname, path string
	var port uint16
	cmd := &cobra.Command{
		Use:   "tunnel-check <id>",
		Short: "Check Tunnel readiness, a serving Service origin and public DNS/TLS/HTTP",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, origin)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			accepted, err := app.Client.CheckTunnelConnections(
				cmd.Context(),
				target(app, args[0]),
				serviceID,
				port,
				hostname,
				path,
			)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	}
	cmd.Flags().StringVar(&origin, "origin", "", "origin Service name in the selected Environment; --id selects its id")
	cmd.Flags().Uint16Var(&port, "port", 0, "HTTP port exposed as TCP by the serving origin")
	cmd.Flags().StringVar(&hostname, "hostname", "", "public hostname configured at the Tunnel provider")
	cmd.Flags().StringVar(&path, "path", "", "concrete HTTP request path without query or fragment")
	for _, flag := range []string{"origin", "port", "hostname", "path"} {
		_ = cmd.MarkFlagRequired(flag)
	}
	return cmd
}

func newComponentTunnelCheckResultCmd() *cobra.Command {
	var taskID string
	cmd := &cobra.Command{
		Use:   "tunnel-check-result <id>",
		Short: "Show a standalone Tunnel's latest check or a selected check Task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			result, err := app.Client.ShowTunnelCheck(cmd.Context(), target(app, args[0]), taskID)
			if err != nil {
				return err
			}
			return renderEdgeCheckResult(cmd, result)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "show this check Task instead of the latest retained result")
	return cmd
}
