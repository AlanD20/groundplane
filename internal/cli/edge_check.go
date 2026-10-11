package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newRouteCheckCmd() *cobra.Command {
	var connector, path string
	cmd := &cobra.Command{
		Use:   "check <route-id>",
		Short: "Check Tunnel readiness, origin and public DNS/TLS/HTTP",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			accepted, err := fromContext(
				cmd,
			).Client.CheckRouteConnections(
				cmd.Context(),
				target(fromContext(cmd), args[0]),
				connector,
				path,
			)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	}
	cmd.Flags().StringVar(&connector, "connector", "", "enabled Tunnel Component id in the Route's Environment")
	cmd.Flags().StringVar(&path, "path", "", "concrete path matching this Route; required for a wildcard Route")
	_ = cmd.MarkFlagRequired("connector")
	return cmd
}

func newRouteCheckResultCmd() *cobra.Command {
	var latest string
	cmd := &cobra.Command{
		Use:   "check-result [task-id]",
		Short: "Show captured access stages and helper cleanup",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 0) != (latest != "") {
				return errs.New(errs.KindValidationFailed, "check-result requires a Task id or --latest Route id")
			}
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			taskID := ""
			if len(args) > 0 {
				taskID = args[0]
			}
			result, err := fromContext(cmd).Client.ShowRouteCheck(cmd.Context(), environmentID, taskID, latest)
			if err != nil {
				return err
			}
			return renderEdgeCheckResult(cmd, result)
		},
	}
	cmd.Flags().StringVar(&latest, "latest", "", "latest retained check for this Route id")
	return cmd
}

func renderEdgeCheckResult(cmd *cobra.Command, result apiTypes.EdgeCheckResult) error {
	fields, values := fieldsOfVia(
		map[string]any{"task_id": result.TaskID, "phase": result.Phase, "outcome": result.Outcome,
			"stale": result.Stale, "configuration": result.Configuration, "stages": result.Stages, "failure": result.Failure,
			"private_failure": result.PrivateFailure, "public_failure": result.PublicFailure, "addresses": result.Addresses,
			"selected_address": result.SelectedAddress, "tls_version": result.TLSVersion, "certificate_expires_at": result.CertificateExpiresAt,
			"cleanup_required": result.CleanupRequired, "helper_name": result.HelperName, "cleanup_task_id": result.CleanupTaskID,
			"created_at": result.CreatedAt, "started_at": result.StartedAt, "finished_at": result.FinishedAt},
	)
	return fromContext(cmd).Out.RenderOne(fields, values, result)
}
