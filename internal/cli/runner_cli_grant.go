package cli

import (
	"bytes"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

const maximumRunnerCLIGrantBytes = 32 << 10

func newRunnerCLIGrantCmd() *cobra.Command {
	command := &cobra.Command{
		Use:   "cli-grant",
		Short: "Manage the exact CLI actions allowed for a scoped Runner",
	}
	command.AddCommand(newRunnerCLIGrantShowCmd(), newRunnerCLIGrantSetCmd(), newRunnerCLIGrantRevokeCmd())
	return command
}

func newRunnerCLIGrantShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <slug>",
		Short: "Show approved actions and the current Runner grant revision",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			status, err := fromContext(cmd).Client.ShowRunnerCLIGrant(cmd.Context(), runnerID)
			if err != nil {
				return err
			}
			return renderRunnerCLIGrant(cmd, status)
		},
	}
}

func newRunnerCLIGrantSetCmd() *cobra.Command {
	var path, key string
	var revision int64
	command := &cobra.Command{
		Use:   "set <slug>",
		Short: "Replace the Runner's CLI grant with the exact actions in a JSON file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			input, err := readRunnerCLIGrant(cmd, path)
			if err != nil {
				return err
			}
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			status, err := fromContext(cmd).Client.SetRunnerCLIGrant(cmd.Context(), runnerID, revision, input, key)
			if err != nil {
				return err
			}
			return renderRunnerCLIGrant(cmd, status)
		},
	}
	command.Flags().
		StringVar(&path, "file", "", "JSON grant file containing scope, actions, and lifetime_seconds (use - for stdin)")
	_ = command.MarkFlagRequired("file")
	addRunnerCLIGrantRevisionFlags(command, &revision, &key)
	return command
}

func newRunnerCLIGrantRevokeCmd() *cobra.Command {
	var key string
	var revision int64
	command := &cobra.Command{
		Use:   "revoke <slug>",
		Short: "Revoke the Runner's approved CLI actions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			if err := app.Client.RevokeRunnerCLIGrant(cmd.Context(), runnerID, revision, key); err != nil {
				return err
			}
			fields := map[string]any{"runner_id": runnerID, "status": "revoked"}
			headers, values := fieldsOfVia(fields)
			return app.Out.RenderOne(headers, values, fields)
		},
	}
	addRunnerCLIGrantRevisionFlags(command, &revision, &key)
	return command
}

func addRunnerCLIGrantRevisionFlags(command *cobra.Command, revision *int64, key *string) {
	command.Flags().Int64Var(revision, "revision", 0, "exact revision from runner cli-grant show")
	command.Flags().
		StringVar(key, "idempotency-key", "", "reuse this key with the same revision to resolve uncertain acceptance")
	_ = command.MarkFlagRequired("revision")
}

func readRunnerCLIGrant(cmd *cobra.Command, path string) (apiTypes.RunnerCLIGrantWrite, error) {
	raw, err := readValueFile(path, cmd.InOrStdin(), maximumRunnerCLIGrantBytes, "Runner CLI grant")
	if err != nil {
		return apiTypes.RunnerCLIGrantWrite{}, err
	}
	canonical, err := jcs.Canonicalize([]byte(raw))
	if err != nil || !bytes.HasPrefix(canonical, []byte("{")) {
		return apiTypes.RunnerCLIGrantWrite{}, errs.New(
			errs.KindValidationFailed,
			"Runner CLI grant requires one valid JSON object with unique fields",
		)
	}
	input, err := jcs.Decode[apiTypes.RunnerCLIGrantWrite](canonical)
	if err != nil {
		return apiTypes.RunnerCLIGrantWrite{}, errs.New(
			errs.KindValidationFailed,
			"Runner CLI grant fields do not match the grant schema. Omit empty optional fields",
		)
	}
	return input, nil
}

func renderRunnerCLIGrant(cmd *cobra.Command, status apiTypes.RunnerCLIGrantStatus) error {
	fields := map[string]any{
		"runner_id": status.RunnerID, "revision": status.Revision, "active": status.Active, "available": status.Available,
	}
	if scope := status.Scope; scope != nil {
		fields["tenant_id"], fields["project_id"], fields["environment_id"] = scope.TenantID, scope.ProjectID, scope.EnvironmentID
	}
	if status.ExpiresAt != nil {
		fields["expires_at"] = *status.ExpiresAt
	}
	if len(status.Actions) != 0 {
		actions := make([]string, 0, len(status.Actions))
		for _, action := range status.Actions {
			line := action.Operation + " " + action.TargetID
			if action.Image != "" {
				line += " " + action.Image
			}
			if len(action.TaskIDs) != 0 {
				line += " after " + strings.Join(action.TaskIDs, ", ")
			}
			actions = append(actions, line)
		}
		fields["actions"] = strings.Join(actions, "\n")
	}
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, status)
}
