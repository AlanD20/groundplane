package cli

import (
	"bytes"
	"encoding/json"
	"io"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newConnectorCheckCmd() *cobra.Command {
	var draftFile, changesFile string
	cmd := &cobra.Command{Use: "check [name]", Short: "Test Connector storage permissions without saving",
		Long: "Check a saved Connector, optionally with unsaved JSON edits, or an unsaved JSON creation draft. Cleanup Retry uses task retry and never repeats the permission probe.",
		Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 0) != (draftFile != "") || draftFile != "" && changesFile != "" {
				return errs.New(
					errs.KindValidationFailed,
					"connector check requires a saved name (with optional --changes-file), or --draft-file",
				)
			}
			environmentID, err := connectorEnvironmentID(cmd)
			if err != nil {
				return err
			}
			input := apiTypes.ConnectorCheckRequest{}
			if draftFile != "" {
				input.Draft = &apiTypes.ConnectorCreateRequest{}
				if err := readConnectorCheckJSON(cmd, draftFile, input.Draft); err != nil {
					return err
				}
				defer clearConnectorCredentialInputs(input.Draft.Credentials)
			} else {
				input.ConnectorID, err = resolveConnectorTarget(cmd, args[0])
				if err != nil {
					return err
				}
				if changesFile != "" {
					input.Changes = &apiTypes.ConnectorEditRequest{}
					if err := readConnectorCheckJSON(cmd, changesFile, input.Changes); err != nil {
						return err
					}
					defer clearConnectorCredentialInputs(input.Changes.Credentials)
				}
			}
			accepted, err := fromContext(cmd).Client.CheckConnector(cmd.Context(), environmentID, input)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		}}
	cmd.Flags().
		StringVar(&draftFile, "draft-file", "", "unsaved Connector creation JSON from PATH or -; values are write-only")
	cmd.Flags().StringVar(&changesFile, "changes-file", "", "unsaved Connector edit JSON from PATH or -")
	return cmd
}

func readConnectorCheckJSON[Input apiTypes.ConnectorCreateRequest | apiTypes.ConnectorEditRequest](
	cmd *cobra.Command,
	file string,
	target *Input,
) error {
	text, err := readValueFile(file, cmd.InOrStdin(), 512<<10, "Connector check input")
	if err != nil {
		return err
	}
	data := []byte(text)
	defer clear(data)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errs.New(errs.KindValidationFailed, "Connector check input must be valid JSON with known fields")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errs.New(errs.KindValidationFailed, "Connector check JSON has trailing data")
	}
	return nil
}

func newConnectorCheckResultCmd() *cobra.Command {
	var latest string
	cmd := &cobra.Command{
		Use:   "check-result [task-id]",
		Short: "Show Connector check stages, configuration and cleanup results",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 0) != (latest != "") {
				return errs.New(errs.KindValidationFailed, "check-result requires a Task id or --latest Connector name")
			}
			environmentID, err := connectorEnvironmentID(cmd)
			if err != nil {
				return err
			}
			var taskID, connectorID string
			if latest != "" {
				connectorID, err = resolveConnectorTarget(cmd, latest)
				if err != nil {
					return err
				}
			} else {
				taskID = args[0]
			}
			result, err := fromContext(cmd).Client.ShowConnectorCheck(cmd.Context(), environmentID, taskID, connectorID)
			if err != nil {
				return err
			}
			fields := map[string]any{"task_id": result.TaskID, "phase": result.Phase, "outcome": result.Outcome,
				"failure": result.Failure, "stale": result.Stale, "draft": result.Draft,
				"created_at": result.CreatedAt, "started_at": result.StartedAt, "finished_at": result.FinishedAt,
				"execution_location": result.ExecutionLocation, "configuration": result.Configuration,
				"stages": result.Stages, "secret_sources": result.SecretSources,
				"cleanup_required": result.CleanupRequired, "object_key": result.ObjectKey,
				"multipart_key": result.MultipartKey, "upload_ids": result.UploadIDs, "cleanup_task_id": result.CleanupTaskID}
			headers, values := fieldsOfVia(fields)
			return fromContext(cmd).Out.RenderOne(headers, values, result)
		},
	}
	cmd.Flags().StringVar(&latest, "latest", "", "show the latest retained saved check for this Connector name")
	return cmd
}
