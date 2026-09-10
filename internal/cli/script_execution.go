package cli

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func loadScriptExecution(cmd *cobra.Command, path string) (*apiTypes.ScriptExecution, error) {
	value, err := readValueFile(path, cmd.InOrStdin(), maximumScriptExecutionFileBytes, "Script execution")
	if err != nil {
		return nil, err
	}
	file, err := decodeScriptExecutionFile(value)
	if err != nil {
		return nil, err
	}
	execution := &apiTypes.ScriptExecution{Mode: file.Mode, Image: file.Image, User: file.User}
	for _, grant := range file.Volumes {
		id, err := resolveVolumeTarget(cmd, grant.Volume)
		if err != nil {
			return nil, err
		}
		if ids.Validate(ids.KindVolume, id) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script Volume grant requires a stable Volume id")
		}
		execution.Volumes = append(execution.Volumes, apiTypes.ScriptVolumeGrant{
			VolumeID: id, Target: grant.Target, ReadOnly: grant.ReadOnly,
		})
	}
	execution.EntryIDs, err = resolveScriptEntryGrants(cmd, file.Entries)
	if err != nil {
		return nil, err
	}
	return execution, nil
}

func resolveScriptEntryGrants(cmd *cobra.Command, references []string) ([]string, error) {
	if len(references) == 0 {
		return nil, nil
	}
	app := fromContext(cmd)
	var entries []apiTypes.Entry
	if !app.Scope.AsID {
		environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
		if err != nil {
			return nil, err
		}
		entries, err = listAllCLIEntries(cmd.Context(), app.Client, environmentID)
		if err != nil {
			return nil, err
		}
	}
	result := make([]string, 0, len(references))
	for _, reference := range references {
		id := reference
		if !app.Scope.AsID {
			id = ""
			for _, entry := range entries {
				if entry.ReconciliationKey == reference {
					if id != "" {
						return nil, errs.New(errs.KindValidationFailed, "Script Entry key is ambiguous")
					}
					id = entry.ID
				}
			}
		}
		if ids.Validate(ids.KindEnvEntry, id) != nil {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Script Entry key was not found; API-owned Entries require --id",
			)
		}
		result = append(result, id)
	}
	return result, nil
}
