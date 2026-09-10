package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func scriptExecutionToGenerated(execution *apiTypes.ScriptExecution) *generated.ScriptExecution {
	if execution == nil {
		return nil
	}
	result := &generated.ScriptExecution{Mode: generated.ScriptExecutionMode(execution.Mode)}
	if execution.Image != "" || execution.Mode == "explicit" {
		result.Image = &execution.Image
	}
	if execution.User != "" || execution.Mode == "explicit" {
		result.User = &execution.User
	}
	if execution.Volumes != nil {
		volumes := make([]generated.ScriptVolumeGrant, 0, len(execution.Volumes))
		for _, grant := range execution.Volumes {
			volumes = append(volumes, generated.ScriptVolumeGrant{
				VolumeId: grant.VolumeID, Target: grant.Target, ReadOnly: grant.ReadOnly,
			})
		}
		result.Volumes = &volumes
	}
	if execution.EntryIDs != nil {
		result.EntryIds = &execution.EntryIDs
	}
	return result
}
