package scriptdefinition

import (
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func executionFromAPI(input *apiTypes.ScriptExecution) *core.ScriptExecution {
	if input == nil || (input.Mode == "inherited" && input.Image == "" && input.User == "" &&
		input.Volumes == nil && input.EntryIDs == nil) {
		return nil
	}
	execution := &core.ScriptExecution{Mode: core.ScriptExecutionMode(input.Mode), Image: input.Image, User: input.User,
		EntryIDs: input.EntryIDs}
	if input.Volumes != nil {
		execution.Volumes = make([]core.ScriptVolumeGrant, 0, len(input.Volumes))
		for _, grant := range input.Volumes {
			execution.Volumes = append(execution.Volumes, core.ScriptVolumeGrant{
				VolumeID: grant.VolumeID, Target: grant.Target, ReadOnly: grant.ReadOnly,
			})
		}
	}
	return execution
}

func executionResponse(execution *core.ScriptExecution) apiTypes.ScriptExecution {
	if execution == nil {
		return apiTypes.ScriptExecution{Mode: "inherited"}
	}
	response := apiTypes.ScriptExecution{Mode: string(execution.Mode), Image: execution.Image, User: execution.User,
		EntryIDs: execution.EntryIDs}
	for _, grant := range execution.Volumes {
		response.Volumes = append(response.Volumes, apiTypes.ScriptVolumeGrant{
			VolumeID: grant.VolumeID, Target: grant.Target, ReadOnly: grant.ReadOnly,
		})
	}
	return response
}

func executionIntent(execution apiTypes.ScriptExecution) idempotentintent.Value {
	fields := []idempotentintent.Field{{Name: "mode", Value: idempotentintent.String(execution.Mode)}}
	if execution.Mode == "inherited" {
		return idempotentintent.Object(fields...)
	}
	volumes := make([]idempotentintent.Value, 0, len(execution.Volumes))
	for _, grant := range execution.Volumes {
		volumes = append(volumes, idempotentintent.Object(
			idempotentintent.Field{Name: "volume_id", Value: idempotentintent.String(grant.VolumeID)},
			idempotentintent.Field{Name: "target", Value: idempotentintent.String(grant.Target)},
			idempotentintent.Field{Name: "read_only", Value: idempotentintent.Bool(grant.ReadOnly)},
		))
	}
	entries := make([]idempotentintent.Value, 0, len(execution.EntryIDs))
	for _, id := range execution.EntryIDs {
		entries = append(entries, idempotentintent.String(id))
	}
	fields = append(fields,
		idempotentintent.Field{Name: "image", Value: idempotentintent.String(execution.Image)},
		idempotentintent.Field{Name: "user", Value: idempotentintent.String(execution.User)},
		idempotentintent.Field{Name: "volumes", Value: idempotentintent.List(volumes...)},
		idempotentintent.Field{Name: "entry_ids", Value: idempotentintent.List(entries...)},
	)
	return idempotentintent.Object(fields...)
}
