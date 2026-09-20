package scriptdefinition

import (
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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

func executionIntent(execution apiTypes.ScriptExecution) requestidempotency.Value {
	fields := []requestidempotency.Field{{Name: "mode", Value: requestidempotency.String(execution.Mode)}}
	if execution.Mode == "inherited" {
		return requestidempotency.Object(fields...)
	}
	volumes := make([]requestidempotency.Value, 0, len(execution.Volumes))
	for _, grant := range execution.Volumes {
		volumes = append(volumes, requestidempotency.Object(
			requestidempotency.Field{Name: "volume_id", Value: requestidempotency.String(grant.VolumeID)},
			requestidempotency.Field{Name: "target", Value: requestidempotency.String(grant.Target)},
			requestidempotency.Field{Name: "read_only", Value: requestidempotency.Bool(grant.ReadOnly)},
		))
	}
	entries := make([]requestidempotency.Value, 0, len(execution.EntryIDs))
	for _, id := range execution.EntryIDs {
		entries = append(entries, requestidempotency.String(id))
	}
	fields = append(fields,
		requestidempotency.Field{Name: "image", Value: requestidempotency.String(execution.Image)},
		requestidempotency.Field{Name: "user", Value: requestidempotency.String(execution.User)},
		requestidempotency.Field{Name: "volumes", Value: requestidempotency.List(volumes...)},
		requestidempotency.Field{Name: "entry_ids", Value: requestidempotency.List(entries...)},
	)
	return requestidempotency.Object(fields...)
}
