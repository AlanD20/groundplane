package taskplanning

import (
	"context"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
)

// BuildReleaseHookRenderInput reuses the manual Script projection boundary
// while retaining the parent release Task and candidate Release identities.
func BuildReleaseHookRenderInput(
	ctx context.Context,
	input ScriptRunnerInput,
) (releaserender.ReleaseHookRenderInput, error) {
	prepared, err := prepareScriptRunner(ctx, input)
	if err != nil {
		return releaserender.ReleaseHookRenderInput{}, err
	}
	snapshot := prepared.Snapshot
	projection := prepared.Projection
	body := prepared.Body
	run := prepared.Run
	createdAt, err := ids.Timestamp(ids.KindTask, input.TaskID)
	if err != nil {
		return releaserender.ReleaseHookRenderInput{}, err
	}
	execution, err := scriptexecutions.NewExecution(scriptexecutions.ExecutionInput{
		TaskID: input.TaskID, WorkItemID: input.WorkItemID, OperationID: input.OperationID, PlanID: input.PlanID,
		ScriptSetGeneration: input.Sources.Script.Record.ScriptSetGeneration,
		Snapshot:            snapshot, Projection: projection, Run: run, Body: body,
	}, createdAt)
	if err != nil {
		return releaserender.ReleaseHookRenderInput{}, err
	}
	defer execution.Clear()
	return releaserender.ReleaseHookRenderInput{
		Input: execution.Input, ScriptSetGeneration: execution.ScriptSetGeneration,
		ScriptID:          input.Sources.Script.Record.Desired.ID,
		ScriptSlug:        input.Sources.Script.Record.Desired.Slug,
		ServiceID:         input.Sources.Service.Record.Desired.ID,
		When:              input.Sources.Script.Record.Desired.When,
		Order:             input.Sources.Script.Record.Desired.Order,
		ScriptGeneration:  input.Sources.BodyGeneration.Record.Generation,
		ScriptExecutionID: input.ExecutionID, RunnerSnapshotID: input.SnapshotID,
		BodySize: body.Size, BodySHA256: hex.EncodeToString(body.Sha256),
		ServiceDefinitionSHA256: hex.EncodeToString(run.ServiceDefinitionSha256),
		RunnerSnapshot: append(
			[]byte(nil),
			execution.Snapshot...), RunnerProjection: append([]byte(nil), execution.RunnerProjection...),
	}, nil
}
