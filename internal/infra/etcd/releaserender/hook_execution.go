package releaserender

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CaptureHookExecution binds a captured hook to the final native Release input.
// Its output joins Release publication; it cannot independently authorize a hook.
func CaptureHookExecution(task workexecution.Task, item workexecution.Item, operationID string,
	hook ReleaseHookRenderInput, at time.Time,
) (scriptexecutions.ScriptExecutionRecord, error) {
	if task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || item.Placement.Executor != workexecution.Agent ||
		item.Input == nil || item.AttemptNumber != 0 || item.ForwardRevoked || task.Stop != nil ||
		task.FinishedAt != nil || at.Before(task.CreatedAt) ||
		validateReleaseHookRenderInputs([]ReleaseHookRenderInput{hook}, item.TargetID) != nil {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Release hook requires unstarted native Service work")
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	var projection agentpb.ScriptRunnerProjection
	if proto.Unmarshal(hook.RunnerSnapshot, &snapshot) != nil || proto.Unmarshal(hook.RunnerProjection, &projection) != nil ||
		snapshot.TenantId != task.Scope.TenantID || snapshot.ProjectId != task.Scope.ProjectID ||
		snapshot.EnvironmentId != task.Scope.EnvironmentID || snapshot.ServiceId != item.TargetID {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Release hook snapshot belongs to another resource scope")
	}
	bodySHA, err := hex.DecodeString(hook.BodySHA256)
	if err != nil || len(bodySHA) != sha256.Size {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Release hook body digest is invalid")
	}
	serviceSHA, err := hex.DecodeString(hook.ServiceDefinitionSHA256)
	if err != nil || len(serviceSHA) != sha256.Size {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Release hook Service digest is invalid")
	}
	snapshotSHA := sha256.Sum256(hook.RunnerSnapshot)
	record, err := scriptexecutions.NewExecution(scriptexecutions.ExecutionInput{
		TaskID: task.ID, WorkItemID: item.ID, OperationID: operationID, PlanID: hook.Input.ID,
		ScriptSetGeneration: hook.ScriptSetGeneration, Snapshot: &snapshot, Projection: &projection,
		Body: &agentpb.ScriptBodyArtifactMetadata{
			ScriptExecutionId: hook.ScriptExecutionID, ScriptId: hook.ScriptID, Generation: hook.ScriptGeneration,
			Size: hook.BodySize, Sha256: bodySHA, Uid: projection.Uid, Gid: projection.Gid,
		},
		Run: &agentpb.RunScript{
			ScriptExecutionId: hook.ScriptExecutionID, ScriptId: hook.ScriptID, ScriptGeneration: hook.ScriptGeneration,
			EnvironmentId: snapshot.EnvironmentId, ServiceId: snapshot.ServiceId, ReleaseId: snapshot.ReleaseId,
			RenderGeneration: snapshot.RenderGeneration, ServiceDefinitionSha256: serviceSHA, BodySha256: bodySHA,
			RunnerSnapshotId: hook.RunnerSnapshotID, RunnerSnapshotSha256: snapshotSHA[:],
		},
	}, at)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	if record.Input != hook.Input {
		record.Clear()
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindStateConflict, "Release hook captured input changed before publication")
	}
	parent := *item.Input
	record.WorkInput = &parent
	return record, nil
}
