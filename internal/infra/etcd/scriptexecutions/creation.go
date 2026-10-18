package scriptexecutions

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ExecutionInput struct {
	TaskID, WorkItemID, OperationID, PlanID string
	ScriptSetGeneration                     string
	Snapshot                                *agentpb.ResolvedRunnerSnapshot
	Projection                              *agentpb.ScriptRunnerProjection
	Run                                     *agentpb.RunScript
	Body                                    *agentpb.ScriptBodyArtifactMetadata
}

// NewExecution captures one Script or Release hook under its native work item.
// No attempt has been assigned and no external effect is authorized here.
func NewExecution(input ExecutionInput, at time.Time) (ScriptExecutionRecord, error) {
	if input.Snapshot == nil || input.Projection == nil || input.Run == nil || input.Body == nil ||
		at.IsZero() {
		return ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Script execution requires captured source inputs")
	}
	if err := scriptrecord.ValidateScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: input.Run.EnvironmentId, GenerationID: input.ScriptSetGeneration,
	}); err != nil {
		return ScriptExecutionRecord{}, err
	}
	run, snapshot := input.Run, input.Snapshot
	record := ScriptExecutionRecord{
		ID: run.ScriptExecutionId, SnapshotID: run.RunnerSnapshotId,
		CurrentTaskID: input.TaskID, WorkItemID: input.WorkItemID, OperationID: input.OperationID,
		ScriptID: run.ScriptId, ScriptGeneration: run.ScriptGeneration, ScriptSetGeneration: input.ScriptSetGeneration,
		EnvironmentID: run.EnvironmentId, ServiceID: run.ServiceId, ReleaseID: run.ReleaseId,
		RenderGeneration: run.RenderGeneration, Input: workexecution.Input{ID: input.PlanID},
		SnapshotSHA256: hex.EncodeToString(run.RunnerSnapshotSha256), BodySHA256: hex.EncodeToString(run.BodySha256),
		RunnerProjectionSHA256: hex.EncodeToString(snapshot.RunnerProjectionSha256),
		State:                  ScriptExecutionNotStarted, ActiveReference: true, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	}
	var err error
	encode := proto.MarshalOptions{Deterministic: true}
	if record.Snapshot, err = encode.Marshal(snapshot); err != nil {
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	if record.RunnerProjection, err = encode.Marshal(input.Projection); err != nil {
		record.Clear()
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	if record.Run, err = encode.Marshal(run); err != nil {
		record.Clear()
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	if record.BodyMetadata, err = encode.Marshal(input.Body); err != nil {
		record.Clear()
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	record.Input.SHA256, err = capturedInputDigest(record)
	if err != nil {
		record.Clear()
		return ScriptExecutionRecord{}, err
	}
	if err := ValidateScriptExecutionRecord(record); err != nil {
		record.Clear()
		return ScriptExecutionRecord{}, err
	}
	return record, nil
}
