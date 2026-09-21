package etcd

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"math"
	"strconv"
	"time"
)

func NewScriptExecutionRecord(
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
	at time.Time,
) (scriptexecutions.ScriptExecutionRecord, error) {
	records, err := NewScriptExecutionRecords(task, plan, at)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	if task.Type != taskjournal.TaskScript || len(records) != 1 {
		for index := range records {
			clear(records[index].Plan)
			clear(records[index].Snapshot)
		}
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "manual Script Task must own one execution")
	}
	return records[0], nil
}

// NewScriptExecutionRecords binds every RunScript step to one durable
// Controller-owned checkpoint record. Release hooks share their parent Task
// and sealed plan; they never create secondary Tasks.
func NewScriptExecutionRecords(
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
	at time.Time,
) ([]scriptexecutions.ScriptExecutionRecord, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	releaseTask := task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback ||
		(task.Type == taskjournal.TaskUpdate && blueprintScriptTaskShape(task) &&
			validated.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
			validated.TargetId == task.Target)
	if task.Executor != taskjournal.TaskExecutorAgent || task.Status != taskjournal.TaskStatusPending ||
		(!releaseTask && task.Type != taskjournal.TaskScript) || len(validated.ScriptRunnerSnapshots) == 0 ||
		len(validated.ScriptRunnerSnapshots) != len(validated.ScriptBodyArtifacts) ||
		validated.RenderGeneration > math.MaxInt32 || task.PlanID != validated.PlanId ||
		task.RenderGeneration != int32(validated.RenderGeneration) ||
		task.PlanHash != hex.EncodeToString(validated.PlanHash) || len(task.Steps) != len(validated.Steps) {
		return nil, errs.New(errs.KindValidationFailed, "Script Task and execution plan shape do not match")
	}
	for index := range task.Steps {
		if task.Steps[index].ID != validated.Steps[index].StepId {
			return nil, errs.New(errs.KindValidationFailed, "Script Task steps do not bind their sealed plan")
		}
	}
	planBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(planBytes)
	snapshots := make(map[string]*agentpb.ResolvedRunnerSnapshot, len(validated.ScriptRunnerSnapshots))
	for _, snapshot := range validated.ScriptRunnerSnapshots {
		snapshots[snapshot.ScriptExecutionId] = snapshot
	}
	records := make([]scriptexecutions.ScriptExecutionRecord, 0, len(snapshots))
	for _, step := range validated.Steps {
		run := step.GetRunScript()
		if run == nil {
			continue
		}
		snapshot := snapshots[run.ScriptExecutionId]
		if snapshot == nil || snapshot.SnapshotId != run.RunnerSnapshotId {
			return nil, errs.New(errs.KindValidationFailed, "RunScript snapshot is missing")
		}
		if task.Type == taskjournal.TaskScript && (task.Target != run.ScriptId || len(validated.Steps) != 1 ||
			task.TimeoutSeconds != executionplan.ScriptExecutionTimeoutSeconds ||
			task.Params[scriptexecutions.ScriptExecutionIDParam] != run.ScriptExecutionId ||
			task.Params[scriptexecutions.ScriptGenerationParam] != strconv.FormatUint(run.ScriptGeneration, 10)) {
			return nil, errs.New(errs.KindValidationFailed, "Script Task does not bind its sealed execution plan")
		}
		snapshotBytes, marshalErr := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
		if marshalErr != nil {
			return nil, errs.Wrap(errs.KindInternal, marshalErr)
		}
		record := scriptexecutions.ScriptExecutionRecord{
			ID: run.ScriptExecutionId, SnapshotID: run.RunnerSnapshotId,
			OperationID: task.OperationID, CurrentTaskID: task.ID, StepID: step.StepId,
			ScriptID: run.ScriptId, ScriptGeneration: run.ScriptGeneration,
			EnvironmentID: run.EnvironmentId, ServiceID: run.ServiceId, ReleaseID: run.ReleaseId,
			RenderGeneration: run.RenderGeneration, PlanHash: hex.EncodeToString(validated.PlanHash),
			SnapshotSHA256: hex.EncodeToString(
				run.RunnerSnapshotSha256,
			), BodySHA256: hex.EncodeToString(run.BodySha256),
			RunnerProjectionSHA256: hex.EncodeToString(snapshot.RunnerProjectionSha256),
			Plan:                   append([]byte(nil), planBytes...), Snapshot: snapshotBytes,
			State: scriptexecutions.ScriptExecutionNotStarted, ActiveReference: true, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
		}
		if err := scriptexecutions.ValidateScriptExecutionRecord(record); err != nil {
			clear(record.Plan)
			clear(record.Snapshot)
			return nil, err
		}
		records = append(records, record)
	}
	if len(records) != len(snapshots) {
		return nil, errs.New(errs.KindValidationFailed, "Script execution set does not match its plan")
	}
	return records, nil
}

func validateScriptExecutionSources(sources ScriptExecutionSources, execution scriptexecutions.ScriptExecutionRecord) error {
	if sources.Revision <= 0 || sources.Tenant.ReadRevision != sources.Revision ||
		sources.Project.ReadRevision != sources.Revision || sources.Environment.ReadRevision != sources.Revision ||
		sources.Service.ReadRevision != sources.Revision || sources.ScriptSet.ReadRevision != sources.Revision ||
		sources.Script.ReadRevision != sources.Revision ||
		sources.BodyGeneration.ReadRevision != sources.Revision || sources.RenderInput.ReadRevision != sources.Revision ||
		sources.DesiredHead.ReadRevision != sources.Revision || sources.DesiredHead.Revision <= 0 ||
		sources.DesiredProjection.ReadRevision != sources.Revision || sources.DesiredProjection.Revision <= 0 ||
		sources.Release.Revision != sources.Revision || sources.Script.Record.Desired.ID != execution.ScriptID ||
		sources.ScriptSet.Revision <= 0 || sources.ScriptSet.Record.EnvironmentID != execution.EnvironmentID ||
		sources.ScriptSet.Record.GenerationID != execution.ScriptSetGeneration ||
		sources.Script.Record.ScriptSetGeneration != execution.ScriptSetGeneration ||
		sources.Script.Record.ActiveGeneration != execution.ScriptGeneration ||
		sources.BodyGeneration.Record.ScriptID != execution.ScriptID ||
		sources.BodyGeneration.Record.Generation != execution.ScriptGeneration ||
		sources.Environment.Record.ID != execution.EnvironmentID || sources.Service.Record.Desired.ID != execution.ServiceID ||
		sources.Release.Intent.ID != execution.ReleaseID || sources.RenderInput.Record.ReleaseID != execution.ReleaseID ||
		sources.RenderInput.Record.Projection.RenderGeneration != execution.RenderGeneration ||
		sources.DesiredHead.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredHead.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredProjection.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.RenderGeneration == 0 ||
		sources.BodyGeneration.Record.BodySHA256 != execution.BodySHA256 {
		return errs.New(errs.KindValidationFailed, "Script execution sources do not match the execution")
	}
	desiredZoneNames := make(map[string]string, len(sources.DesiredProjection.Record.DesiredZones))
	for _, desired := range sources.DesiredProjection.Record.DesiredZones {
		if _, duplicate := desiredZoneNames[desired.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Script execution desired zone identity is duplicated")
		}
		desiredZoneNames[desired.Desired.ID] = desired.Desired.Name
	}
	if len(sources.Networks) != len(desiredZoneNames) {
		return errs.New(errs.KindValidationFailed, "Script execution network sources are incomplete")
	}
	for _, network := range sources.Networks {
		identity, found := desiredZoneNames[network.Record.Desired.ID]
		if network.ReadRevision != sources.Revision || network.Revision != sources.DesiredProjection.Revision ||
			network.Record.EnvironmentID != execution.EnvironmentID || !found || network.Record.Desired.Name != identity {
			return errs.New(errs.KindValidationFailed, "Script execution network source revision is invalid")
		}
	}
	return validateStoredScriptContext(sources, execution)
}

func scriptExecutionProjectionConditions(sources ScriptExecutionSources) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{
			Key:         blueprints.EnvironmentBlueprintHeadKey(sources.Environment.Record.ID),
			ModRevision: sources.DesiredHead.Revision,
		},
		{
			Key: blueprints.EnvironmentBlueprintRootKey(
				sources.Environment.Record.ID, sources.DesiredProjection.Record.RevisionID,
			),
			ModRevision: sources.DesiredProjection.Revision,
		},
	}
	for _, network := range sources.Networks {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), network.Record.Desired.ID),
		})
	}
	conditions = append(conditions, scriptAttachSourceConditions(sources.AttachSources)...)
	return conditions
}
