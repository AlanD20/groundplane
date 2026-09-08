package agent

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const scriptCheckpointTimeout = 30 * time.Second

type DockerScriptRuntime struct {
	engine scriptexecution.Engine
}

func NewDockerScriptRuntime(engine scriptexecution.Engine) (*DockerScriptRuntime, error) {
	if engine == nil {
		return nil, errs.New(errs.KindInternal, "agent: Script execution engine is required")
	}
	return &DockerScriptRuntime{engine: engine}, nil
}

func (runtime *DockerScriptRuntime) ExecuteScript(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (int32, error) {
	request, durable, err := scriptExecutionRequest(ctx, assignment, step, checkpoint)
	if err != nil {
		return 0, err
	}
	defer clearScriptExecutionRequest(&request)
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return scriptDurableResult(durable)
	}

	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED {
		if ctx.Err() != nil || !time.Now().Before(assignment.Deadline) {
			reason := agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START
			if errors.Is(ctx.Err(), context.DeadlineExceeded) || !time.Now().Before(assignment.Deadline) {
				reason = agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START
			}
			if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
				return 0, err
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		if err := checkpointStart(checkpoint, request, durable); err != nil {
			return 0, err
		}
	}

	var body scriptexecution.BodyEvidence
	if durable.BodyPrepared != nil {
		body = bodyEvidenceFromCheckpoint(durable.BodyPrepared)
	}
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED ||
		durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED ||
		durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		prepared, prepareErr := runtime.engine.PrepareBody(ctx, request)
		if durable.BodyPrepared != nil && !sameBodyEvidence(prepared, body) {
			prepareErr = errors.Join(
				prepareErr,
				errs.New(errs.KindStateConflict, "agent: recovered Script body evidence differs"),
			)
		}
		if durable.BodyPrepared == nil && prepared.Device != 0 {
			if err := checkpointBody(checkpoint, request, durable, prepared); err != nil {
				return 0, err
			}
			body = prepared
		}
		if prepareErr != nil {
			reason := scriptFailureReason(ctx, durable.State, prepareErr)
			if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
				return 0, errors.Join(prepareErr, err)
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		body = prepared
	}

	var containerEvidence scriptexecution.ContainerEvidence
	if durable.ContainerCreated != nil {
		containerEvidence = containerEvidenceFromCheckpoint(durable.ContainerCreated)
	}
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED {
		recovered, recoverErr := runtime.engine.RecoverContainer(ctx, request, body)
		if recoverErr != nil {
			if err := checkpointOutcome(
				checkpoint, request, durable,
				agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE, nil,
			); err != nil {
				return 0, errors.Join(recoverErr, err)
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		if recovered.Found {
			containerEvidence = recovered.Evidence
		} else {
			containerEvidence, err = runtime.engine.CreateContainer(ctx, request, body)
			if err != nil {
				reason := scriptFailureReason(ctx, durable.State, err)
				if containerEvidence.ID != "" {
					if reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START {
						reason = agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT
					}
					if checkpointErr := checkpointContainer(checkpoint, request, durable, containerEvidence); checkpointErr != nil {
						return 0, errors.Join(err, checkpointErr)
					}
				}
				if checkpointErr := checkpointOutcome(checkpoint, request, durable, reason, nil); checkpointErr != nil {
					return 0, errors.Join(err, checkpointErr)
				}
				exitCode, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
				return exitCode, errors.Join(err, cleanupErr)
			}
		}
		if err := checkpointContainer(checkpoint, request, durable, containerEvidence); err != nil {
			return 0, err
		}
	}

	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		result, runErr := runtime.engine.RunContainer(ctx, request, body, containerEvidence)
		if runErr != nil {
			reason := scriptFailureReason(ctx, durable.State, runErr)
			if checkpointErr := checkpointOutcome(checkpoint, request, durable, reason, nil); checkpointErr != nil {
				return result.ExitCode, errors.Join(runErr, checkpointErr)
			}
			exitCode, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
			return exitCode, errors.Join(runErr, cleanupErr)
		} else if err := checkpointOutcome(
			checkpoint, request, durable,
			agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT, &result.ExitCode,
		); err != nil {
			return result.ExitCode, err
		}
	}
	return runtime.cleanupAndReturn(checkpoint, request, durable)
}

// CompleteScriptWithoutStart records a release hook that cannot safely select
// a serving Release. It proves cleanup without authorizing body preparation or
// container creation.
func (runtime *DockerScriptRuntime) CompleteScriptWithoutStart(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	reason agentpb.ScriptOutcomeReason,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) error {
	if reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE &&
		reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE {
		return errs.New(errs.KindValidationFailed, "agent: Script no-start outcome reason is invalid")
	}
	request, durable, err := scriptExecutionRequest(ctx, assignment, step, checkpoint)
	if err != nil {
		return err
	}
	defer clearScriptExecutionRequest(&request)
	switch durable.State {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
			return err
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		if durable.GetOutcome().GetReason() != reason {
			return errs.New(errs.KindStateConflict, "agent: recovered Script no-start outcome differs")
		}
		if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
			return nil
		}
	default:
		return errs.New(errs.KindStateConflict, "agent: Script already crossed its no-start boundary")
	}
	_, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return nil
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	return errs.New(errs.KindInternal, "agent: Script no-start cleanup was not proven")
}

func scriptExecutionRequest(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (scriptexecution.Request, *agentpb.ScriptExecutionCheckpoint, error) {
	if ctx == nil || checkpoint == nil || step == nil || step.GetRunScript() == nil || assignment.Plan == nil ||
		assignment.ScriptArtifacts == nil {
		return scriptexecution.Request{}, nil, errs.New(
			errs.KindInternal,
			"agent: Script execution request is incomplete",
		)
	}
	run := step.GetRunScript()
	var projection *agentpb.ScriptRunnerProjection
	var snapshot *agentpb.ResolvedRunnerSnapshot
	for _, candidate := range assignment.Plan.ScriptRunnerProjections {
		if candidate != nil && candidate.SnapshotId == run.RunnerSnapshotId {
			projection = candidate
			break
		}
	}
	for _, candidate := range assignment.Plan.ScriptRunnerSnapshots {
		if candidate != nil && candidate.SnapshotId == run.RunnerSnapshotId {
			snapshot = candidate
			break
		}
	}
	var body *agentpb.ScriptBodyArtifact
	for _, candidate := range assignment.ScriptArtifacts.Bodies {
		if candidate != nil && candidate.Metadata != nil &&
			candidate.Metadata.ScriptExecutionId == run.ScriptExecutionId {
			body = candidate
			break
		}
	}
	if projection == nil || snapshot == nil || body == nil || body.Metadata == nil ||
		projection.SnapshotId != run.RunnerSnapshotId ||
		body.Metadata.ScriptExecutionId != run.ScriptExecutionId || body.Metadata.ScriptId != run.ScriptId ||
		body.Metadata.Generation != run.ScriptGeneration {
		return scriptexecution.Request{}, nil, errs.New(
			errs.KindInternal,
			"agent: Script execution artifacts do not match RunScript",
		)
	}
	var durable *agentpb.ScriptExecutionCheckpoint
	for _, candidate := range assignment.ScriptCheckpoints {
		if candidate != nil && candidate.ScriptExecutionId == run.ScriptExecutionId {
			durable = candidate
			break
		}
	}
	durable, err := executionplan.ValidateScriptExecutionCheckpoint(durable)
	if err != nil {
		return scriptexecution.Request{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	return scriptexecution.Request{
		TaskID: assignment.TaskID, OperationID: assignment.OperationID, AssignmentID: assignment.AssignmentID,
		StepID: step.StepId, ExecutionID: run.ScriptExecutionId,
		PlanHash:     append([]byte(nil), assignment.Plan.PlanHash...),
		Projection:   projection,
		BodyMetadata: proto.Clone(body.Metadata).(*agentpb.ScriptBodyArtifactMetadata),
		Body:         append([]byte(nil), body.Body...),
		Entries: cloneScriptEntriesForSnapshot(
			assignment.ScriptArtifacts.Entries,
			assignment.Plan,
			run.RunnerSnapshotId,
		),
	}, durable, nil
}

func cloneScriptEntriesForSnapshot(
	entries []*agentpb.ScriptEntryArtifact,
	plan *agentpb.ExecutionPlan,
	snapshotID string,
) []*agentpb.ScriptEntryArtifact {
	wanted := make(map[string]struct{})
	for _, snapshot := range plan.ScriptRunnerSnapshots {
		if snapshot == nil || snapshot.SnapshotId != snapshotID {
			continue
		}
		for _, binding := range snapshot.EntryBindings {
			wanted[binding.EntryId+"\x00"+binding.ValueGenerationId] = struct{}{}
		}
	}
	selected := make([]*agentpb.ScriptEntryArtifact, 0, len(wanted))
	for _, entry := range entries {
		if entry == nil || entry.Binding == nil {
			continue
		}
		key := entry.Binding.EntryId + "\x00" + entry.Binding.ValueGenerationId
		if _, exists := wanted[key]; exists {
			selected = append(selected, entry)
		}
	}
	return cloneScriptEntries(selected)
}

func checkpointStart(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_StartAuthorized{
		StartAuthorized: &agentpb.ScriptStartAuthorizedCheckpoint{},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State, durable.StartAuthorized = wire.State, true
	return nil
}

func checkpointBody(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	body scriptexecution.BodyEvidence,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_BodyPrepared{
		BodyPrepared: &agentpb.ScriptBodyPreparedCheckpoint{
			BodySha256: append([]byte(nil), body.SHA256...), Uid: body.UID, Gid: body.GID,
			Device: body.Device, Inode: body.Inode, Leaf: body.Leaf,
		},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.BodyPrepared = proto.Clone(wire.GetBodyPrepared()).(*agentpb.ScriptBodyPreparedCheckpoint)
	return nil
}

func checkpointContainer(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	evidence scriptexecution.ContainerEvidence,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_ContainerCreated{
		ContainerCreated: &agentpb.ScriptContainerCreatedCheckpoint{
			ContainerId:           evidence.ID,
			OwnershipLabelsSha256: append([]byte(nil), evidence.OwnershipLabelsSHA256...),
		},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.ContainerCreated = proto.Clone(wire.GetContainerCreated()).(*agentpb.ScriptContainerCreatedCheckpoint)
	return nil
}

func checkpointOutcome(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	reason agentpb.ScriptOutcomeReason,
	exitCode *int32,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
	)
	outcome := &agentpb.ScriptOutcomeCheckpoint{Reason: reason, ObservedAt: timestamppb.Now()}
	if exitCode != nil {
		outcome.ExitCode = new(int32)
		*outcome.ExitCode = *exitCode
	}
	wire.Evidence = &agentpb.ScriptCheckpointRequest_Outcome{Outcome: outcome}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.Outcome = proto.Clone(outcome).(*agentpb.ScriptOutcomeCheckpoint)
	durable.ReconciliationRequired = reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	return nil
}

func (runtime *DockerScriptRuntime) cleanupAndReturn(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
) (int32, error) {
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
		var body *scriptexecution.BodyEvidence
		if durable.BodyPrepared != nil {
			value := bodyEvidenceFromCheckpoint(durable.BodyPrepared)
			body = &value
		}
		var container *scriptexecution.ContainerEvidence
		if durable.ContainerCreated != nil {
			value := containerEvidenceFromCheckpoint(durable.ContainerCreated)
			container = &value
		}
		proof, err := runtime.engine.Cleanup(request, body, container)
		if err != nil {
			return 0, err
		}
		wire := newScriptCheckpointRequest(
			request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN,
		)
		wire.Evidence = &agentpb.ScriptCheckpointRequest_Cleanup{
			Cleanup: &agentpb.ScriptCleanupCheckpoint{
				ContainerId: proof.ContainerID, BodyDevice: proof.BodyDevice, BodyInode: proof.BodyInode,
				BodyLeaf: proof.BodyLeaf, ContainerAbsent: proof.ContainerAbsent, BodyAbsent: proof.BodyAbsent,
				ExecutionDirectoryAbsent: proof.ExecutionDirectoryAbsent,
			},
		}
		if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
			return 0, err
		}
		durable.State = wire.State
		durable.Cleanup = proto.Clone(wire.GetCleanup()).(*agentpb.ScriptCleanupCheckpoint)
	}
	return scriptDurableResult(durable)
}

func newScriptCheckpointRequest(
	request scriptexecution.Request,
	expected agentpb.ScriptExecutionState,
	state agentpb.ScriptExecutionState,
) *agentpb.ScriptCheckpointRequest {
	return &agentpb.ScriptCheckpointRequest{
		TaskId: request.TaskID, OperationId: request.OperationID, AssignmentId: request.AssignmentID,
		StepId: request.StepID, ScriptExecutionId: request.ExecutionID,
		PlanHash: append([]byte(nil), request.PlanHash...), ExpectedState: expected, State: state,
	}
}

func sendScriptCheckpoint(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request *agentpb.ScriptCheckpointRequest,
) error {
	digest, err := executionplan.ComputeScriptCheckpointPayloadDigest(request)
	if err != nil {
		return err
	}
	request.ControlPayloadSha256 = digest
	controlCtx, cancel := context.WithTimeout(context.Background(), scriptCheckpointTimeout)
	defer cancel()
	return checkpoint(controlCtx, request)
}

func scriptFailureReason(
	ctx context.Context,
	state agentpb.ScriptExecutionState,
	err error,
) agentpb.ScriptOutcomeReason {
	if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		if state == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
			return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT
		}
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START
	}
	if state == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE
	}
	return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE
}

func scriptDurableResult(checkpoint *agentpb.ScriptExecutionCheckpoint) (int32, error) {
	if checkpoint == nil || checkpoint.Outcome == nil {
		return 0, errs.New(errs.KindInternal, "agent: Script outcome is missing")
	}
	outcome := checkpoint.Outcome
	switch outcome.Reason {
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT:
		exitCode := outcome.GetExitCode()
		if exitCode != 0 {
			return exitCode, errs.Newf(errs.KindStateConflict, "Script container exited with status %d", exitCode)
		}
		return 0, nil
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT,
		agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START:
		return 0, context.DeadlineExceeded
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT,
		agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START:
		return 0, context.Canceled
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE:
		return 0, errs.New(errs.KindStateConflict, "Script recovery invariant failed")
	default:
		return 0, errs.New(errs.KindInternal, "Script execution failed")
	}
}

func bodyEvidenceFromCheckpoint(value *agentpb.ScriptBodyPreparedCheckpoint) scriptexecution.BodyEvidence {
	return scriptexecution.BodyEvidence{
		SHA256: append([]byte(nil), value.GetBodySha256()...), UID: value.GetUid(), GID: value.GetGid(),
		Device: value.GetDevice(), Inode: value.GetInode(), Leaf: value.GetLeaf(),
	}
}

func containerEvidenceFromCheckpoint(
	value *agentpb.ScriptContainerCreatedCheckpoint,
) scriptexecution.ContainerEvidence {
	return scriptexecution.ContainerEvidence{
		ID: value.GetContainerId(), OwnershipLabelsSHA256: append([]byte(nil), value.GetOwnershipLabelsSha256()...),
	}
}

func sameBodyEvidence(left, right scriptexecution.BodyEvidence) bool {
	return bytes.Equal(left.SHA256, right.SHA256) && left.UID == right.UID && left.GID == right.GID &&
		left.Device == right.Device && left.Inode == right.Inode && left.Leaf == right.Leaf
}

func cloneScriptEntries(values []*agentpb.ScriptEntryArtifact) []*agentpb.ScriptEntryArtifact {
	result := make([]*agentpb.ScriptEntryArtifact, len(values))
	for index, value := range values {
		result[index] = proto.Clone(value).(*agentpb.ScriptEntryArtifact)
	}
	return result
}

func clearScriptExecutionRequest(request *scriptexecution.Request) {
	if request == nil {
		return
	}
	clear(request.Body)
	request.Body = nil
	for _, entry := range request.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}

func (runtime *DockerScriptRuntime) Close() error {
	if runtime == nil || runtime.engine == nil {
		return nil
	}
	return runtime.engine.Close()
}
