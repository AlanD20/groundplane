package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingHookAssignmentReader interface {
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}

type backingHookPlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
}

type backingHookCheckpointRepository interface {
	GetAttach(context.Context, string) (etcd.Versioned[etcd.AttachRecord], error)
	CheckpointBackingHook(
		context.Context,
		etcd.BackingHookCheckpointInput,
	) (etcd.Versioned[etcd.BackingHookCheckpointRecord], bool, error)
}

type backingHookFactSealer interface {
	SealHookResult(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		[]backinghook.FactDefinition,
		backinghook.Output,
	) (etcd.AttachEncryptedFacts, error)
}

type BackingHookCheckpointService struct {
	assignments backingHookAssignmentReader
	plans       backingHookPlanResolver
	repository  backingHookCheckpointRepository
	facts       backingHookFactSealer
	now         func() time.Time
}

func NewBackingHookCheckpointService(
	assignments backingHookAssignmentReader,
	plans backingHookPlanResolver,
	repository backingHookCheckpointRepository,
	facts backingHookFactSealer,
) (*BackingHookCheckpointService, error) {
	if assignments == nil || plans == nil || repository == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Backing hook checkpoint dependencies are required")
	}
	return &BackingHookCheckpointService{
		assignments: assignments, plans: plans, repository: repository, facts: facts, now: time.Now,
	}, nil
}

func (service *BackingHookCheckpointService) CheckpointBackingHook(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	request *agentpb.BackingHookCheckpointRequest,
) (*agentpb.BackingHookCheckpointAck, error) {
	if ctx == nil || service == nil || service.now == nil || agentID == "" || agentGeneration == 0 {
		return nil, errs.New(errs.KindInternal, "Backing hook checkpoint service is not configured")
	}
	validated, err := executionplan.ValidateBackingHookCheckpointRequest(request)
	if err != nil {
		return nil, err
	}
	assignment, err := service.assignments.GetTaskAssignment(ctx, validated.GetTaskId())
	if err != nil {
		return nil, err
	}
	if assignment.Task.Record.OperationID != validated.GetOperationId() ||
		assignment.Assignment.Record.AssignmentID != validated.GetAssignmentId() ||
		assignment.Assignment.Record.ExecutionEpoch != validated.GetExecutionEpoch() ||
		assignment.Assignment.Record.AgentID != agentID ||
		assignment.Assignment.Record.AgentGeneration != agentGeneration {
		return nil, errs.New(errs.KindStateConflict, "Backing hook checkpoint assignment changed")
	}
	planned, err := service.plans.ResolveExecutionPlan(ctx, assignment.Task.Record)
	if err != nil {
		return nil, err
	}
	defer ClearBackingHookPlan(planned)
	if !bytes.Equal(planned.GetPlanHash(), validated.GetPlanHash()) {
		return nil, errs.New(errs.KindStateConflict, "Backing hook checkpoint plan changed")
	}
	var procedure *agentpb.BackingHookProcedure
	for _, step := range planned.GetSteps() {
		if step.GetStepId() == validated.GetStepId() {
			procedure = step.GetBackingHookProcedure()
			break
		}
	}
	definition, _, schema, err := executionplan.DecodeBackingHookProcedure(procedure)
	if err != nil || procedure.GetEvent() != validated.GetEvent() || definition.TimeoutSeconds == 0 {
		return nil, errs.New(errs.KindStateConflict, "Backing hook checkpoint step changed")
	}
	state := etcd.BackingHookCheckpointStarted
	var sealed *etcd.AttachEncryptedFacts
	if validated.GetState() == agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_RESULT {
		state = etcd.BackingHookCheckpointResult
		output := backingHookCheckpointOutput(schema, validated.GetFacts())
		defer output.Clear()
		if err := backinghook.ValidateOutput(schema, output); err != nil {
			return nil, err
		}
		if procedure.GetEvent() == agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH {
			current, getErr := service.repository.GetAttach(ctx, procedure.GetAttachId())
			if getErr != nil {
				return nil, getErr
			}
			facts, sealErr := service.facts.SealHookResult(ctx, current, schema, output)
			if sealErr != nil {
				return nil, sealErr
			}
			sealed = &facts
			defer clear(facts.Ciphertext)
		}
	}
	_, existing, err := service.repository.CheckpointBackingHook(ctx, etcd.BackingHookCheckpointInput{
		TaskID: validated.GetTaskId(), OperationID: validated.GetOperationId(),
		AssignmentID: validated.GetAssignmentId(), AgentID: agentID, AgentGeneration: agentGeneration,
		ExecutionEpoch: validated.GetExecutionEpoch(), StepID: validated.GetStepId(),
		PlanHash: hex.EncodeToString(validated.GetPlanHash()), AttachID: procedure.GetAttachId(),
		Event: backingHookCheckpointEvent(validated.GetEvent()), State: state,
		ResultSHA256: hex.EncodeToString(validated.GetResultSha256()), Facts: sealed, At: service.now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	return &agentpb.BackingHookCheckpointAck{
		TaskId: validated.GetTaskId(), OperationId: validated.GetOperationId(),
		AssignmentId: validated.GetAssignmentId(), ExecutionEpoch: validated.GetExecutionEpoch(),
		StepId: validated.GetStepId(), PlanHash: append([]byte(nil), validated.GetPlanHash()...),
		Event: validated.GetEvent(), State: validated.GetState(),
		ResultSha256: append([]byte(nil), validated.GetResultSha256()...), Existing: existing,
	}, nil
}

func ClearBackingHookPlan(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.GetSteps() {
		ClearBackingHookProcedure(step.GetBackingHookProcedure())
	}
}

func backingHookCheckpointOutput(
	schema []backinghook.FactDefinition,
	values []*agentpb.BackingHookValue,
) backinghook.Output {
	secret := make(map[string]bool, len(schema))
	for _, definition := range schema {
		secret[definition.Key] = definition.Secret
	}
	output := backinghook.Output{Facts: make([]backinghook.Fact, 0, len(values))}
	for _, value := range values {
		output.Facts = append(output.Facts, backinghook.Fact{
			Key: value.GetKey(), Value: append([]byte(nil), value.GetValue()...), Secret: secret[value.GetKey()],
		})
	}
	return output
}

func backingHookCheckpointEvent(event agentpb.BackingHookEvent) string {
	decoded, _ := executionplan.BackingHookEvent(event)
	return string(decoded)
}
