package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type executionStepResultStore interface {
	AcknowledgeExecutionStepResult(
		context.Context,
		etcd.ExecutionStepResultInput,
	) (etcd.ExecutionStepResultWrite, error)
	ListExecutionStepResults(context.Context, string, string) ([]etcd.ExecutionStepResultRecord, error)
}

func (s *Server) acknowledgeExecutionStepResult(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	request *agentpb.ExecutionStepResultRequest,
) (*agentpb.ExecutionStepResultAck, error) {
	if _, err := executionplan.ValidateExecutionStepResultRequest(request); err != nil {
		return nil, err
	}
	store, ok := s.tasks.(executionStepResultStore)
	if !ok {
		return nil, errs.New(errs.KindInternal, "execution step result store is not configured")
	}
	task, err := s.tasks.GetTask(ctx, request.GetTaskId())
	if err != nil {
		return nil, err
	}
	result := request.GetResult()
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, result.GetPlanHash()) ||
		task.Record.OperationID != result.GetOperationId() ||
		task.Record.Status != etcd.TaskStatusRunning {
		return nil, errs.New(errs.KindStateConflict, "execution step result does not match the running Task")
	}
	if s.plans == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	resolved, err := s.plans.ResolveExecutionPlan(ctx, task.Record)
	if err != nil {
		return nil, err
	}
	plan, err := executionplan.Validate(resolved)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if _, err := executionplan.ValidateExecutionStepResultForPlan(plan, result); err != nil {
		return nil, err
	}
	write, err := store.AcknowledgeExecutionStepResult(ctx, etcd.ExecutionStepResultInput{
		TaskID: request.GetTaskId(), AssignmentID: request.GetAssignmentId(),
		AgentID: agentID, AgentGeneration: agentGeneration,
		Result: result, At: s.now(),
	})
	if err != nil {
		return nil, err
	}
	durable, err := write.Record.Proto()
	if err != nil {
		return nil, err
	}
	ack := &agentpb.ExecutionStepResultAck{
		TaskId: request.GetTaskId(), AssignmentId: request.GetAssignmentId(),
		OperationId: durable.GetOperationId(), PlanHash: append([]byte(nil), durable.GetPlanHash()...),
		StepId:               durable.GetStepId(),
		ControlPayloadSha256: append([]byte(nil), durable.GetControlPayloadSha256()...),
	}
	if _, err := executionplan.ValidateExecutionStepResultAck(ack, request); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return ack, nil
}

func (s *Server) acknowledgedExecutionStepResults(
	ctx context.Context,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
) ([]*agentpb.ExecutionStepResult, error) {
	requiresResults := false
	for _, snapshot := range plan.GetScriptRunnerSnapshots() {
		if snapshot.GetProcedureServiceImage() != nil {
			requiresResults = true
			break
		}
	}
	if !requiresResults {
		return nil, nil
	}
	store, ok := s.tasks.(executionStepResultStore)
	if !ok {
		return nil, errs.New(errs.KindInternal, "execution step result store is not configured")
	}
	records, err := store.ListExecutionStepResults(ctx, task.OperationID, task.PlanHash)
	if err != nil {
		return nil, err
	}
	results := make([]*agentpb.ExecutionStepResult, 0, len(records))
	for _, record := range records {
		result, protoErr := record.Proto()
		if protoErr != nil {
			return nil, protoErr
		}
		results = append(results, result)
	}
	if _, err := executionplan.ValidateExecutionStepResults(plan, results); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return results, nil
}
