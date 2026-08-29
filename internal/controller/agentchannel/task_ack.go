package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) acknowledge(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	acknowledgement *agentpb.TaskAck,
) error {
	if acknowledgement == nil ||
		ids.Validate(ids.KindAssignment, acknowledgement.AssignmentId) != nil ||
		len(acknowledgement.PlanHash) != 32 {
		return errs.New(errs.KindValidationFailed, "Agent Task acknowledgement is invalid")
	}
	task, err := s.tasks.GetTask(ctx, acknowledgement.TaskId)
	if err != nil {
		return err
	}
	environmentTarget := ids.Validate(ids.KindEnvironment, task.Record.Target) == nil
	environmentCreation := task.Record.Type == etcd.TaskCreate && environmentTarget
	if s.plans == nil {
		return errs.New(errs.KindInternal, "Agent Task plan resolver is not configured")
	}
	plan, err := s.plans.ResolveExecutionPlan(ctx, task.Record)
	if err != nil {
		return err
	}
	environmentDirectory := executionplan.UsesEnvironmentDirectoryResult(plan)
	if environmentDirectory {
		if err := validateEnvironmentDirectoryTaskResult(acknowledgement); err != nil {
			return err
		}
	} else if err := validateComposeTaskResult(acknowledgement); err != nil {
		return err
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, acknowledgement.PlanHash) {
		return errs.New(
			errs.KindStateConflict,
			"Agent Task acknowledgement plan hash does not match",
		)
	}
	var terminal etcd.TaskStatus
	switch acknowledgement.Terminal {
	case agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED:
		terminal = etcd.TaskStatusCompleted
	case agentpb.TaskTerminal_TASK_TERMINAL_FAILED:
		terminal = etcd.TaskStatusFailed
	case agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT:
		terminal = etcd.TaskStatusTimedOut
	case agentpb.TaskTerminal_TASK_TERMINAL_ABORTED:
		terminal = etcd.TaskStatusAborted
	default:
		return errs.New(
			errs.KindValidationFailed,
			"Agent Task acknowledgement terminal state is invalid",
		)
	}
	if environmentCreation {
		store, ok := s.tasks.(environmentCreationTaskStore)
		if !ok {
			return errs.New(errs.KindInternal, "Environment creation Task store is not configured")
		}
		_, err = store.AcknowledgeEnvironmentCreation(
			ctx,
			agentID,
			agentGeneration,
			acknowledgement.TaskId,
			acknowledgement.AssignmentId,
			task.Record.Target,
			terminal,
			durableEnvironmentDirectoryTaskResult(acknowledgement),
			s.now().UTC(),
		)
	} else {
		result := durableComposeTaskResult(acknowledgement)
		if environmentDirectory {
			result = durableEnvironmentDirectoryTaskResult(acknowledgement)
		}
		_, err = s.tasks.AcknowledgeTask(
			ctx,
			agentID,
			agentGeneration,
			acknowledgement.TaskId,
			acknowledgement.AssignmentId,
			terminal,
			result,
			s.now().UTC(),
		)
	}
	return err
}
