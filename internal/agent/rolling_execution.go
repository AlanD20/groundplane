package agent

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func rollingStepMember(plan *agentpb.ExecutionPlan, stepID string) *agentpb.CandidateReleaseMember {
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		if member.GetRolling() != nil && slices.Contains(member.GetForwardStepIds(), stepID) {
			return member
		}
	}
	return nil
}

func (p *WorkerPool) beginRollingStep(reservation *taskReservation, step *agentpb.ExecutionStep,
	member *agentpb.CandidateReleaseMember, running TaskProgress,
) (*agentpb.RollingStepBudget, error) {
	if !reservation.eventsDurable ||
		reservation.assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD {
		return nil, errs.New(errs.KindStateConflict, "agent: rolling execution requires durable forward event receipts")
	}
	if _, _, err := executionplan.RollingStepPosition(member, step.GetStepId()); err != nil {
		return nil, err
	}
	ack, err := p.emitProgressForReceipt(reservation.ctx, running)
	if err != nil {
		return nil, err
	}
	budget := ack.GetRollingBudget()
	if budget == nil || budget.GetDeadline() == nil || budget.GetDeadline().CheckValid() != nil ||
		budget.GetDeadline().AsTime().After(reservation.assignment.Deadline) ||
		(step.GetRollingReplicaDrain() != nil) != (budget.GetDrainUntil() != nil) {
		return nil, errs.New(errs.KindStateConflict, "agent: rolling phase has no matching Controller execution budget")
	}
	if !time.Now().Before(budget.GetDeadline().AsTime()) {
		return nil, context.DeadlineExceeded
	}
	return budget, nil
}

// The durable switch completion selected this deadline. Repeated delivery or
// time spent awaiting the start receipt must not add another drain interval.
func waitRollingDrain(ctx context.Context, budget *agentpb.RollingStepBudget) error {
	if budget.GetDrainUntil() == nil {
		return errs.New(errs.KindStateConflict, "agent: rolling drain has no Controller deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	wait := time.Until(budget.GetDrainUntil().AsTime())
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
