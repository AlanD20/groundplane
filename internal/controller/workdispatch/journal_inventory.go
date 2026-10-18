package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AccountJournal checks unsubmitted and partial local records against the actual
// durable attempt. Readiness separately reconciles interrupted operation effects.
// An unrecognized file remains intact and prevents blind startup adoption.
func (delivery *AgentDeliveries) AccountJournal(ctx context.Context, authorization agentregistration.WorkAuthorization, attemptID string, expected *agentpb.WorkAttemptIdentity) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	stored, err := delivery.queue.ReadJournalAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	attempt := stored.Attempt
	retained, err := authorization.ForReceipt(attempt.Executor)
	if err != nil {
		return err
	}
	if expected != nil {
		claimed, err := workexecution.DecodeAgentAttempt(expected, attempt.Executor)
		if executionplan.RejectUnknown(expected) != nil || err != nil || !workexecution.SameAssignment(claimed, attempt) {
			return errs.New(errs.KindStateConflict, "local work journal differs from its captured Controller assignment")
		}
	}
	view, err := retained.ReadResult(ctx, delivery.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return err
	}
	if !workexecution.SameAssignment(view.Attempt.Attempt, attempt) {
		return errs.New(errs.KindStateConflict, "journal attempt changed during startup accounting")
	}
	return delivery.queue.ConfirmJournalOwnership(ctx, view, authorization.Identity(), guards)
}
