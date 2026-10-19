package taskinspection

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) agentOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil {
		return nil
	}
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.AgentLifecycle &&
		view.Item.Item.Operation != workexecution.AgentSoftware {
		return nil
	}
	input, err := service.agentLifecycle.Load(ctx, view.Item.Item, result.Revision)
	if err != nil {
		return err
	}
	saved, found, err := service.agentLifecycle.ReadResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	_, proof, err := agentoperations.PrepareResult(input, saved, attempt)
	if err != nil {
		return err
	}
	if attempt.Result == nil || *attempt.Result != proof.Evidence {
		return errs.New(errs.KindStateConflict, "Agent lifecycle observation differs from its accepted result")
	}
	value := saved.Record.Result
	stage := workexecution.Stage{
		Key:        "agent_lifecycle_result",
		Name:       "Agent lifecycle outcome",
		Expected:   "Prove the captured runtime's authenticated readiness or exact removal",
		State:      workexecution.StageFailed,
		StartedAt:  attempt.StartedAt,
		FinishedAt: attempt.FinishedAt,
	}
	switch value.Observed {
	case agentoperations.NotStarted:
		stage.State, stage.Observed = workexecution.StageSkipped, "The Agent operation ended before execution. No lifecycle changes were started."
	case agentoperations.Absent:
		stage.Observed = "The captured Agent has no remaining managed runtime or credentials."
	case agentoperations.ReadyDesired:
		stage.Observed = "The selected Agent image is installed and its authenticated channel reported Ready at generation " + strconv.FormatUint(
			value.Generation,
			10,
		) + "."
	case agentoperations.ReadyPrevious:
		stage.Observed = "The previous Agent image is preserved or restored; authenticated readiness was verified at generation " + strconv.FormatUint(
			value.Generation,
			10,
		) + "."
	case agentoperations.Unresolved:
		stage.State = workexecution.StageFailed
		stage.Observed = "Readiness or cleanup could not be proved. Lifecycle protection is retained; Retry reconciles the same captured operation without starting another replacement."
	}
	if value.Outcome == workexecution.Succeeded {
		stage.State = workexecution.StageSucceeded
	}
	if value.Failure != nil {
		stage.FailureCode = value.Failure.Code
		stage.Observed += " " + value.Failure.Summary()
	}
	if attempt.Direction == workexecution.Recovery && value.Settled() {
		stage.Name, stage.State, stage.FailureCode = "Agent reconciliation outcome", workexecution.StageSucceeded, ""
		stage.Observed += " Cleanup does not change the original action's failure or cancellation."
	}
	return appendComponentOutcome(result, stage)
}
