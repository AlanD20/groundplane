package taskinspection

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) controllerOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil {
		return nil
	}
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.ControllerSoftware {
		return nil
	}
	capture, err := service.controllerSoftware.Load(ctx, view.Item.Item, result.Revision)
	if err != nil {
		return err
	}
	saved, found, err := service.controllerSoftware.ReadResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	_, proof, err := controllersoftware.PrepareResult(capture, saved, attempt)
	if err != nil {
		return err
	}
	if attempt.Result == nil || *attempt.Result != proof.Evidence {
		return errs.New(errs.KindStateConflict, "Controller activation observation differs from its accepted result")
	}
	value := saved.Record.Result
	stage := workexecution.Stage{Key: "controller_activation_result", Name: "Controller activation outcome",
		Expected: "Qualify the captured release or prove exact predecessor restoration", State: workexecution.StageFailed,
		StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
	if attempt.Direction == workexecution.Recovery {
		stage.Name = "Controller handoff reconciliation outcome"
	}
	if attempt.StartedAt == nil {
		stage.State, stage.Observed = workexecution.StageSkipped, "This attempt ended without starting a Controller activation."
	} else if !value.Settled {
		stage.Observed = "Qualification or predecessor restoration could not be proved. The release claim is retained; inspect the native handoff and Retry captured reconciliation."
	} else if value.Journal == nil {
		stage.Observed = "No native activation journal was published; the existing Controller and Agent were preserved."
	} else {
		switch value.Journal.Phase {
		case upgrade.PhaseHealthy:
			stage.State = workexecution.StageSucceeded
			stage.Observed = "The replacement Controller qualified its running binary, listeners, storage and captured Agent state. The outgoing process did not report its own replacement."
		case upgrade.PhaseRecovered:
			stage.Observed = "The exact previous Controller and Agent state were restored and their readiness was verified. The original activation did not succeed."
		case upgrade.PhaseCancelled:
			stage.Observed = "Activation ended before cutover. The installed Controller and captured Agent predecessor were preserved."
		}
	}
	if value.Failure != nil && stage.State == workexecution.StageFailed {
		stage.FailureCode = value.Failure.Code
		stage.Observed += " " + value.Failure.Summary()
	}
	if attempt.Direction == workexecution.Recovery && value.Settled && attempt.StartedAt != nil {
		stage.State, stage.FailureCode = workexecution.StageSucceeded, ""
		stage.Observed += " Reconciliation does not change the original Task failure or cancellation."
	}
	return appendComponentOutcome(result, stage)
}
