package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) stagingOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil {
		return nil
	}
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.SoftwareStaging {
		return nil
	}
	record, err := service.activations.At(ctx, attempt.TaskID, result.Revision)
	if err != nil {
		return err
	}
	saved, found, err := service.activations.StagingResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	_, proof, err := activation.PrepareStagingResult(record, saved, attempt)
	if err != nil {
		return err
	}
	if attempt.Result == nil || *attempt.Result != proof.Evidence {
		return errs.New(errs.KindStateConflict, "software staging observation differs from its accepted result")
	}
	value := saved.Record.Result
	stage := workexecution.Stage{
		Key:        "software_staging_result",
		Name:       "Software staging outcome",
		Expected:   "Verify and import the selected artifacts without replacing running software",
		State:      workexecution.StageFailed,
		StartedAt:  attempt.StartedAt,
		FinishedAt: attempt.FinishedAt,
	}
	switch {
	case attempt.StartedAt == nil:
		stage.State, stage.Observed = workexecution.StageSkipped, "Staging ended before execution; running software was not changed."
	case !value.Settled:
		stage.Observed = "Staging helper or scratch cleanup could not be proved. Activation is blocked; Retry reconciles only these retained effects."
	case value.Failure != nil:
		stage.Observed = "Staging did not finish every selected artifact. Its helper and scratch are retired; running software was not replaced. Apply the preparation again to start a new activation."
	default:
		stage.State, stage.Observed = workexecution.StageSucceeded, "The selected artifacts are ready for the following activation work."
		if value.Controller != nil {
			stage.Observed += " Controller payload digests were verified and its immutable native release was staged."
		}
		if value.Agent {
			stage.Observed += " The exact prepared Agent image was imported."
		}
	}
	if value.Failure != nil && stage.State == workexecution.StageFailed {
		stage.FailureCode = value.Failure.Code
		stage.Observed += " " + value.Failure.Summary()
	}
	if value.CleanupFailure != nil {
		stage.Observed += " Cleanup: " + value.CleanupFailure.Summary()
	}
	if attempt.Direction == workexecution.Recovery && value.Settled {
		stage.Name, stage.State, stage.FailureCode = "Software staging cleanup outcome", workexecution.StageSucceeded, ""
		stage.Observed += " Successful cleanup does not change the original activation failure."
	}
	return appendComponentOutcome(result, stage)
}
