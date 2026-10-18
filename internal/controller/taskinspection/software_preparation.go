package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) preparationOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil {
		return nil
	}
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.SoftwarePreparation {
		return nil
	}
	snapshot, err := service.preparations.LoadWork(ctx, view.Item.Item, result.Revision)
	if err != nil {
		return err
	}
	saved, found, err := service.preparations.ReadOutcome(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	_, proof, err := preparationrecord.PrepareResult(snapshot, saved, attempt)
	if err != nil {
		return err
	}
	if attempt.Result == nil || *attempt.Result != proof.Evidence {
		return errs.New(errs.KindStateConflict, "software preparation evidence differs from its finished attempt")
	}
	stage := workexecution.Stage{Key: "software_preparation_result", Name: "Software preparation outcome",
		Expected: "Verify selected registry outputs and retire this operation's temporary builder resources",
		State:    workexecution.StageFailed, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
	if attempt.StartedAt == nil {
		stage.State, stage.Observed = workexecution.StageSkipped, "This attempt did not start a build or publish artifacts."
	} else if attempt.Direction == workexecution.Recovery {
		stage.Name = "Clean up interrupted software build"
		if saved.Record.Cleaned {
			stage.State = workexecution.StageSucceeded
			stage.Observed = "The captured build client, builder, temporary tags and workspace were removed. Published registry artifacts were retained; the original failure is unchanged."
		} else {
			stage.Observed = "Cleanup could not be proved. The build claim remains protected; inspect the host and Retry captured cleanup."
		}
	} else if snapshot.Record.Record.Progress.Phase == preparation.PhaseVerified {
		stage.State = workexecution.StageSucceeded
		stage.Observed = "All selected artifacts were published and verified in GP's registry; temporary build resources were removed. Nothing was activated."
	} else {
		progress := snapshot.Record.Record.Progress
		stage.FailureCode, stage.Observed = progress.ErrorCode, progress.ErrorDetail
		if saved.Record.Cleaned {
			stage.Observed += " Temporary build resources were removed. Submit a new preparation to build again."
		} else {
			stage.Observed += " Cleanup remains unresolved; inspect the host and Retry captured cleanup."
		}
	}
	return appendComponentOutcome(result, stage)
}
