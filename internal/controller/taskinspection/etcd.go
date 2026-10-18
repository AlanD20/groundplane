package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Etcd observations describe the actual activation receipt, without publishing
// configuration contents or pretending to know separate restart/probe timings.
func (service *Service) etcdOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	work, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if work.Item.Item.Operation != workexecution.EtcdConfiguration {
		return nil
	}
	input, err := service.etcd.Load(ctx, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if err := input.Record.Match(work.Item.Item); err != nil {
		return err
	}
	stage := workexecution.Stage{Key: "etcd_activation", Name: "Apply saved etcd configuration",
		Expected: "Activate saved revision " + input.Record.SelectedRevision + "; verify etcd container configuration and readiness",
		State:    workexecution.StageWaiting}
	if attempt.Direction == workexecution.Recovery {
		stage.Key, stage.Name = "etcd_restoration", "Reconcile interrupted etcd activation"
		stage.Expected = "Verify the captured activation receipt or restore its exact predecessor; do not repeat forward Apply"
	}
	if attempt.StartedAt != nil {
		stage.State, stage.StartedAt = workexecution.StageRunning, attempt.StartedAt
		stage.Observed = "The Controller is checking the local activation receipt and etcd runtime. Application containers are not restarted."
	}
	saved, found, err := service.etcd.ReadResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	if attempt.FinishedAt != nil {
		if !found {
			return nil
		} // Missing evidence is not replaced with a guessed outcome.
		stage.FinishedAt = attempt.FinishedAt
		if attempt.StartedAt == nil {
			stage.State = workexecution.StageSkipped
			stage.Observed = "Execution stopped before this attempt activated configuration."
		} else {
			stage.State = workexecution.StageFailed
			switch saved.Record.Observed {
			case etcdactivation.Applied:
				stage.State = workexecution.StageSucceeded
				stage.Observed = "The selected configuration is active; the owned etcd container is running with matching settings and passes its readiness check."
				if saved.Record.Failed {
					stage.State = workexecution.StageFailed
					stage.Observed += " Apply reported a failure before result settlement."
				}
			case etcdactivation.Restored:
				stage.Observed = "Apply did not complete. The exact previous configuration and healthy etcd runtime were restored. Submit a new Apply to try another configuration."
				if attempt.Direction == workexecution.Recovery {
					stage.State = workexecution.StageSucceeded
				}
			case etcdactivation.NotStarted:
				stage.Observed = "The local receipt proves configuration activation never started. Existing etcd configuration was preserved."
			case etcdactivation.Unresolved:
				stage.Observed = "The Controller could not prove the active configuration and healthy etcd runtime. The configuration claim is retained; inspect etcd and use Retry for captured cleanup only."
			}
		}
	}
	if err := stage.Validate(); err != nil {
		return err
	}
	result.Items = append(result.Items, stage)
	result.Available = true
	if stage.FinishedAt != nil {
		result.UpdatedAt = stage.FinishedAt
	} else if stage.StartedAt != nil {
		result.UpdatedAt = stage.StartedAt
	}
	return nil
}
