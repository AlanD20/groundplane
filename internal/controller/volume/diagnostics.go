package volume

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Observations follow committed checkpoints; diagnostic failure never changes
// the acknowledgement or grants permission to repeat a storage operation.
func (service *WorkReports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.VolumeWorkInput, progress volumeexecution.Progress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	snapshot, err := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err == nil && (snapshot.Expired || snapshot.Record != nil && snapshot.Record.Sequence >= progress.Sequence) {
		return
	}
	if err == nil {
		stage := volumeDiagnosticStage(volumeCheckpointStage(input.Action, progress.Sequence))
		if progress.InFlight {
			stage.StartedAt = progress.BeganAt
			stage.State = workexecution.StageRunning
			stage.Observed = "The Agent is applying or inspecting this storage action"
		} else if snapshot.Record == nil || snapshot.Record.Sequence != progress.Sequence-1 ||
			snapshot.Record.Stages[len(snapshot.Record.Stages)-1].Key != stage.Key {
			err = errs.New(errs.KindStateConflict, "Volume stage start observation is unavailable")
		} else {
			stage = snapshot.Record.Stages[len(snapshot.Record.Stages)-1]
			stage.State, stage.FinishedAt = workexecution.StageSucceeded, &progress.Observation.At
			stage.Observed = "Ownership-checked host inspection confirms the required storage state"
		}
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, progress.Sequence, stage, progress.UpdatedAt)
		}
	}
	if err != nil {
		slog.WarnContext(bounded, "Volume diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if markErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); markErr != nil {
			slog.WarnContext(
				bounded,
				"could not record Volume diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				markErr,
			)
		}
	}
}

func volumeCheckpointStage(action agentpb.VolumeWorkAction, sequence uint64) volumeexecution.Stage {
	var stages []volumeexecution.Stage
	switch action {
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE:
		stages = []volumeexecution.Stage{volumeexecution.PrepareDirectory, volumeexecution.PrepareVolume}
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY:
		stages = []volumeexecution.Stage{volumeexecution.VerifyVolume}
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE:
		stages = []volumeexecution.Stage{
			volumeexecution.DetachConsumers,
			volumeexecution.RemoveVolume,
			volumeexecution.RemoveDirectory,
		}
	}
	if sequence == 0 || (sequence-1)/2 >= uint64(len(stages)) {
		return ""
	}
	return stages[(sequence-1)/2]
}

func volumeDiagnosticStage(stage volumeexecution.Stage) workexecution.Stage {
	result := workexecution.Stage{Key: string(stage)}
	switch stage {
	case volumeexecution.PrepareDirectory:
		result.Name, result.Expected = "Prepare managed directory", "The owned Volume directory exists with its captured permissions"
	case volumeexecution.PrepareVolume:
		result.Name, result.Expected = "Create Docker Volume", "The Docker Volume uses the captured owned directory"
	case volumeexecution.VerifyVolume:
		result.Name, result.Expected = "Verify Volume storage", "The owned directory and Docker Volume match the captured definition"
	case volumeexecution.DetachConsumers:
		result.Name, result.Expected = "Detach Volume consumers", "No captured container remains attached to this Volume"
	case volumeexecution.RemoveVolume:
		result.Name, result.Expected = "Remove Docker Volume", "Consumers are detached and the Docker Volume is absent"
	case volumeexecution.RemoveDirectory:
		result.Name, result.Expected = "Remove managed directory", "The Docker Volume and its owned directory are absent"
	}
	return result
}
