package composehelper

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ExecuteVolumeWork(ctx context.Context, taskRunner runner.Runner, volumeRoot string,
	request *agentpb.VolumeWorkHelperRequest,
) (*agentpb.VolumeWorkHelperResponse, error) {
	if ctx == nil || taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Volume helper dependencies are missing")
	}
	input, err := executionplan.OpenVolumeHelperRequest(request)
	if err != nil {
		return nil, err
	}
	scope, err := environmentpath.Parse(volumeRoot, input.Procedure.Candidate.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
		return nil, errs.New(errs.KindValidationFailed, "Volume helper directory belongs to another owner")
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	if !input.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	if !input.InspectOnly {
		switch input.Progress.Stage {
		case volumeexecution.PrepareDirectory:
			at, err := ids.Timestamp(ids.KindWorkItem, input.Attempt.WorkItemID)
			if err != nil {
				return nil, err
			}
			operationID := ids.DeriveAt(ids.KindOperation, at, input.Attempt.WorkItemID, "volume-directory")
			if err := (environmentdirectoryhelper.Creator{}).EnsureManagedVolumes(bounded,
				environmentdirectoryhelper.ManagedVolumeEnsureRequest{
					TaskID: input.Attempt.TaskID, OperationID: operationID, IntentSHA256: input.Procedure.IntentSha256,
					VolumeRoot: volumeRoot, VolumeDir: input.Procedure.Candidate.AuthorizedVolumeDir,
					Volumes: []environmentdirectoryhelper.ManagedVolume{{ID: input.Procedure.VolumeId, Key: input.Procedure.ComposeKey}},
				}); err != nil {
				return nil, err
			}
		case volumeexecution.PrepareVolume, volumeexecution.VerifyVolume:
			// Do not create a Docker bind pointing at an absent or unfinished
			// directory. The prior receipt is not a replacement for this check.
			directory, err := (environmentdirectoryhelper.Creator{}).InspectManagedVolume(bounded,
				environmentdirectoryhelper.ManagedVolumeInspectRequest{
					VolumeRoot: volumeRoot, VolumeDir: input.Procedure.Candidate.AuthorizedVolumeDir,
					VolumeID: input.Procedure.VolumeId, ComposeKey: input.Procedure.ComposeKey,
				})
			if err != nil {
				return nil, err
			}
			if !directory.Ready {
				return nil, errs.New(errs.KindStateConflict, "Volume directory is not ready for its Docker bind")
			}
			if err := ApplyVolumeDocker(bounded, taskRunner, input.Procedure); err != nil {
				return nil, err
			}
		case volumeexecution.RemoveVolume:
			if err := ApplyVolumeDocker(bounded, taskRunner, input.Procedure); err != nil {
				return nil, err
			}
		case volumeexecution.DetachConsumers:
			if err := detachVolumeConsumers(bounded, taskRunner, input.Procedure); err != nil {
				return nil, err
			}
		case volumeexecution.RemoveDirectory:
			return executeVolumeDirectoryBatch(bounded, taskRunner, volumeRoot, input)
		default:
			return nil, errs.New(errs.KindValidationFailed, "Volume helper stage is invalid")
		}
	}
	observation, err := ObserveVolumeWork(bounded, taskRunner, volumeRoot, input.Attempt, input.Procedure)
	if err != nil {
		return nil, err
	}
	response, err := volumeObservationResponse(observation, input.InspectOnly)
	if err != nil {
		return nil, err
	}
	if err := executionplan.ValidateVolumeHelperResponse(input, response); err != nil {
		return nil, err
	}
	return response, nil
}

func volumeObservationResponse(
	observation volumeexecution.Observation,
	inspection bool,
) (*agentpb.VolumeWorkHelperResponse, error) {
	digest, err := hex.DecodeString(observation.Input.SHA256)
	if err != nil {
		return nil, err
	}
	response := &agentpb.VolumeWorkHelperResponse{
		VolumeId: observation.VolumeID, Input: &agentpb.WorkInputReference{InputId: observation.Input.ID, Sha256: digest},
		DirectoryReady: observation.DirectoryReady, VolumeReady: observation.VolumeReady,
		ConsumersDetached: observation.ConsumersDetached, VolumeAbsent: observation.VolumeAbsent,
		DirectoryAbsent: observation.DirectoryAbsent, ObservedAt: timestamppb.New(observation.At), Inspection: inspection,
	}
	return response, nil
}
