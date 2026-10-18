package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeVolumeDirectoryBatch(ctx context.Context, taskRunner runner.Runner, root string, input executionplan.VolumeHelperInput) (*agentpb.VolumeWorkHelperResponse, error) {
	if input.DirectoryBatch == nil || input.InspectOnly {
		return nil, errs.New(errs.KindValidationFailed, "Volume directory deletion requires its saved batch")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	docker, err := ObserveVolumeDocker(bounded, taskRunner, input.Procedure)
	if err != nil {
		return nil, err
	}
	if !docker.Absent || !docker.ConsumersDetached {
		return nil, errs.New(errs.KindResourceInUse, "Volume directory deletion requires all containers detached and the Docker Volume removed")
	}
	at, err := ids.Timestamp(ids.KindWorkItem, input.Attempt.WorkItemID)
	if err != nil {
		return nil, err
	}
	removed, err := (environmentdirectoryhelper.Creator{}).RemoveManagedVolume(bounded,
		environmentdirectoryhelper.ManagedVolumeDirectoryRemoveRequest{
			TaskID: input.Attempt.TaskID, OperationID: ids.DeriveAt(ids.KindOperation, at, input.Attempt.WorkItemID, "volume-directory"),
			IntentSHA256: input.Procedure.IntentSha256, Cursor: input.DirectoryBatch.Cursor,
			VolumeRoot: root, VolumeDir: input.Procedure.Candidate.AuthorizedVolumeDir,
			VolumeID: input.Procedure.VolumeId, ComposeKey: input.Procedure.ComposeKey,
		})
	if err != nil {
		return nil, err
	}
	observation, err := ObserveVolumeWork(bounded, taskRunner, root, input.Attempt, input.Procedure)
	if err != nil {
		return nil, err
	}
	if observation.DirectoryAbsent != removed.Complete {
		return nil, errs.New(errs.KindStateConflict, "Volume directory changed after its deletion batch")
	}
	raw, err := volumeexecution.EncodeDirectoryBatch(input.Attempt, input.Progress, *input.DirectoryBatch)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	result := volumeexecution.DirectoryResult{RequestSHA256: hex.EncodeToString(digest[:]),
		NextCursor: removed.NextCursor, Mutations: removed.MutationCount, Absent: removed.Complete, ObservedAt: observation.At}
	response, err := volumeObservationResponse(observation, false)
	if err != nil {
		return nil, err
	}
	response.DirectoryResult, err = volumeexecution.EncodeDirectoryResult(input.Attempt, input.Progress, *input.DirectoryBatch, result)
	if err != nil {
		return nil, err
	}
	return response, executionplan.ValidateVolumeHelperResponse(input, response)
}
