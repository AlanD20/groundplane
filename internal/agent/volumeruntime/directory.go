package volumeruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (host *RuntimeHost) removeDirectory(
	ctx context.Context,
	input Input,
	request *agentpb.VolumeWorkHelperRequest,
) (volumeexecution.Observation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return volumeexecution.Observation{}, err
		}
		if err := host.ownership.Check(); err != nil {
			return volumeexecution.Observation{}, err
		}
		saved, fresh, err := host.journal.PrepareVolumeDirectoryBatch(
			ctx,
			input.attempt,
			input.procedure.VolumeId,
			time.Now().UTC(),
		)
		if err != nil {
			return volumeexecution.Observation{}, err
		}
		if !fresh {
			// A missing batch result is not permission to issue it again. Stop
			// the exact helper and inspect; only proved absence can complete.
			return host.Inspect(ctx, input, volumeexecution.RemoveDirectory)
		}
		request.DirectoryBatch, err = volumeexecution.EncodeDirectoryBatch(input.attempt, saved.Boundary, saved.Batch)
		if err != nil {
			return volumeexecution.Observation{}, err
		}
		captured, err := executionplan.OpenVolumeHelperRequest(request)
		if err != nil {
			return volumeexecution.Observation{}, err
		}
		response, err := host.helper.ExecuteVolumeWork(ctx, host.root, request)
		if err != nil {
			return volumeexecution.Observation{}, err
		}
		if err := host.ownership.Check(); err != nil {
			return volumeexecution.Observation{}, err
		}
		if err := executionplan.ValidateVolumeHelperResponse(captured, response); err != nil {
			return volumeexecution.Observation{}, err
		}
		result, err := volumeexecution.DecodeDirectoryResult(
			input.attempt,
			saved.Boundary,
			saved.Batch,
			response.DirectoryResult,
		)
		if err != nil {
			return volumeexecution.Observation{}, err
		}
		if err := host.journal.SaveVolumeDirectoryResult(ctx, input.attempt, input.procedure.VolumeId, saved.Batch, result); err != nil {
			return volumeexecution.Observation{}, err
		}
		if result.Absent {
			return executionplan.ReadVolumeHelperObservation(captured, response)
		}
	}
}
