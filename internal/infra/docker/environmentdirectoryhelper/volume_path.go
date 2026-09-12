package environmentdirectoryhelper

import (
	"bytes"
	"context"
	"time"

	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateVolumePathRequest(request *agentpb.EnvironmentDirectoryHelperRequest, step *agentpb.ExecutionStep) error {
	payload := step.GetManagedVolumeDirectoryRemove()
	if payload == nil {
		if len(request.VolumeRemovalPendingPath) != 0 {
			return errs.New(
				errs.KindValidationFailed,
				"Volume path intent cannot authorize another directory operation",
			)
		}
		return nil
	}
	pending, err := removal.DecodePendingPath(request.VolumeRemovalPendingPath)
	if err != nil {
		return err
	}
	if pending.OperationID != request.OperationId || pending.VolumeID != payload.VolumeId ||
		pending.Key != payload.ComposeKey ||
		!bytes.Equal(pending.IntentSHA256[:], payload.IntentSha256) ||
		len(pending.ComponentStack) != 0 ||
		request.TimeoutSeconds > 30 {
		return errs.New(errs.KindValidationFailed, "Volume path intent does not match its immutable plan")
	}
	return nil
}

func executeVolumePath(ctx context.Context, remover ManagedVolumeDirectoryRemover,
	request *agentpb.EnvironmentDirectoryHelperRequest, payload *agentpb.ManagedVolumeDirectoryRemove,
	volumeRoot, volumeDir string,
) (ManagedVolumeDirectoryRemoveResult, error) {
	pending, err := removal.DecodePendingPath(request.VolumeRemovalPendingPath)
	if err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, err
	}
	result, err := remover.RemoveManagedVolume(ctx, ManagedVolumeDirectoryRemoveRequest{
		TaskID: request.TaskId, OperationID: request.OperationId, IntentSHA256: pending.IntentSHA256[:], Cursor: pending.Cursor,
		VolumeRoot: volumeRoot, VolumeDir: volumeDir, VolumeID: payload.VolumeId, ComposeKey: payload.ComposeKey,
	})
	if err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, err
	}
	completion := removal.Completion{
		OperationID: pending.OperationID, RequestOrdinal: pending.RequestOrdinal, RequestSHA256: pending.RequestSHA256,
		NextCursor: result.NextCursor, MutationCount: result.MutationCount, DirectoryAbsent: result.Complete, CompletedAt: time.Now().UTC(),
	}
	completion.ResponseSHA256, completion.ResponseBytes = removal.PathResponseDigest(
		completion,
	), removal.PathResponseBytes(
		completion,
	)
	result.Completion, err = removal.EncodeCompletion(completion)
	return result, err
}

func validateVolumePathResponse(response *agentpb.EnvironmentDirectoryHelperResponse) error {
	if len(response.VolumeRemovalCompletion) == 0 {
		return nil
	}
	completion, err := removal.DecodeCompletion(response.VolumeRemovalCompletion)
	if err != nil {
		return err
	}
	if response.ExitCode != 0 || response.FailedStepId != "" || completion.DirectoryAbsent != response.Complete ||
		completion.MutationCount != response.MutationCount || !bytes.Equal(completion.NextCursor, response.NextCursor) {
		return errs.New(errs.KindValidationFailed, "Volume path completion does not match its helper result")
	}
	return nil
}
