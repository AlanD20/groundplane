//go:build linux

package backupstage

import "context"

// Suspend abandons execution descriptors, not durable stage contents. Only a
// later explicit Controller disposition may remove the retained staged files and
// reservation. Close/Cleanup remain destructive completion operations.
func (stage *Stage) Suspend(ctx context.Context) error {
	if stage == nil {
		return contextError(ctx)
	}
	stage.mu.Lock()
	if stage.closed {
		stage.mu.Unlock()
		return contextError(ctx)
	}
	artifactsErr := stage.closeArtifactsLocked(ctx)
	growthErr := stage.releaseGrowthMarker(ctx)
	reservationErr := closeFDs(
		ctx,
		stage.reservationFD,
		stage.reservationStepFD,
		stage.reservationTaskFD,
		stage.reservationRootFD,
	)
	stage.reservationFD, stage.reservationStepFD, stage.reservationTaskFD, stage.reservationRootFD = -1, -1, -1, -1
	descriptorErr := stage.closeDescriptors(ctx)
	stage.closed = true
	stage.mu.Unlock()
	stage.releaseLeaseMap()
	return joinPrivate(contextError(ctx), artifactsErr, growthErr, reservationErr, descriptorErr)
}
