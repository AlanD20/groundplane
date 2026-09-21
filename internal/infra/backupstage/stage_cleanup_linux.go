package backupstage

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func (artifact *Artifact) Abort(ctx context.Context) error {
	if artifact == nil || artifact.stage == nil {
		return joinPrivate(contextError(ctx), internalError("artifact is required"))
	}
	return artifact.stage.Cleanup(ctx)
}

// Cleanup validates and removes only safe point contents and empty ancestors.
func (stage *Stage) Cleanup(ctx context.Context) error {
	callerErr := contextError(ctx)
	if stage == nil {
		return callerErr
	}
	stage.mu.Lock()
	if stage.cleaned {
		stage.mu.Unlock()
		return callerErr
	}
	if stage.closed {
		stage.mu.Unlock()
		return joinPrivate(callerErr, internalError("stage is closed"))
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), stage.owner.ops.cleanupTimeout)
	defer cancel()
	artifactErr := stage.removeUnpublishedArtifactsLocked(cleanupCtx)
	removeErr := removePointContents(cleanupCtx, stage.pointFD, stage.owner.ops)
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.stepFD, stage.ids.Point); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			removeErr = systemError("remove point directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.stepFD)
	}
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.taskFD, stage.ids.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			removeErr = systemError("remove empty step directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.taskFD)
	}
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.rootFD, stage.ids.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			removeErr = systemError("remove empty task directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.rootFD)
	}
	artifactCloseErr := stage.closeArtifactsLocked(cleanupCtx)
	if stage.owner.ops.beforeReservationRelease != nil {
		stage.owner.ops.beforeReservationRelease()
	}
	reservationErr := stage.releaseReservation(cleanupCtx)
	closeErr := stage.closeDescriptors(cleanupCtx)
	stage.cleaned, stage.closed = true, true
	stage.mu.Unlock()
	stage.releaseLeaseMap()
	return joinPrivate(callerErr, artifactErr, removeErr, artifactCloseErr, reservationErr, closeErr)
}

func (artifact *Artifact) removeUnpublishedLocked(ctx context.Context) error {
	var closeErr error
	if artifact.file != nil {
		if err := artifact.file.Close(); err != nil {
			closeErr = systemError("close unpublished artifact", err)
		}
		artifact.file = nil
	}
	if artifact.state == artifactOpen {
		artifact.state = artifactPartial
	}
	name := artifact.temporary
	if artifact.state == artifactLinked {
		name = artifact.final
	}
	var unlinkErr error
	if name != "" {
		if err := unix.Unlinkat(artifact.stage.pointFD, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			unlinkErr = systemError("remove unpublished artifact", err)
		}
	}
	artifact.state = artifactRemoved
	delete(artifact.stage.artifacts, artifact)
	return joinPrivate(closeErr, unlinkErr)
}

func (artifact *Artifact) failWriteLocked(_ context.Context, primary error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), artifact.stage.owner.ops.cleanupTimeout)
	defer cancel()
	cleanupErr := artifact.removeUnpublishedLocked(cleanupCtx)
	syncErr := syncFD(cleanupCtx, artifact.stage.pointFD)
	growthErr := artifact.sealGrowthLocked(cleanupCtx)
	return joinPrivate(primary, cleanupErr, syncErr, growthErr)
}

func (artifact *Artifact) sealGrowthLocked(ctx context.Context) error {
	if !artifact.growing {
		return nil
	}
	artifact.growing = false
	if artifact.stage.growingFiles == 0 {
		return internalError("exclusive growth file accounting underflow")
	}
	artifact.stage.growingFiles--
	if artifact.stage.growingFiles != 0 {
		return nil
	}
	return artifact.stage.releaseGrowthMarker(ctx)
}

func (stage *Stage) closeArtifactsLocked(ctx context.Context) error {
	var causes []error
	for artifact := range stage.artifacts {
		for reader := range artifact.readers {
			if err := reader.closeLocked(ctx); err != nil {
				causes = append(causes, err)
			}
		}
		if artifact.file == nil {
			continue
		}
		if err := artifact.file.Close(); err != nil {
			causes = append(causes, err)
		}
		artifact.file = nil
		if artifact.state == artifactOpen {
			artifact.state = artifactPartial
		}
	}
	return wrapPrivateCauses("close staged artifact descriptors", causes...)
}

func (stage *Stage) removeUnpublishedArtifactsLocked(ctx context.Context) error {
	var result error
	for artifact := range stage.artifacts {
		if artifact.state != artifactPublished && artifact.state != artifactRemoved {
			result = joinPrivate(result, artifact.removeUnpublishedLocked(ctx))
		}
	}
	return result
}

func (stage *Stage) releaseLeaseMap() {
	if stage.owner == nil {
		return
	}
	stage.owner.mu.Lock()
	defer stage.owner.mu.Unlock()
	if stage.owner.leases[stage.ids] == stage {
		delete(stage.owner.leases, stage.ids)
	}
}

func removeDirectoryEntry(ctx context.Context, parentFD int, name string) error {
	return unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR)
}

func (stage *Stage) syncParents(ctx context.Context) error {
	for _, fd := range []int{stage.pointFD, stage.stepFD, stage.taskFD, stage.rootFD} {
		if err := syncFD(ctx, fd); err != nil {
			return err
		}
	}
	return nil
}

func syncFD(ctx context.Context, fd int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fsync(fd); err != nil {
		return storageSystemError("sync staging directory", err)
	}
	return nil
}

func (stage *Stage) closeDescriptors(ctx context.Context) error {
	var causes []error
	for _, fd := range []*int{&stage.pointFD, &stage.stepFD, &stage.taskFD, &stage.rootFD} {
		if *fd >= 0 {
			if err := unix.Close(*fd); err != nil {
				causes = append(causes, err)
			}
			*fd = -1
		}
	}
	return wrapPrivateCauses("close staging descriptors", causes...)
}

func closeFDs(ctx context.Context, fds ...int) error {
	var causes []error
	for _, fd := range fds {
		if fd >= 0 {
			if err := unix.Close(fd); err != nil {
				causes = append(causes, err)
			}
		}
	}
	return wrapPrivateCauses("close staging descriptors", causes...)
}

func closeWithPrimary(ctx context.Context, primary error, fds ...int) error {
	var cleanup []error
	for _, fd := range fds {
		if fd >= 0 {
			if err := unix.Close(fd); err != nil {
				cleanup = append(cleanup, err)
			}
		}
	}
	if len(cleanup) == 0 {
		return primary
	}
	kind := errs.KindInternal
	if value, ok := errs.KindOf(primary); ok {
		kind = value
	}
	return errs.WrapJoined(kind, primary, cleanup...)
}
