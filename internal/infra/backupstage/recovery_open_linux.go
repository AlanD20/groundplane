package backupstage

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// OpenRecovery anchors and exclusively owns the configured root, inventories
// crash state, and returns only the pre-Ready recovery surface.
func OpenRecovery(ctx context.Context, config Config) (*RecoverySession, error) {
	return openRecoveryWithOperations(ctx, config, defaultLinuxOperations())
}

func openRecoveryWithOperations(
	ctx context.Context,
	config Config,
	ops linuxOperations,
) (*RecoverySession, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ops.openat2 == nil || ops.renameat2 == nil || ops.flock == nil || ops.fallocate == nil || ops.fstatfs == nil ||
		ops.pwrite == nil || ops.fdatasync == nil || ops.write == nil || ops.fsync == nil || ops.cleanupTimeout <= 0 {
		return nil, internalError("linux staging operations are required")
	}
	root, err := validateRootPath(config.Root)
	if err != nil {
		return nil, err
	}
	rootFD, err := openConfiguredRoot(ctx, root, ops)
	if err != nil {
		return nil, err
	}
	if err := probeRenameat2(ctx, ops); err != nil {
		return nil, closeWithPrimary(ctx, err, rootFD)
	}
	ownerFD, err := openOwnerMarker(ctx, rootFD, ops)
	if err != nil {
		return nil, closeWithPrimary(ctx, err, rootFD)
	}
	if err := ops.flock(ownerFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("acquire staging Agent ownership", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("another Agent owns the staging root")
		}
		return nil, closeWithPrimary(ctx, primary, ownerFD, rootFD)
	}
	stager := &Stager{rootFD: rootFD, ownerFD: ownerFD, leases: make(map[IDs]*Stage), ops: ops}
	if err := lockWithContext(ctx, rootFD, ops); err != nil {
		return nil, closeStagerWithPrimary(ctx, stager, err)
	}
	reservationRootFD, err := openOrCreateDirectory(ctx, rootFD, reservationDirName, ops)
	if err != nil {
		return nil, closeStagerWithPrimary(ctx, stager,
			unlockWithPrimary(ctx, err, rootFD, ops))
	}
	if err := probeGrowthMarker(ctx, reservationRootFD, ops); err != nil {
		primary := closeWithPrimary(ctx, err, reservationRootFD)
		return nil, closeStagerWithPrimary(ctx, stager,
			unlockWithPrimary(ctx, primary, rootFD, ops))
	}
	session := &RecoverySession{
		stager: stager, reservationRootFD: reservationRootFD,
		recovered: make(map[RecoveryID]*recoveredStage), rootLocked: true,
	}
	if err := session.inventoryLocked(ctx); err != nil {
		return nil, closeRecoveryWithPrimary(ctx, session, err)
	}
	return session, nil
}

// Close releases the lifetime Agent-owner lease and root descriptor only after
// every Stage has released its lease and capacity reservation.
func (stager *Stager) Close(ctx context.Context) error {
	callerErr := contextError(ctx)
	if stager == nil {
		return callerErr
	}
	stager.mu.Lock()
	defer stager.mu.Unlock()
	if stager.rootFD < 0 {
		return callerErr
	}
	if len(stager.leases) != 0 {
		return stateConflictError("stager still has live stages")
	}
	rootFD, ownerFD := stager.rootFD, stager.ownerFD
	stager.rootFD, stager.ownerFD = -1, -1
	unlockErr := rawOperationError("unlock staging Agent ownership", stager.ops.flock(ownerFD, unix.LOCK_UN))
	closeErr := closeFDs(ctx, ownerFD, rootFD)
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, callerErr, unlockErr, closeErr)
	}
	return joinPrivate(callerErr, closeErr)
}

func closeStagerWithPrimary(ctx context.Context, stager *Stager, primary error) error {
	if stager == nil {
		return primary
	}
	unlockErr := rawOperationError("unlock staging Agent ownership",
		stager.ops.flock(stager.ownerFD, unix.LOCK_UN))
	closeErr := closeFDs(ctx, stager.ownerFD, stager.rootFD)
	stager.ownerFD, stager.rootFD = -1, -1
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, primary, unlockErr, closeErr)
	}
	return joinPrivate(primary, closeErr)
}

func closeRecoveryWithPrimary(ctx context.Context, session *RecoverySession, primary error) error {
	return joinPrivate(primary, closeRecoveryLocked(ctx, session))
}

func closeRecoveryLocked(ctx context.Context, session *RecoverySession) error {
	if session == nil || session.stager == nil {
		return nil
	}
	var causes []error
	for _, recovered := range session.recovered {
		for name, file := range recovered.files {
			if err := file.Close(); err != nil {
				causes = append(causes, systemError("close retained recovery artifact "+name, err))
			}
			delete(recovered.files, name)
		}
		if err := closeFDs(ctx, recovered.reservationFD, recovered.reservationStepFD,
			recovered.reservationTaskFD, recovered.reservationRootFD,
			recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD); err != nil {
			causes = append(causes, err)
		}
	}
	for _, prepared := range session.prepared {
		stage := prepared.Stage
		stage.mu.Lock()
		if err := stage.closeArtifactsLocked(ctx); err != nil {
			causes = append(causes, err)
		}
		if err := stage.releaseGrowthMarker(ctx); err != nil {
			causes = append(causes, err)
		}
		if err := closeFDs(ctx, stage.reservationFD, stage.reservationStepFD,
			stage.reservationTaskFD, stage.reservationRootFD,
			stage.pointFD, stage.stepFD, stage.taskFD, stage.rootFD); err != nil {
			causes = append(causes, err)
		}
		stage.closed = true
		stage.mu.Unlock()
	}
	if session.rootLocked {
		if err := session.stager.ops.flock(session.stager.rootFD, unix.LOCK_UN); err != nil {
			causes = append(causes, rawOperationError("unlock startup recovery root", err))
		}
		session.rootLocked = false
	}
	if err := closeFDs(ctx, session.reservationRootFD); err != nil {
		causes = append(causes, err)
	}
	session.reservationRootFD = -1
	return joinPrivate(wrapPrivateCauses("close recovery descriptors", causes...),
		closeStagerWithPrimary(ctx, session.stager, nil))
}
