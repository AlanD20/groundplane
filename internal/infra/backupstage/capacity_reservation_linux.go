package backupstage

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func (stage *Stage) reserveCapacityLocked(ctx context.Context) error {
	dirFD, err := openOrCreateDirectory(ctx, stage.rootFD, reservationDirName, stage.owner.ops)
	if err != nil {
		return err
	}
	growthFD, err := openGrowthMarker(ctx, dirFD, stage.owner.ops, false)
	if err != nil {
		return closeWithPrimary(ctx, err, dirFD)
	}
	growthLock := unix.LOCK_SH
	if stage.capacity.kind == capacityExclusiveUnknown {
		growthLock = unix.LOCK_EX
	}
	if err := stage.owner.ops.flock(growthFD, growthLock|unix.LOCK_NB); err != nil {
		primary := systemError("acquire staging growth admission", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("staging filesystem has an incompatible live growth admission")
		}
		return closeWithPrimary(ctx, primary, growthFD, dirFD)
	}
	reserved, err := sweepReservations(ctx, stage.rootFD, dirFD, stage.owner.ops, false)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, dirFD)
	}
	required := stage.capacity.requiredBytes
	if stage.capacity.kind == capacityBounded {
		available, err := availableBytes(ctx, stage.rootFD, stage.owner.ops)
		if err != nil {
			return closeWithPrimary(ctx, err, growthFD, dirFD)
		}
		wanted, overflow := addUint64(reserved, required)
		if overflow {
			return closeWithPrimary(ctx, internalError("capacity reservation accounting overflow"), growthFD, dirFD)
		}
		if wanted > available {
			return closeWithPrimary(ctx, errs.New(errs.KindStorageUnavailable,
				"backup stage has insufficient free space"), growthFD, dirFD)
		}
	} else if reserved != 0 {
		return closeWithPrimary(ctx,
			stateConflictError("bounded reservations exist during exclusive growth admission"), growthFD, dirFD)
	}
	taskFD, err := openOrCreateDirectory(ctx, dirFD, stage.ids.Task, stage.owner.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, dirFD)
	}
	stepFD, err := openOrCreateDirectory(ctx, taskFD, stage.ids.Step, stage.owner.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, taskFD, dirFD)
	}
	fd, err := stage.owner.ops.openat2(stepFD, stage.ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:  uint64(fileMode), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return closeWithPrimary(ctx, storageSystemError("create capacity reservation", err),
			growthFD, stepFD, taskFD, dirFD)
	}
	fail := func(primary error) error {
		unlinkErr := unix.Unlinkat(stepFD, stage.ids.Point, 0)
		if errors.Is(unlinkErr, unix.ENOENT) {
			unlinkErr = nil
		}
		syncErr := unix.Fsync(stepFD)
		return closeWithPrimary(ctx, joinPrivate(primary,
			rawOperationError("remove failed capacity reservation", unlinkErr),
			storageOperationError("sync failed reservation removal", syncErr)),
			fd, growthFD, stepFD, taskFD, dirFD)
	}
	if err := unix.Fchown(fd, 0, 0); err != nil {
		return fail(systemError("set capacity reservation ownership", err))
	}
	if err := unix.Fchmod(fd, fileMode); err != nil {
		return fail(systemError("set capacity reservation mode", err))
	}
	if err := stage.owner.ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(systemError("lock capacity reservation", err))
	}
	if err := writeReservation(ctx, fd, required, stage.owner.ops); err != nil {
		return fail(err)
	}
	if err := unix.Fsync(fd); err != nil {
		return fail(storageSystemError("sync capacity reservation", err))
	}
	if err := unix.Fsync(stepFD); err != nil {
		return fail(storageSystemError("sync capacity reservation directory", err))
	}
	stage.reservationRootFD, stage.reservationTaskFD = dirFD, taskFD
	stage.reservationStepFD, stage.reservationFD = stepFD, fd
	stage.growthFD = growthFD
	stage.reservationRemaining = required
	return nil
}

func (stage *Stage) consumeReservation(ctx context.Context, consumed uint64) error {
	if consumed == 0 || stage.reservationFD < 0 {
		return nil
	}
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	if err := lockWithContext(ctx, stage.rootFD, stage.owner.ops); err != nil {
		return err
	}
	remaining := stage.reservationRemaining - consumed
	if err := appendReservationConsumption(ctx, stage.reservationFD, consumed, stage.owner.ops); err != nil {
		stage.poisoned = true
		return unlockWithPrimary(ctx, err, stage.rootFD, stage.owner.ops)
	}
	stage.reservationRemaining = remaining
	return unlockWithPrimary(ctx, nil, stage.rootFD, stage.owner.ops)
}

func (stage *Stage) releaseReservation(ctx context.Context) error {
	if stage.reservationFD < 0 && stage.reservationStepFD < 0 {
		return nil
	}
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	primary := lockWithContext(ctx, stage.rootFD, stage.owner.ops)
	locked := primary == nil
	if locked && stage.reservationStepFD >= 0 {
		if err := unix.Unlinkat(stage.reservationStepFD, stage.ids.Point, 0); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove capacity reservation", err)
		}
		if err := unix.Fsync(stage.reservationStepFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync capacity reservation removal", err))
		}
		if err := removeDirectoryEntry(ctx, stage.reservationTaskFD, stage.ids.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = joinPrivate(primary, systemError("remove empty reservation step directory", err))
		}
		if err := unix.Fsync(stage.reservationTaskFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync reservation task directory", err))
		}
		if err := removeDirectoryEntry(ctx, stage.reservationRootFD, stage.ids.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = joinPrivate(primary, systemError("remove empty reservation task directory", err))
		}
		if err := unix.Fsync(stage.reservationRootFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync reservation root directory", err))
		}
	}
	growthErr := stage.releaseGrowthMarker(ctx)
	closeErr := closeFDs(ctx, stage.reservationFD, stage.reservationStepFD,
		stage.reservationTaskFD, stage.reservationRootFD)
	stage.reservationFD, stage.reservationStepFD = -1, -1
	stage.reservationTaskFD, stage.reservationRootFD = -1, -1
	stage.reservationRemaining = 0
	if locked {
		unlockErr := rawOperationError("unlock capacity reservation root",
			stage.owner.ops.flock(stage.rootFD, unix.LOCK_UN))
		if unlockErr != nil {
			if primary == nil {
				primary = joinPrivate(unlockErr)
			} else {
				primary = errs.WrapJoined(errs.KindInternal, primary, unlockErr)
			}
		}
	}
	return joinPrivate(primary, growthErr, closeErr)
}
