package backupstage

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func (stage *Stage) releaseGrowthMarker(ctx context.Context) error {
	if stage.growthFD < 0 {
		return nil
	}
	fd := stage.growthFD
	stage.growthFD = -1
	unlockErr := rawOperationError("unlock staging growth admission", stage.owner.ops.flock(fd, unix.LOCK_UN))
	closeErr := rawOperationError("close staging growth admission", unix.Close(fd))
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, unlockErr, closeErr)
	}
	return joinPrivate(closeErr)
}

func (stage *Stage) acquireExclusiveGrowth(ctx context.Context) error {
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	if err := lockWithContext(ctx, stage.rootFD, stage.owner.ops); err != nil {
		return err
	}
	fd, err := openGrowthMarker(ctx, stage.reservationRootFD, stage.owner.ops, false)
	if err != nil {
		return unlockWithPrimary(ctx, err, stage.rootFD, stage.owner.ops)
	}
	if err := stage.owner.ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("reacquire exclusive staging growth admission", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("staging filesystem has an incompatible live growth admission")
		}
		return closeAndUnlock(ctx, primary, stage.rootFD, stage.owner.ops, fd)
	}
	if err := stage.owner.ops.flock(stage.rootFD, unix.LOCK_UN); err != nil {
		return closeWithPrimary(ctx, systemError("unlock capacity reservation root", err), fd)
	}
	stage.growthFD = fd
	return nil
}

func probeGrowthMarker(ctx context.Context, reservationRootFD int, ops linuxOperations) error {
	fd, err := openGrowthMarker(ctx, reservationRootFD, ops, true)
	if err != nil {
		return err
	}
	if err := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock staging growth capability marker", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live staging growth admission violates singleton Agent startup")
		}
		return closeWithPrimary(ctx, primary, fd)
	}
	primary := fallocateError("probe staging filesystem allocation", ops.fallocate(
		fd, unix.FALLOC_FL_KEEP_SIZE, 0, 1,
	))
	if primary == nil {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			primary = systemError("inspect staging allocation probe", err)
		} else if stat.Size != 0 {
			primary = internalError("staging FALLOC_FL_KEEP_SIZE probe changed marker size")
		}
	}
	if primary == nil {
		primary = storageOperationError("sync staging allocation probe", ops.fsync(fd))
	}
	unlockErr := rawOperationError("unlock staging growth capability marker", ops.flock(fd, unix.LOCK_UN))
	if unlockErr != nil {
		if primary == nil {
			primary = joinPrivate(unlockErr)
		} else {
			primary = errs.WrapJoined(errs.KindInternal, primary, unlockErr)
		}
	}
	return closeWithPrimary(ctx, primary, fd)
}

func openOwnerMarker(ctx context.Context, rootFD int, ops linuxOperations) (int, error) {
	flags := uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW)
	fd, err := ops.openat2(rootFD, ownerMarkerName, &unix.OpenHow{
		Flags: flags | uint64(unix.O_CREAT|unix.O_EXCL), Mode: uint64(fileMode),
		Resolve: descendantResolvePolicy,
	})
	created := err == nil
	if err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, storageSystemError("create staging Agent owner marker", err)
	}
	if !created {
		fd, err = ops.openat2(rootFD, ownerMarkerName, &unix.OpenHow{
			Flags: flags, Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return -1, joinPrivate(internalError("staging Agent owner marker is unavailable"),
				systemError("open staging Agent owner marker", err))
		}
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging Agent owner ownership", err), fd)
		}
		if err := unix.Fchmod(fd, fileMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging Agent owner mode", err), fd)
		}
		if err := unix.Fsync(rootFD); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging Agent owner creation", err), fd)
		}
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("staging Agent owner marker is ambiguous"), err), fd)
	}
	return fd, nil
}

func openGrowthMarker(
	ctx context.Context,
	reservationRootFD int,
	ops linuxOperations,
	create bool,
) (int, error) {
	flags := uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW)
	created := false
	var fd int
	var err error
	if create {
		fd, err = ops.openat2(reservationRootFD, growthMarkerName, &unix.OpenHow{
			Flags: flags | uint64(unix.O_CREAT|unix.O_EXCL), Mode: uint64(fileMode),
			Resolve: descendantResolvePolicy,
		})
		if err == nil {
			created = true
		} else if !errors.Is(err, unix.EEXIST) {
			return -1, storageSystemError("create staging growth marker", err)
		}
	}
	if !created {
		fd, err = ops.openat2(reservationRootFD, growthMarkerName, &unix.OpenHow{
			Flags: flags, Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return -1, joinPrivate(internalError("staging growth marker is unavailable"),
				systemError("open staging growth marker", err))
		}
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging growth marker ownership", err), fd)
		}
		if err := unix.Fchmod(fd, fileMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging growth marker mode", err), fd)
		}
		if err := unix.Fsync(reservationRootFD); err != nil {
			return -1, closeWithPrimary(ctx,
				storageSystemError("sync staging growth marker creation", err), fd)
		}
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("staging growth marker is ambiguous"), err), fd)
	}
	return fd, nil
}

func fallocateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return requiredLinuxError("fallocate with FALLOC_FL_KEEP_SIZE")
	}
	return storageSystemError(operation, err)
}
