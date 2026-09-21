package backupstage

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
)

func sweepReservations(
	ctx context.Context,
	stageRootFD int,
	reservationRootFD int,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	tasks, err := directoryNames(ctx, reservationRootFD, "reservation tasks")
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, task := range tasks {
		if task == growthMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "reservation task"); err != nil {
			return 0, err
		}
		value, err := sweepReservationTask(ctx, stageRootFD, reservationRootFD, task, ops, startup)
		if err != nil {
			return 0, err
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, internalError("capacity reservation accounting overflow")
		}
	}
	return total, nil
}

func sweepReservationTask(
	ctx context.Context,
	stageRootFD int,
	reservationRootFD int,
	task string,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	taskFD, err := openManagedReservationDirectory(ctx, reservationRootFD, task, ops)
	if err != nil {
		return 0, err
	}
	steps, err := directoryNames(ctx, taskFD, "reservation steps")
	if err != nil {
		return 0, closeWithPrimary(ctx, err, taskFD)
	}
	var total uint64
	for _, step := range steps {
		if err := validateManagedReservationComponent(step, "step"); err != nil {
			return 0, closeWithPrimary(ctx, err, taskFD)
		}
		value, err := sweepReservationStep(ctx, stageRootFD, taskFD, IDs{Task: task, Step: step}, ops, startup)
		if err != nil {
			return 0, closeWithPrimary(ctx, err, taskFD)
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, closeWithPrimary(ctx,
				internalError("capacity reservation accounting overflow"), taskFD)
		}
	}
	if err := unix.Close(taskFD); err != nil {
		return 0, systemError("close reservation task directory", err)
	}
	if err := removeEmptyReservationDirectory(ctx, reservationRootFD, task, "task"); err != nil {
		return 0, err
	}
	return total, nil
}

func sweepReservationStep(
	ctx context.Context,
	stageRootFD int,
	reservationTaskFD int,
	ids IDs,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	stepFD, err := openManagedReservationDirectory(ctx, reservationTaskFD, ids.Step, ops)
	if err != nil {
		return 0, err
	}
	points, err := directoryNames(ctx, stepFD, "reservation points")
	if err != nil {
		return 0, closeWithPrimary(ctx, err, stepFD)
	}
	var total uint64
	for _, point := range points {
		pointIDs := IDs{Task: ids.Task, Step: ids.Step, Point: point}
		if err := validateIDs(pointIDs); err != nil {
			return 0, closeWithPrimary(ctx,
				joinPrivate(internalError("managed reservation point name is invalid"), err), stepFD)
		}
		value, err := sweepReservationLeaf(ctx, stageRootFD, stepFD, pointIDs, ops, startup)
		if err != nil {
			return 0, closeWithPrimary(ctx, err, stepFD)
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, closeWithPrimary(ctx,
				internalError("capacity reservation accounting overflow"), stepFD)
		}
	}
	if err := unix.Close(stepFD); err != nil {
		return 0, systemError("close reservation step directory", err)
	}
	if err := removeEmptyReservationDirectory(ctx, reservationTaskFD, ids.Step, "step"); err != nil {
		return 0, err
	}
	return total, nil
}

func sweepReservationLeaf(
	ctx context.Context,
	_ int,
	reservationStepFD int,
	ids IDs,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	fd, err := ops.openat2(reservationStepFD, ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return 0, joinPrivate(internalError("managed reservation leaf is ambiguous"),
			systemError("open capacity reservation", err))
	}
	lockErr := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if lockErr == nil {
		if err := validateStaleReservationLeaf(ctx, fd); err != nil {
			return 0, closeWithPrimary(ctx, err, fd)
		}
		return 0, closeWithPrimary(ctx,
			internalError("stale reservation appeared after mandatory startup recovery"), fd)
	}
	if !flockWouldBlock(lockErr) {
		return 0, closeWithPrimary(ctx,
			systemError("inspect capacity reservation lock", lockErr), fd)
	}
	if startup {
		return 0, closeWithPrimary(ctx,
			stateConflictError("live capacity reservation violates singleton Agent startup"), fd)
	}
	if err := validateRegularFD(ctx, fd, -1, 1); err != nil {
		return 0, closeWithPrimary(ctx,
			joinPrivate(internalError("capacity reservation state is corrupt"), err), fd)
	}
	value, readErr := readReservation(ctx, fd)
	closeErr := unix.Close(fd)
	if readErr != nil {
		return 0, joinPrivate(readErr, rawOperationError("close capacity reservation", closeErr))
	}
	if closeErr != nil {
		return 0, systemError("close capacity reservation", closeErr)
	}
	return value, nil
}

func validateStaleReservationLeaf(ctx context.Context, fd int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return joinPrivate(internalError("managed reservation leaf is ambiguous"),
			systemError("inspect stale capacity reservation", err))
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return internalError("managed reservation leaf is ambiguous")
	}
	return nil
}

func validateManagedReservationComponent(value, label string) error {
	if err := validateComponent(value, "reservation "+label); err != nil {
		return joinPrivate(internalError("managed reservation "+label+" name is invalid"), err)
	}
	return nil
}

func validateManagedTaskComponent(value, label string) error {
	if err := validateTaskComponent(value); err != nil {
		return joinPrivate(internalError("managed "+label+" name is invalid"), err)
	}
	return nil
}

func openManagedReservationDirectory(
	ctx context.Context,
	parentFD int,
	name string,
	ops linuxOperations,
) (int, error) {
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	if err != nil {
		return -1, joinPrivate(internalError("managed reservation directory is ambiguous"),
			systemError("open managed reservation directory", err))
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("managed reservation directory is ambiguous"), err), fd)
	}
	return fd, nil
}

func removeEmptyReservationDirectory(ctx context.Context, parentFD int, name, label string) error {
	err := removeDirectoryEntry(ctx, parentFD, name)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTEMPTY) {
		return nil
	}
	if err != nil {
		return systemError("remove empty reservation "+label+" directory", err)
	}
	return syncFD(ctx, parentFD)
}

func openRecoveryDirectory(
	ctx context.Context,
	parentFD int,
	name string,
	ops linuxOperations,
) (int, bool, error) {
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	if errors.Is(err, unix.ENOENT) {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, joinPrivate(internalError("managed stage namespace is ambiguous"),
			systemError("open abandoned stage directory", err))
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, false, closeWithPrimary(ctx,
			joinPrivate(internalError("managed stage namespace is ambiguous"), err), fd)
	}
	return fd, true, nil
}
