package backupstage

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
)

func (session *RecoverySession) cleanupUnindexedNamespacesLocked(
	ctx context.Context,
	indexed map[IDs]struct{},
) error {
	tasks, err := directoryNames(ctx, session.stager.rootFD, "staging task namespaces")
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task == reservationDirName || task == ownerMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "unindexed task"); err != nil {
			return err
		}
		taskFD, err := openManagedReservationDirectory(ctx, session.stager.rootFD, task, session.stager.ops)
		if err != nil {
			return err
		}
		steps, err := directoryNames(ctx, taskFD, "unindexed staging steps")
		if err != nil {
			return closeWithPrimary(ctx, err, taskFD)
		}
		for _, step := range steps {
			if err := validateManagedReservationComponent(step, "unindexed step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			stepFD, err := openManagedReservationDirectory(ctx, taskFD, step, session.stager.ops)
			if err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			points, err := directoryNames(ctx, stepFD, "unindexed staging points")
			if err != nil {
				return closeWithPrimary(ctx, err, stepFD, taskFD)
			}
			for _, point := range points {
				ids := IDs{Task: task, Step: step, Point: point}
				if err := validateIDs(ids); err != nil {
					return closeWithPrimary(ctx,
						joinPrivate(internalError("managed unindexed point name is invalid"), err),
						stepFD, taskFD)
				}
				if _, exists := indexed[ids]; exists {
					continue
				}
				pointFD, err := openManagedReservationDirectory(ctx, stepFD, point, session.stager.ops)
				if err != nil {
					return closeWithPrimary(ctx, err, stepFD, taskFD)
				}
				if err := session.stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
					primary := systemError("lock unindexed staging point", err)
					if flockWouldBlock(err) {
						primary = stateConflictError("live unindexed point violates Agent ownership")
					}
					return closeWithPrimary(ctx, primary, pointFD, stepFD, taskFD)
				}
				files, evidence, err := scanRecoveredFiles(ctx, pointFD, session.stager.ops)
				if err != nil {
					return closeRecoveredCapture(ctx, err, files, pointFD, stepFD, taskFD)
				}
				if len(evidence) != 0 {
					return closeRecoveredCapture(ctx,
						internalError("unindexed final artifact violates durable publication ordering"),
						files, pointFD, stepFD, taskFD)
				}
				if err := removeDirectoryEntry(ctx, stepFD, point); err != nil && !errors.Is(err, unix.ENOENT) {
					return closeWithPrimary(ctx,
						systemError("remove unindexed point directory", err), pointFD, stepFD, taskFD)
				}
				if err := syncFD(ctx, stepFD); err != nil {
					return closeWithPrimary(ctx, err, pointFD, stepFD, taskFD)
				}
				if err := unix.Close(pointFD); err != nil {
					return closeWithPrimary(ctx,
						systemError("close unindexed point directory", err), stepFD, taskFD)
				}
			}
			if err := unix.Close(stepFD); err != nil {
				return closeWithPrimary(ctx, systemError("close unindexed step directory", err), taskFD)
			}
			if err := removeEmptyReservationDirectory(ctx, taskFD, step, "unindexed step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
		}
		if err := unix.Close(taskFD); err != nil {
			return systemError("close unindexed task directory", err)
		}
		if err := removeEmptyReservationDirectory(ctx, session.stager.rootFD, task, "unindexed task"); err != nil {
			return err
		}
	}
	return nil
}

func (session *RecoverySession) discardRecoveredLocked(ctx context.Context, recovered *recoveredStage) error {
	var primary error
	for name, file := range recovered.files {
		if err := file.Close(); err != nil {
			primary = joinPrivate(primary, systemError("close discarded recovered artifact "+name, err))
		}
		delete(recovered.files, name)
	}
	if primary == nil {
		primary = removePointContents(ctx, recovered.pointFD, session.stager.ops)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.stepFD, recovered.entry.IDs.Point); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove discarded recovered point", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.stepFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.taskFD, recovered.entry.IDs.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty discarded recovered step", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.taskFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.rootFD, recovered.entry.IDs.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty discarded recovered task", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.rootFD)
	}
	if primary == nil {
		if err := unix.Unlinkat(recovered.reservationStepFD, recovered.entry.IDs.Point, 0); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove discarded recovered reservation", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationStepFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.reservationTaskFD, recovered.entry.IDs.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty recovered reservation step", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationTaskFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.reservationRootFD, recovered.entry.IDs.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty recovered reservation task", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationRootFD)
	}
	closeErr := closeFDs(ctx, recovered.reservationFD, recovered.reservationStepFD,
		recovered.reservationTaskFD, recovered.reservationRootFD,
		recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD)
	recovered.reservationFD, recovered.reservationStepFD = -1, -1
	recovered.reservationTaskFD, recovered.reservationRootFD = -1, -1
	recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD = -1, -1, -1, -1
	return joinPrivate(primary, closeErr)
}
