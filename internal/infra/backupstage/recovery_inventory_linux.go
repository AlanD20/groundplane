package backupstage

import (
	"context"
	"crypto/sha256"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

func (session *RecoverySession) inventoryLocked(ctx context.Context) error {
	indexed := make(map[IDs]struct{})
	tasks, err := directoryNames(ctx, session.reservationRootFD, "recovery reservation tasks")
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task == growthMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "reservation task"); err != nil {
			return err
		}
		taskFD, err := openManagedReservationDirectory(ctx, session.reservationRootFD, task, session.stager.ops)
		if err != nil {
			return err
		}
		steps, err := directoryNames(ctx, taskFD, "recovery reservation steps")
		if err != nil {
			return closeWithPrimary(ctx, err, taskFD)
		}
		for _, step := range steps {
			if err := validateManagedReservationComponent(step, "step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			stepFD, err := openManagedReservationDirectory(ctx, taskFD, step, session.stager.ops)
			if err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			points, err := directoryNames(ctx, stepFD, "recovery reservation points")
			if err != nil {
				return closeWithPrimary(ctx, err, stepFD, taskFD)
			}
			for _, point := range points {
				ids := IDs{Task: task, Step: step, Point: point}
				if err := validateIDs(ids); err != nil {
					return closeWithPrimary(ctx,
						joinPrivate(internalError("managed recovery reservation name is invalid"), err),
						stepFD, taskFD)
				}
				indexed[ids] = struct{}{}
				if err := session.captureRecoveredLocked(ctx, ids, taskFD, stepFD); err != nil {
					return closeWithPrimary(ctx, err, stepFD, taskFD)
				}
			}
			if err := unix.Close(stepFD); err != nil {
				return closeWithPrimary(ctx, systemError("close recovery reservation step", err), taskFD)
			}
			if err := removeEmptyReservationDirectory(ctx, taskFD, step, "step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
		}
		if err := unix.Close(taskFD); err != nil {
			return systemError("close recovery reservation task", err)
		}
		if err := removeEmptyReservationDirectory(ctx, session.reservationRootFD, task, "task"); err != nil {
			return err
		}
	}
	return session.cleanupUnindexedNamespacesLocked(ctx, indexed)
}

func (session *RecoverySession) captureRecoveredLocked(
	ctx context.Context,
	ids IDs,
	reservationTaskFD int,
	reservationStepFD int,
) error {
	reservationFD, err := session.stager.ops.openat2(reservationStepFD, ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return joinPrivate(internalError("managed recovery reservation leaf is ambiguous"),
			systemError("open recovery reservation leaf", err))
	}
	if err := session.stager.ops.flock(reservationFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock recovery reservation leaf", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live recovery reservation violates Agent ownership")
		}
		return closeWithPrimary(ctx, primary, reservationFD)
	}
	if err := validateStaleReservationLeaf(ctx, reservationFD); err != nil {
		return closeWithPrimary(ctx, err, reservationFD)
	}
	taskFD, exists, err := openRecoveryDirectory(ctx, session.stager.rootFD, ids.Task, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD)
	}
	if !exists {
		return removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD)
	}
	stepFD, exists, err := openRecoveryDirectory(ctx, taskFD, ids.Step, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD, taskFD)
	}
	if !exists {
		return closeWithPrimary(ctx,
			removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD), taskFD)
	}
	pointFD, exists, err := openRecoveryDirectory(ctx, stepFD, ids.Point, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD, stepFD, taskFD)
	}
	if !exists {
		return closeWithPrimary(ctx,
			removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD), stepFD, taskFD)
	}
	if err := session.stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock recovered point namespace", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live recovered point violates Agent ownership")
		}
		return closeWithPrimary(ctx, primary, reservationFD, pointFD, stepFD, taskFD)
	}
	files, evidence, err := scanRecoveredFiles(ctx, pointFD, session.stager.ops)
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationFD, pointFD, stepFD, taskFD)
	}
	rootFD, err := duplicateFD(ctx, session.stager.rootFD, "recovered stage root")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationFD, pointFD, stepFD, taskFD)
	}
	reservationRootFD, err := duplicateFD(ctx, session.reservationRootFD, "recovery reservation root")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, rootFD, reservationFD, pointFD, stepFD, taskFD)
	}
	reservationTaskCopy, err := duplicateFD(ctx, reservationTaskFD, "recovery reservation task")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationRootFD, rootFD,
			reservationFD, pointFD, stepFD, taskFD)
	}
	reservationStepCopy, err := duplicateFD(ctx, reservationStepFD, "recovery reservation step")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationTaskCopy, reservationRootFD, rootFD,
			reservationFD, pointFD, stepFD, taskFD)
	}
	recovered := &recoveredStage{
		entry: Recovered{
			RecoveryID: recoveryID(ids), IDs: ids, Files: evidence,
			ReservationState: ReservationUntrusted,
		},
		rootFD: rootFD, taskFD: taskFD, stepFD: stepFD, pointFD: pointFD,
		reservationRootFD: reservationRootFD, reservationTaskFD: reservationTaskCopy,
		reservationStepFD: reservationStepCopy, reservationFD: reservationFD, files: files,
	}
	if len(evidence) == 0 {
		if err := session.discardRecoveredLocked(ctx, recovered); err != nil {
			return err
		}
		return nil
	}
	session.recovered[recovered.entry.RecoveryID] = recovered
	session.order = append(session.order, recovered.entry.RecoveryID)
	return nil
}

func scanRecoveredFiles(
	ctx context.Context,
	pointFD int,
	ops linuxOperations,
) (map[string]*os.File, []ArtifactEvidence, error) {
	names, err := directoryNames(ctx, pointFD, "recovered point contents")
	if err != nil {
		return nil, nil, err
	}
	files := make(map[string]*os.File)
	var evidence []ArtifactEvidence
	partialsRemoved := false
	for _, name := range names {
		fd, err := ops.openat2(pointFD, name, &unix.OpenHow{
			Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return files, evidence, joinPrivate(internalError("managed recovered artifact is ambiguous"),
				systemError("open recovered artifact", err))
		}
		if err := validateRegularFD(ctx, fd, -1, 1); err != nil {
			return files, evidence, closeWithPrimary(ctx,
				joinPrivate(internalError("managed recovered artifact metadata is invalid"), err), fd)
		}
		if isManagedPartial(name) {
			if err := unix.Close(fd); err != nil {
				return files, evidence, systemError("close recovered partial", err)
			}
			if err := unix.Unlinkat(pointFD, name, 0); err != nil {
				return files, evidence, systemError("remove recovered partial", err)
			}
			partialsRemoved = true
			continue
		}
		if err := validateFinalArtifactName(name); err != nil {
			return files, evidence, closeWithPrimary(ctx,
				joinPrivate(internalError("managed recovered artifact name is invalid"), err), fd)
		}
		item, err := evidenceFromFD(ctx, name, fd)
		if err != nil {
			return files, evidence, closeWithPrimary(ctx, err, fd)
		}
		file := os.NewFile(uintptr(fd), "recovered-backup-artifact")
		if file == nil {
			return files, evidence, closeWithPrimary(ctx,
				internalError("wrap recovered artifact descriptor"), fd)
		}
		files[name], evidence = file, append(evidence, item)
	}
	if partialsRemoved {
		if err := syncFD(ctx, pointFD); err != nil {
			return files, evidence, err
		}
	}
	return files, evidence, nil
}

func evidenceFromFD(ctx context.Context, name string, fd int) (ArtifactEvidence, error) {
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return ArtifactEvidence{}, systemError("inspect recovered artifact", err)
	}
	if before.Size < 0 {
		return ArtifactEvidence{}, internalError("recovered artifact has negative size")
	}
	hasher := sha256.New()
	buffer := make([]byte, 128*1024)
	for offset := int64(0); offset < before.Size; {
		if err := contextError(ctx); err != nil {
			return ArtifactEvidence{}, err
		}
		length := int64(len(buffer))
		if length > before.Size-offset {
			length = before.Size - offset
		}
		n, err := unix.Pread(fd, buffer[:length], offset)
		if err != nil {
			return ArtifactEvidence{}, systemError("hash recovered artifact", err)
		}
		if n == 0 {
			return ArtifactEvidence{}, internalError("recovered artifact changed while hashing")
		}
		if _, err := hasher.Write(buffer[:n]); err != nil {
			return ArtifactEvidence{}, internalError("hash recovered artifact bytes")
		}
		offset += int64(n)
	}
	if err := validateRegularFD(ctx, fd, before.Size, 1); err != nil {
		return ArtifactEvidence{}, joinPrivate(internalError("recovered artifact metadata changed"), err)
	}
	item := ArtifactEvidence{Name: name, Size: uint64(before.Size)}
	copy(item.SHA256[:], hasher.Sum(nil))
	return item, nil
}

func isManagedPartial(name string) bool {
	if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".partial") {
		return false
	}
	base := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".partial")
	return validateComponent(base, "partial artifact name") == nil
}

func recoveryID(ids IDs) RecoveryID {
	return RecoveryID(ids.Task + "/" + ids.Step + "/" + ids.Point)
}

func duplicateFD(ctx context.Context, fd int, label string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	duplicate, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return -1, systemError("duplicate "+label+" descriptor", err)
	}
	return duplicate, nil
}

func removeOrphanReservation(ctx context.Context, parentFD int, name string, fd int) error {
	if err := unix.Unlinkat(parentFD, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return closeWithPrimary(ctx, systemError("remove orphan recovery reservation", err), fd)
	}
	if err := unix.Fsync(parentFD); err != nil {
		return closeWithPrimary(ctx, storageSystemError("sync orphan recovery reservation removal", err), fd)
	}
	return closeFDs(ctx, fd)
}

func closeRecoveredCapture(ctx context.Context, primary error, files map[string]*os.File, fds ...int) error {
	var causes []error
	for _, file := range files {
		if err := file.Close(); err != nil {
			causes = append(causes, systemError("close recovered artifact", err))
		}
	}
	causes = append(causes, closeFDs(ctx, fds...))
	return joinPrivate(primary, wrapPrivateCauses("close recovered capture", causes...))
}
