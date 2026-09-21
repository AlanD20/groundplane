package backupstage

import (
	"context"
	"crypto/sha256"
	"golang.org/x/sys/unix"
	"io"
	"math"
	"os"
)

// CreateFile creates one descriptor-relative named partial with O_EXCL and
// retains its O_RDWR descriptor through publication.
func (stage *Stage) CreateFile(ctx context.Context, name string) (*Artifact, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if stage == nil {
		return nil, internalError("stage is required")
	}
	if err := validateFinalArtifactName(name); err != nil {
		return nil, err
	}
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if stage.closed || stage.cleaned {
		return nil, internalError("stage is closed")
	}
	if stage.capacity.kind == capacityNoGrowth {
		return nil, stateConflictError("recovered stage was resumed without future growth")
	}
	if err := rejectExistingEntry(ctx, stage.pointFD, name); err != nil {
		return nil, err
	}
	temporary := "." + name + ".partial"
	if err := rejectExistingEntry(ctx, stage.pointFD, temporary); err != nil {
		return nil, err
	}
	acquiredGrowth := false
	if stage.capacity.kind == capacityExclusiveUnknown && stage.growthFD < 0 {
		if err := stage.acquireExclusiveGrowth(ctx); err != nil {
			return nil, err
		}
		acquiredGrowth = true
	}
	fd, err := stage.owner.ops.openat2(stage.pointFD, temporary, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:  uint64(fileMode), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		if acquiredGrowth {
			return nil, joinPrivate(storageSystemError("create temporary artifact", err),
				stage.releaseGrowthMarker(ctx))
		}
		return nil, storageSystemError("create temporary artifact", err)
	}
	fail := func(primary error) error {
		closeErr := rawOperationError("close failed temporary artifact", unix.Close(fd))
		unlinkErr := rawOperationError("remove failed temporary artifact",
			unix.Unlinkat(stage.pointFD, temporary, 0))
		syncErr := storageOperationError("sync failed temporary artifact removal", unix.Fsync(stage.pointFD))
		var growthErr error
		if acquiredGrowth {
			growthErr = stage.releaseGrowthMarker(ctx)
		}
		return joinPrivate(primary, closeErr, unlinkErr, syncErr, growthErr)
	}
	if err := unix.Fchown(fd, 0, 0); err != nil {
		return nil, fail(systemError("set temporary artifact ownership", err))
	}
	if err := unix.Fchmod(fd, fileMode); err != nil {
		return nil, fail(systemError("set temporary artifact mode", err))
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return nil, fail(joinPrivate(internalError("temporary artifact metadata is invalid"), err))
	}
	file := os.NewFile(uintptr(fd), "backup-artifact")
	if file == nil {
		return nil, fail(internalError("wrap temporary artifact descriptor"))
	}
	artifact := &Artifact{
		stage: stage, file: file, temporary: temporary, final: name, state: artifactOpen,
		readers: make(map[*preadCloser]struct{}), hasher: sha256.New(),
		growing: stage.capacity.kind == capacityExclusiveUnknown,
	}
	if artifact.growing {
		stage.growingFiles++
	}
	stage.artifacts[artifact] = struct{}{}
	return artifact, nil
}

func (artifact *Artifact) Write(ctx context.Context, content []byte) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if artifact == nil || artifact.stage == nil {
		return 0, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.state != artifactOpen || artifact.file == nil || stage.closed || stage.cleaned {
		return 0, internalError("artifact is not open")
	}
	if stage.poisoned {
		return 0, internalError("stage capacity reservation is poisoned")
	}
	if stage.capacity.kind == capacityBounded && uint64(len(content)) > stage.reservationRemaining {
		return 0, internalError("write exceeds caller-supplied required byte bound")
	}
	if int64(len(content)) > math.MaxInt64-artifact.written {
		return 0, artifact.failWriteLocked(ctx, internalError("staged artifact byte count overflow"))
	}
	if stage.capacity.kind == capacityExclusiveUnknown && len(content) != 0 {
		if err := stage.owner.ops.fallocate(
			int(artifact.file.Fd()), unix.FALLOC_FL_KEEP_SIZE, artifact.written, int64(len(content)),
		); err != nil {
			return 0, artifact.failWriteLocked(ctx, fallocateError("allocate staged artifact range", err))
		}
		if err := contextError(ctx); err != nil {
			return 0, artifact.failWriteLocked(ctx, err)
		}
	}
	written, writeErr := stage.owner.ops.write(int(artifact.file.Fd()), content)
	if written < 0 || written > len(content) {
		return 0, artifact.failWriteLocked(ctx, internalError("staged artifact write returned an invalid count"))
	}
	if written != 0 {
		hashed, hashErr := artifact.hasher.Write(content[:written])
		if hashErr != nil || hashed != written {
			return written, artifact.failWriteLocked(ctx, internalError("staged artifact hash count mismatch"))
		}
		artifact.written += int64(written)
	}
	if stage.capacity.kind == capacityExclusiveUnknown {
		if err := contextError(ctx); err != nil {
			return written, artifact.failWriteLocked(ctx, err)
		}
	}
	var reservationErr error
	if stage.capacity.kind == capacityBounded {
		reservationErr = stage.consumeReservation(ctx, uint64(written))
	}
	if writeErr != nil {
		return written, artifact.failWriteLocked(ctx,
			joinPrivate(storageSystemError("write staged artifact", writeErr), reservationErr))
	}
	if written != len(content) {
		return written, artifact.failWriteLocked(ctx,
			joinPrivate(storageSystemError("write staged artifact", io.ErrShortWrite), reservationErr))
	}
	if reservationErr != nil {
		return written, artifact.failWriteLocked(ctx, reservationErr)
	}
	return written, nil
}

// Publish renames without replacement while retaining and validating the
// original descriptor. Evidence is returned only after the final inode and
// parent namespace are durable; a successful replay returns the same evidence.
func (artifact *Artifact) Publish(ctx context.Context) (ArtifactEvidence, error) {
	if err := contextError(ctx); err != nil {
		return ArtifactEvidence{}, err
	}
	if artifact == nil || artifact.stage == nil {
		return ArtifactEvidence{}, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.file == nil || stage.closed || stage.cleaned {
		return ArtifactEvidence{}, internalError("artifact is not open")
	}
	if artifact.state == artifactPublished {
		return artifact.evidenceLocked(), nil
	}
	if artifact.state != artifactOpen {
		return ArtifactEvidence{}, internalError("artifact is not open")
	}
	if err := stage.owner.ops.fsync(int(artifact.file.Fd())); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, storageSystemError("sync temporary artifact", err))
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(artifact.file.Fd()), &stat); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, systemError("inspect temporary artifact", err))
	}
	if stat.Size != artifact.written {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, internalError("staged artifact size does not match counted bytes"),
		)
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.written, 1); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx,
			joinPrivate(internalError("temporary artifact metadata is invalid"), err))
	}
	if err := stage.syncParents(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	if err := stage.owner.ops.renameat2(
		stage.pointFD, artifact.temporary, stage.pointFD, artifact.final, unix.RENAME_NOREPLACE,
	); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, storageSystemError("publish named partial artifact", err),
		)
	}
	artifact.state = artifactLinked
	if err := stage.syncParents(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	if err := unix.Fstat(int(artifact.file.Fd()), &stat); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, systemError("inspect published artifact", err))
	}
	if stat.Size != artifact.written {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, internalError("published artifact size does not match counted bytes"),
		)
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.written, 1); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx,
			joinPrivate(internalError("published artifact metadata is invalid"), err))
	}
	copy(artifact.digest[:], artifact.hasher.Sum(nil))
	if err := artifact.sealGrowthLocked(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	artifact.size, artifact.state = artifact.written, artifactPublished
	return artifact.evidenceLocked(), nil
}

func (artifact *Artifact) evidenceLocked() ArtifactEvidence {
	return ArtifactEvidence{Name: artifact.final, Size: uint64(artifact.size), SHA256: artifact.digest}
}
