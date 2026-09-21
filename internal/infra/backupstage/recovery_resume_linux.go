package backupstage

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
	"math"
)

func (session *RecoverySession) resumePreparedLocked(
	ctx context.Context,
	recovered *recoveredStage,
	disposition ResumeDisposition,
) error {
	if !validRecoveryCapacity(disposition.RemainingGrowth) {
		return validationError("recovered remaining growth must be NoGrowth, Bounded, or ExclusiveUnknown")
	}
	if len(disposition.ExpectedFiles) != len(recovered.entry.Files) {
		return internalError("controller recovery evidence file count does not match retained files")
	}
	verified := make([]ArtifactEvidence, 0, len(recovered.entry.Files))
	for index, expected := range disposition.ExpectedFiles {
		inventory := recovered.entry.Files[index]
		if expected != inventory {
			return internalError("controller recovery evidence does not match startup inventory")
		}
		file := recovered.files[expected.Name]
		if file == nil {
			return internalError("retained recovery artifact descriptor is missing")
		}
		actual, err := evidenceFromFD(ctx, expected.Name, int(file.Fd()))
		if err != nil {
			return err
		}
		if actual != expected {
			return internalError("retained recovery artifact evidence changed before resume")
		}
		verified = append(verified, actual)
	}
	capacity := disposition.RemainingGrowth
	var growthFD = -1
	if capacity.kind != capacityNoGrowth {
		var err error
		growthFD, err = openGrowthMarker(ctx, session.reservationRootFD, session.stager.ops, false)
		if err != nil {
			return err
		}
		lock := unix.LOCK_SH
		if capacity.kind == capacityExclusiveUnknown {
			lock = unix.LOCK_EX
		}
		if err := session.stager.ops.flock(growthFD, lock|unix.LOCK_NB); err != nil {
			primary := systemError("admit recovered stage growth", err)
			if flockWouldBlock(err) {
				primary = stateConflictError("recovered growth conflicts with another resumed stage")
			}
			return closeWithPrimary(ctx, primary, growthFD)
		}
	}
	remaining := capacity.requiredBytes
	if capacity.kind == capacityBounded {
		available, err := availableBytes(ctx, session.stager.rootFD, session.stager.ops)
		if err != nil {
			return closeWithPrimary(ctx, err, growthFD)
		}
		wanted, overflow := addUint64(session.reserved, remaining)
		if overflow {
			return closeWithPrimary(ctx, internalError("recovered capacity accounting overflow"), growthFD)
		}
		if wanted > available {
			return closeWithPrimary(ctx,
				errs.New(errs.KindStorageUnavailable, "recovered stage has insufficient free space"), growthFD)
		}
	}
	if err := unix.Fchown(recovered.reservationFD, 0, 0); err != nil {
		return closeWithPrimary(ctx, systemError("set recovered reservation ownership", err), growthFD)
	}
	if err := unix.Fchmod(recovered.reservationFD, fileMode); err != nil {
		return closeWithPrimary(ctx, systemError("set recovered reservation mode", err), growthFD)
	}
	if err := writeReservation(ctx, recovered.reservationFD, remaining, session.stager.ops); err != nil {
		return closeWithPrimary(ctx, err, growthFD)
	}
	if err := session.stager.ops.fsync(recovered.reservationFD); err != nil {
		return closeWithPrimary(ctx, storageSystemError("sync recovered capacity reservation", err), growthFD)
	}
	if err := session.stager.ops.fsync(recovered.reservationStepFD); err != nil {
		return closeWithPrimary(ctx,
			storageSystemError("sync recovered reservation directory", err), growthFD)
	}
	stage := &Stage{
		owner: session.stager, rootFD: recovered.rootFD, taskFD: recovered.taskFD,
		stepFD: recovered.stepFD, pointFD: recovered.pointFD,
		reservationRootFD: recovered.reservationRootFD, reservationTaskFD: recovered.reservationTaskFD,
		reservationStepFD: recovered.reservationStepFD, reservationFD: recovered.reservationFD,
		growthFD: growthFD, reservationRemaining: remaining, capacity: capacity,
		ids: recovered.entry.IDs, artifacts: make(map[*Artifact]struct{}),
	}
	prepared := &PreparedStage{
		RecoveryID: disposition.RecoveryID, DispositionID: disposition.DispositionID,
		IDs: recovered.entry.IDs, Stage: stage,
	}
	for _, item := range verified {
		file := recovered.files[item.Name]
		artifact := &Artifact{
			stage: stage, file: file, final: item.Name, size: int64(item.Size), written: int64(item.Size),
			state: artifactPublished, readers: make(map[*preadCloser]struct{}), digest: item.SHA256,
		}
		stage.artifacts[artifact] = struct{}{}
		prepared.Files = append(prepared.Files, PreparedArtifact{Evidence: item, Artifact: artifact})
		delete(recovered.files, item.Name)
	}
	recovered.rootFD, recovered.taskFD, recovered.stepFD, recovered.pointFD = -1, -1, -1, -1
	recovered.reservationRootFD, recovered.reservationTaskFD = -1, -1
	recovered.reservationStepFD, recovered.reservationFD = -1, -1
	recovered.resolved = true
	recovered.resumed = true
	recovered.dispositionID = disposition.DispositionID
	storedDisposition := disposition
	storedDisposition.ExpectedFiles = append([]ArtifactEvidence(nil), disposition.ExpectedFiles...)
	recovered.resumeDisposition = &storedDisposition
	session.stager.leases[stage.ids] = stage
	session.prepared = append(session.prepared, prepared)
	if capacity.kind == capacityBounded {
		session.reserved += remaining
	}
	return nil
}

func validRecoveryCapacity(capacity CapacityMode) bool {
	switch capacity.kind {
	case capacityNoGrowth, capacityExclusiveUnknown:
		return capacity.requiredBytes == 0
	case capacityBounded:
		return capacity.requiredBytes > 0 && capacity.requiredBytes <= math.MaxInt64
	default:
		return false
	}
}

func recoveryDispositionEqual(stored *ResumeDisposition, candidate ResumeDisposition) bool {
	if stored == nil || stored.RecoveryID != candidate.RecoveryID ||
		stored.DispositionID != candidate.DispositionID || stored.RemainingGrowth != candidate.RemainingGrowth ||
		len(stored.ExpectedFiles) != len(candidate.ExpectedFiles) {
		return false
	}
	for index := range stored.ExpectedFiles {
		if stored.ExpectedFiles[index] != candidate.ExpectedFiles[index] {
			return false
		}
	}
	return true
}
