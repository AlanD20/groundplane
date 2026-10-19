package backupruntime

import (
	"bytes"
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *Staging) Restore(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32,
) (CaptureStage, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return CaptureStage{}, err
	}
	if state == nil || uint64(ordinal) >= uint64(len(owned.Input.Steps)) {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore staging source is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	return state.prepareRestore(ctx, attempt.TaskID, step)
}

func (state *Staging) RestoreRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, ordinal uint32,
) (CaptureStage, error) {
	owned, attempt, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return CaptureStage{}, err
	}
	if state == nil ||
		owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION ||
		uint64(ordinal) >= uint64(len(owned.Input.Steps)) {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore recovery staging authority is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	retained, found, err := state.activeRestoreSource(ctx, attempt.TaskID, step)
	if err != nil || found {
		return retained, err
	}
	return state.prepareRestore(ctx, attempt.TaskID, step)
}

func (state *Staging) prepareRestore(
	ctx context.Context,
	taskID string,
	step *agentpb.BackupStepAuthority,
) (CaptureStage, error) {
	var err error
	restore := step.GetRestore()
	if restore.GetVolume() == nil && restore.GetConfig() == nil && restore.GetPostgres() == nil &&
		restore.GetMysql() == nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Backup source is not a supported Restore")
	}
	evidence := restore.GetExpectedEvidence()
	if evidence == nil || evidence.SourceSizeBytes == 0 || evidence.StoredSizeBytes == 0 ||
		evidence.SourceSizeBytes > backupformat.MaxStoredBytes ||
		evidence.StoredSizeBytes > backupformat.MaxStoredBytes ||
		evidence.SourceSizeBytes > math.MaxUint64-evidence.StoredSizeBytes {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore size evidence is invalid")
	}
	bound := evidence.SourceSizeBytes + evidence.StoredSizeBytes
	sharedObject := restore.GetConfig() == nil &&
		restore.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE
	if sharedObject {
		bound = evidence.SourceSizeBytes
	}
	var headroom uint64
	if restore.GetVolume() != nil {
		headroom = uint64(agentvolumemanifest.MaximumJournalBytes + agentvolumejournal.MaximumJournalBytes)
	} else if config := restore.GetConfig(); config != nil {
		bound, err = backupconfiguration.RequiredRestoreBytes(evidence)
		if err != nil {
			return CaptureStage{}, err
		}
		headroom, err = backupconfigtransfer.JournalGrowthUpperBound(config.GetExpectedArchive().GetContent())
		if err != nil {
			return CaptureStage{}, err
		}
	}
	if bound > math.MaxUint64-headroom {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore reservation exceeds its size limit")
	}
	ids := backupstage.IDs{Task: taskID, Step: step.ExecutionId, Point: restore.PointId}
	key, err := executionplan.BackupSourceStagingKey(ids.Task, ids.Step, ids.Point)
	if err != nil {
		return CaptureStage{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Backup staging session is closed")
	}
	if state.sourceRecovery[string(key)].GetRecoveryRequired() != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore source staging is held by startup recovery")
	}
	if state.active[ids] != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Restore staging is already in use")
	}
	result := CaptureStage{IDs: ids}
	if retained := state.prepared[ids]; retained != nil {
		for _, file := range retained.Files {
			if file.Artifact == nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Restore descriptor is missing")
			}
			switch file.Evidence.Name {
			case executionplan.BackupSourceStagingFinal:
				if result.Source != nil || file.Evidence.Size != evidence.SourceSizeBytes ||
					!bytes.Equal(file.Evidence.SHA256[:], evidence.SourceSha256) {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Recovered Restore source differs from the selected Point",
					)
				}
				result.Source, result.SourceEvidence = file.Artifact, file.Evidence
			case executionplan.BackupStoredStagingFinal:
				if result.Stored != nil || file.Evidence.Size != evidence.StoredSizeBytes ||
					!bytes.Equal(file.Evidence.SHA256[:], evidence.StoredSha256) {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Recovered Restore object differs from the selected Point",
					)
				}
				result.Stored = file.Artifact
			default:
				return CaptureStage{}, errs.New(
					errs.KindStateConflict,
					"Recovered Restore stage has an unexpected file",
				)
			}
		}
		if sharedObject {
			if result.Stored != nil {
				return CaptureStage{}, errs.New(
					errs.KindStateConflict,
					"Unencrypted Restore has an extra stored object",
				)
			}
			result.Stored = result.Source
		}
		if result.Source == nil || result.Stored == nil {
			return CaptureStage{}, errs.New(
				errs.KindStateConflict,
				"Recovered Restore is missing a required source or stored object",
			)
		}
		result.Stage = retained.Stage
		state.active[ids] = retained.Stage
		return result, nil
	}
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(bound+headroom))
	if err != nil {
		return CaptureStage{}, err
	}
	state.active[ids] = stage
	result.Stage = stage
	return result, nil
}
