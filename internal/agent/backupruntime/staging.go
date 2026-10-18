package backupruntime

import (
	"bytes"
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Staging owns descriptors only after startup recovery has resolved every
// inventory entry. It does not infer recovery permission from Task history.
type Staging struct {
	mu             sync.Mutex
	closed         bool
	stager         *backupstage.Stager
	prepared       map[backupstage.IDs]*backupstage.PreparedStage
	held           map[backupstage.IDs]*backupstage.PreparedStage
	active         map[backupstage.IDs]*backupstage.Stage
	volumeRecovery []volumeRecoveryPermission
	sourceRecovery map[string]*agentpb.BackupStagingDisposition
}

func NewStaging(stager *backupstage.Stager, prepared []*backupstage.PreparedStage,
	inventory *agentpb.BackupStagingInventory, plan *agentpb.BackupStagingRecoveryPlan,
) (*Staging, error) {
	if stager == nil {
		return nil, errs.New(errs.KindInternal, "Backup staging requires completed startup recovery")
	}
	state := &Staging{stager: stager, prepared: make(map[backupstage.IDs]*backupstage.PreparedStage), held: make(map[backupstage.IDs]*backupstage.PreparedStage), active: make(map[backupstage.IDs]*backupstage.Stage)}
	permissions, err := volumeRecoveryPermissions(inventory, plan)
	if err != nil {
		return nil, err
	}
	state.volumeRecovery = permissions
	state.sourceRecovery = make(map[string]*agentpb.BackupStagingDisposition, len(plan.Dispositions))
	for _, disposition := range plan.Dispositions {
		state.sourceRecovery[string(disposition.RecoveryKeySha256)] = proto.CloneOf(disposition)
	}
	for _, row := range prepared {
		if row == nil || row.Stage == nil || row.DispositionID == "" || state.prepared[row.IDs] != nil || state.held[row.IDs] != nil {
			return nil, errs.New(errs.KindStateConflict, "Backup staging recovery contains an invalid or duplicate source")
		}
		key, err := executionplan.BackupSourceStagingKey(row.IDs.Task, row.IDs.Step, row.IDs.Point)
		if err != nil {
			return nil, err
		}
		disposition := state.sourceRecovery[string(key)]
		if disposition.GetResumePrepared() == nil && disposition.GetRecoveryRequired() == nil {
			return nil, errs.New(errs.KindStateConflict, "Prepared Backup files have no source resume decision")
		}
		owned := *row
		owned.Files = append([]backupstage.PreparedArtifact(nil), row.Files...)
		if disposition.GetRecoveryRequired() != nil {
			state.held[row.IDs] = &owned
		} else {
			state.prepared[row.IDs] = &owned
		}
	}
	return state, nil
}

type CaptureStage struct {
	IDs            backupstage.IDs
	Stage          *backupstage.Stage
	Source         *backupstage.Artifact
	SourceEvidence backupstage.ArtifactEvidence
	Stored         *backupstage.Artifact
}

func (state *Staging) VolumeCapture(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, resume *agentpb.BackupCaptureResume,
) (CaptureStage, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return CaptureStage{}, err
	}
	if state == nil || uint64(ordinal) >= uint64(len(owned.Input.Steps)) || resume == nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Volume capture source is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	if step.GetCapture().GetVolume() == nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Backup source is not a Volume capture")
	}
	// ExecutionId identifies the captured source across reconnects; a different
	// work input cannot inherit the older source's private staging namespace.
	ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetCapture().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Backup staging session is closed")
	}
	if state.active[ids] != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Backup source staging is already in use")
	}
	result := CaptureStage{IDs: ids}
	if retained := state.prepared[ids]; retained != nil {
		prepared := resume.GetPreparedArtifact()
		if prepared.GetPointId() != ids.Point || prepared.GetVolume() == nil || prepared.GetEvidence() == nil || prepared.GetFinals() == nil {
			return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume files lack acknowledged artifact evidence")
		}
		for _, file := range retained.Files {
			if file.Artifact == nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume descriptor is missing")
			}
			switch file.Evidence.Name {
			case executionplan.BackupSourceStagingFinal:
				if result.Source != nil || file.Evidence.Size != prepared.Evidence.SourceSizeBytes || !bytes.Equal(file.Evidence.SHA256[:], prepared.Evidence.SourceSha256) {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume source differs from captured evidence")
				}
				result.Source, result.SourceEvidence = file.Artifact, file.Evidence
			case executionplan.BackupStoredStagingFinal:
				if result.Stored != nil || file.Evidence.Size != prepared.Evidence.StoredSizeBytes || !bytes.Equal(file.Evidence.SHA256[:], prepared.Evidence.StoredSha256) {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume object differs from captured evidence")
				}
				result.Stored = file.Artifact
			default:
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume stage contains an unexpected file")
			}
		}
		if result.Source == nil {
			return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Volume source is missing")
		}
		if prepared.Finals.GetSameInode() {
			if result.Stored != nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Unencrypted Volume stage contains an extra stored object")
			}
			result.Stored = result.Source
		} else if result.Stored == nil {
			return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered encrypted Volume object is missing")
		}
		result.Stage = retained.Stage
		state.active[ids] = retained.Stage
		return result, nil
	}
	if resume.GetPreparedArtifact() != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Acknowledged Volume artifact has no retained staging files")
	}
	stage, err := state.stager.Prepare(ctx, ids, backupstage.ExclusiveUnknown())
	if err != nil {
		return CaptureStage{}, err
	}
	state.active[ids] = stage
	result.Stage = stage
	result.Source, err = stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
	return result, err
}

func (state *Staging) SourceAbsent(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity, ordinal uint32) (bool, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return false, err
	}
	if state == nil || uint64(ordinal) >= uint64(len(owned.Input.Steps)) {
		return false, errs.New(errs.KindStateConflict, "Backup staging source is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	point := step.GetCapture().GetPointId()
	if point == "" {
		point = step.GetRestore().GetPointId()
	}
	if point == "" {
		return false, errs.New(errs.KindStateConflict, "Backup source has no staged artifact")
	}
	ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: point}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return false, errs.New(errs.KindStateConflict, "Backup staging session is closed")
	}
	return state.active[ids] == nil && state.prepared[ids] == nil, nil
}

func (state *Staging) Cleanup(ctx context.Context, captured CaptureStage) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errs.New(errs.KindStateConflict, "Backup staging session is closed")
	}
	if captured.Stage == nil || state.active[captured.IDs] != captured.Stage {
		return errs.New(errs.KindStateConflict, "Backup source does not own this stage")
	}
	if err := captured.Stage.Cleanup(ctx); err != nil {
		return err
	}
	delete(state.active, captured.IDs)
	delete(state.prepared, captured.IDs)
	return nil
}
