package backupwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareCaptureProgress joins resource-run progress to the native checkpoint.
// Source cleanup additionally publishes the Point, indexes and retention
// obligation in the same change. Upload verification alone never publishes it.
func (repository *InputRepository) PrepareCaptureProgress(ctx context.Context, view workqueue.ResumeView,
	admitted CheckpointAdmission, evidence CheckpointEvidence, at time.Time,
) (workqueue.Change, error) {
	if admitted.Replay != nil || admitted.Assignment == nil ||
		admitted.Assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		!backupruntime.ValidBackupRuntimeInstant(at) {
		return workqueue.Change{}, conflict("Backup capture progress requires an admitted checkpoint")
	}
	nextCursor, err := advanceCheckpoint(admitted, view.Attempt.Attempt.Executor, evidence, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := backupruntime.BackupRunKey(view.Work.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup capture run is unavailable")
	}
	run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	ordinal := admitted.Request.SourceOrdinal
	input := admitted.Assignment.Input
	if run.TaskID != view.Work.Task.Task.ID || run.EnvironmentID != input.EnvironmentId ||
		len(run.Sources) != len(input.Steps) || uint64(ordinal) >= uint64(len(run.Sources)) ||
		(run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning) ||
		!at.After(run.UpdatedAt) {
		return workqueue.Change{}, conflict("Backup capture run differs from its active work")
	}
	source := &run.Sources[ordinal]
	capture := input.Steps[ordinal].GetCapture()
	if source.Ordinal != ordinal || capture == nil || source.RecoveryPointID != capture.PointId ||
		source.ObjectKey != capture.Target.ObjectKey {
		return workqueue.Change{}, conflict("Backup capture source differs from captured authority")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	if err := backupplanning.ValidateBackupRunStepAuthority(run, *source, input.Steps[ordinal], scope); err != nil {
		return workqueue.Change{}, err
	}
	var effectGuards []keyvalue.Condition
	if admitted.Request.GetPostgresDumpStart() != nil || admitted.Request.GetMysqlDumpStart() != nil {
		effectGuards, err = repository.captureDatabaseGuards(ctx, *source, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	if admitted.Request.GetSourceCleanupCompleted() != nil {
		verified := admitted.Assignment.Sources[ordinal].GetCapture().GetUploadVerified()
		if verified == nil || source.Object != captureObjectIdentity(verified.Object) ||
			!backupruntime.BackupArtifactEvidenceMatchesWire(source.Evidence, verified.Evidence) {
			return workqueue.Change{}, conflict("Backup Point source differs from acknowledged object verification")
		}
	}
	if err := projectCaptureProgress(source, nextCursor.GetCapture(), run, evidence); err != nil {
		return workqueue.Change{}, err
	}
	run.State, run.UpdatedAt = backupruntime.BackupRunRunning, at
	encoded, err := backupruntime.EncodeBackupRunRecord(run)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}}}
	change.Conditions = append(change.Conditions, effectGuards...)
	if admitted.Request.GetSourceCleanupCompleted() != nil {
		publication, err := repository.prepareCapturedPoint(ctx, run, ordinal, view.Work.ReadRevision, at)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, publication.Conditions...)
		change.Mutations = append(change.Mutations, publication.Mutations...)
	}
	return change, nil
}

func projectCaptureProgress(source *backupruntime.BackupRunSourceAttemptRecord,
	cursor *agentpb.BackupCaptureResume, run backupruntime.BackupRunRecord, evidence CheckpointEvidence,
) error {
	if cursor == nil {
		return conflict("Backup capture cursor is missing")
	}
	switch {
	case cursor.GetSourceCleanupCompleted() != nil:
		if source.State != backupruntime.BackupSourceAttemptStaged ||
			source.Phase != backupruntime.BackupSourcePhasePointCommit ||
			!backupruntime.BackupArtifactEvidenceMatchesWire(
				source.Evidence,
				cursor.GetSourceCleanupCompleted().Evidence,
			) {
			return conflict("Backup source cleanup differs from its verified upload")
		}
		source.State, source.Phase = backupruntime.BackupSourceAttemptPointCommitted, backupruntime.BackupSourcePhaseRetention
	case cursor.GetPostgresContainerObserved() != nil, cursor.GetMysqlContainerObserved() != nil:
		source.State = backupruntime.BackupSourceAttemptCapturing
	case cursor.GetPostgresDumpStart() != nil:
		archive, err := backupruntime.PostgresArchiveEvidenceFromWire(cursor.GetPostgresDumpStart().Archive)
		if err != nil {
			return err
		}
		source.PostgresArchive = archive
		source.State, source.Phase = backupruntime.BackupSourceAttemptReady, backupruntime.BackupSourcePhaseStaging
	case cursor.GetMysqlDumpStartCheckpoint() != nil:
		archive, err := backupruntime.MySQLArchiveEvidenceFromWire(cursor.GetMysqlDumpStartCheckpoint().Archive)
		if err != nil {
			return err
		}
		source.MySQLArchive = archive
		source.State, source.Phase = backupruntime.BackupSourceAttemptReady, backupruntime.BackupSourcePhaseStaging
	case cursor.GetConfigProgress() != nil:
		if transfer := cursor.GetConfigProgress().GetTransfer(); transfer != nil {
			archive, err := backupruntime.ConfigArchiveEvidenceFromCapture(transfer)
			if err != nil {
				return err
			}
			source.ConfigArchive = archive
			source.State, source.Phase = backupruntime.BackupSourceAttemptReady, backupruntime.BackupSourcePhaseStaging
		} else {
			source.State = backupruntime.BackupSourceAttemptCapturing
		}
	case cursor.GetArtifactPrepared() != nil:
		prepared := cursor.PreparedArtifact
		if prepared == nil {
			return conflict("Backup artifact preparation evidence is missing")
		}
		if prepared.GetVolume() != nil {
			if !evidence.VolumeReference.Valid() {
				return conflict("Volume preparation requires its durable manifest settlement")
			}
			archive, err := backupruntime.VolumeArchiveEvidenceFromWire(evidence.VolumeManifest)
			if err != nil {
				return err
			}
			archive.Manifest = evidence.VolumeReference
			source.VolumeArchive = archive
		}
		source.Evidence = backupruntime.BackupArtifactEvidence{
			SourceSizeBytes: prepared.Evidence.SourceSizeBytes, SourceSHA256: hex.EncodeToString(prepared.Evidence.SourceSha256),
			StoredSizeBytes: prepared.Evidence.StoredSizeBytes, StoredSHA256: hex.EncodeToString(prepared.Evidence.StoredSha256)}
		source.Upload = backupruntime.BackupUploadOutcome{Kind: backupruntime.BackupUploadPrepared,
			Target: backupruntime.BackupObjectTarget{ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle, ObjectKey: source.ObjectKey}}
		source.State, source.Phase = backupruntime.BackupSourceAttemptStaged, backupruntime.BackupSourcePhaseUpload
	default:
		return projectCaptureUpload(source, cursor)
	}
	return nil
}
