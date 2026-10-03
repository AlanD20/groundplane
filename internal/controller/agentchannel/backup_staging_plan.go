package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backupStagingStore interface {
	ReadBackupStagingSource(context.Context, string, uint64, []byte) (etcd.BackupStagingSource, error)
	ReadBackupStagingDelivery(
		context.Context,
		string,
	) (etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord], bool, error)
	PublishBackupStagingDelivery(
		context.Context,
		backupruntime.BackupStagingDeliveryRecord,
		[]etcd.BackupStagingSource,
	) error
	ApplyBackupStagingDelivery(
		context.Context,
		string,
		uint64,
		[16]byte,
		*agentpb.BackupStagingRecoveryAck,
	) (*agentpb.BackupStagingRecoveryAckReceipt, error)
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}

func (s *Server) resolveBackupStagingPlan(ctx context.Context, store backupStagingStore, process ProcessAuthentication,
	agentID string, agentGeneration uint64, inventory *agentpb.BackupStagingInventory,
) (*agentpb.BackupStagingRecoveryPlan, error) {
	digest, err := executionplan.BackupStagingInventorySHA256(inventory)
	if err != nil {
		return nil, err
	}
	current, found, err := store.ReadBackupStagingDelivery(ctx, agentID)
	if err != nil {
		return nil, err
	}
	record := backupruntime.BackupStagingDeliveryRecord{
		AgentID:           agentID,
		AgentGeneration:   agentGeneration,
		ProcessGeneration: process.ProcessGeneration,
		Inventory:         proto.CloneOf(inventory),
		Plan:              &agentpb.BackupStagingRecoveryPlan{InventorySha256: digest},
	}
	// Always classify against current native state. Even unchanged staged bytes
	// may belong to a Task that timed out while helper inspection blocked Ack.
	sources := make([]etcd.BackupStagingSource, 0, len(inventory.Entries)+len(inventory.VolumeRestores))
	for _, entry := range inventory.Entries {
		source, err := store.ReadBackupStagingSource(ctx, agentID, agentGeneration, entry.RecoveryKeySha256)
		if err != nil {
			return nil, err
		}
		disposition, err := s.resolveBackupStagingDisposition(ctx, store, agentID, agentGeneration, entry, source)
		if err != nil {
			return nil, err
		}
		if err := attachPostgresStagingGuard(source, disposition); err != nil {
			return nil, err
		}
		sources = append(sources, source)
		record.Plan.Dispositions = append(record.Plan.Dispositions, disposition)
	}
	for _, entry := range inventory.VolumeRestores {
		source, err := store.ReadBackupStagingSource(ctx, agentID, agentGeneration, entry.RecoveryKeySha256)
		if err != nil {
			return nil, err
		}
		disposition, err := s.resolveBackupVolumeRestoreDisposition(ctx, store, agentID, agentGeneration, entry, source)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
		record.Plan.VolumeDispositions = append(record.Plan.VolumeDispositions, disposition)
	}
	if found && current.Record.AgentGeneration == agentGeneration &&
		proto.Equal(current.Record.Inventory, inventory) && current.Record.Ack == nil {
		record.Plan.SupersedesPlanSha256 = append([]byte(nil), current.Record.Plan.SupersedesPlanSha256...)
		if !proto.Equal(record.Plan, current.Record.Plan) {
			previous, err := executionplan.BackupStagingRecoveryPlanSHA256(current.Record.Plan)
			if err != nil {
				return nil, err
			}
			record.Plan.SupersedesPlanSha256 = previous
		}
	}
	if err := store.PublishBackupStagingDelivery(ctx, record, sources); err != nil {
		return nil, err
	}
	return record.Plan, nil
}

func (s *Server) resolveBackupStagingDisposition(
	ctx context.Context,
	store backupStagingStore,
	agentID string,
	agentGeneration uint64,
	entry *agentpb.BackupRecoveredStage,
	source etcd.BackupStagingSource,
) (*agentpb.BackupStagingDisposition, error) {
	task := source.Task.Record
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	if task.Type == taskjournal.TaskRestore && source.Step.GetRestore().GetConfig() != nil {
		return s.resolveConfigRestoreDisposition(ctx, store, entry, source)
	}
	if task.Type == taskjournal.TaskRestore && source.Step.GetRestore().GetVolume() != nil {
		return s.resolveVolumeRestoreStageDisposition(ctx, store, entry, source)
	}
	if task.Type == taskjournal.TaskRestore && source.Step.GetRestore().GetPostgres() != nil {
		return s.resolvePostgresRestoreStageDisposition(ctx, store, entry, source)
	}
	// A Restore may contain an irreversible target mutation; it cannot be
	// classified as a read-only Capture just because its files look alike.
	if task.Type != taskjournal.TaskBackup || source.Step.GetCapture() == nil {
		return nil, unresolvedBackupStage()
	}
	if taskjournal.IsTerminalTaskStatus(task.Status) {
		if task.Result == nil || task.Result.ReconciliationRequired || task.TerminalAssignment == nil ||
			task.TerminalAssignment.AgentID != agentID ||
			task.TerminalAssignment.AgentGeneration != source.Index.Record.AgentGeneration {
			return nil, unresolvedBackupStage()
		}
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{},
		}
		return result, nil
	}
	claim, err := store.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if claim.Task.Revision != source.Task.Revision || source.Assignment == nil ||
		claim.Assignment.Revision != source.Assignment.Revision {
		return nil, unresolvedBackupStage()
	}
	message, err := s.taskAssignmentMessage(ctx, claim, true)
	if err != nil {
		return nil, err
	}
	prepared, err := s.checkpoints.ResolveBackupStagingPrepared(
		ctx,
		agentID,
		agentGeneration,
		message.BackupAuthority,
		source.Index.Record.StepID,
	)
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		if source.Step.GetCapture().GetPostgres() != nil {
			return resolvePostgresCaptureDisposition(entry, source, message)
		}
		if source.Step.GetCapture().GetVolume() != nil {
			// Capture has not published an artifact, so no Put could have
			// begun. A partial local archive has no replayable content proof.
			return &agentpb.BackupStagingDisposition{
				RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
				Disposition: &agentpb.BackupStagingDisposition_DiscardRecovered{
					DiscardRecovered: &agentpb.BackupDiscardRecovered{}},
			}, nil
		}
		return s.resolveConfigPrefixDisposition(ctx, agentID, agentGeneration, entry, source, message)
	}
	if entry.PostgresExecution && len(entry.Files) == 0 && source.Step.GetCapture().GetPostgres() != nil &&
		postgresCaptureCleanupAuthorized(source.Step, message.BackupResume) {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	expected := []*agentpb.BackupRecoveredFile{
		{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
			SizeBytes: prepared.Evidence.SourceSizeBytes, Sha256: prepared.Evidence.SourceSha256},
	}
	if !prepared.Finals.GetSameInode() {
		expected = append(
			expected,
			&agentpb.BackupRecoveredFile{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
				SizeBytes: prepared.Evidence.StoredSizeBytes, Sha256: prepared.Evidence.StoredSha256},
		)
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: &agentpb.BackupResumePrepared{
		AssignmentResumeSha256: resumeSHA, RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH, ExpectedFiles: expected}}
	return result, nil
}

func unresolvedBackupStage() error {
	return errs.New(
		errs.KindStateConflict,
		"Backup staging needs exact prepared evidence or completed recovery before Ready",
	)
}
