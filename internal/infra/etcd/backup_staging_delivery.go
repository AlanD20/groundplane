package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *TaskRepository) ReadBackupStagingDelivery(
	ctx context.Context,
	agentID string,
) (etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord]{}, false, err
	}
	read, err := repository.store.Get(ctx, backupruntime.BackupStagingDeliveryKey(agentID))
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord]{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord]{}, false, backupStagingConflict()
	}
	if read.Entry == nil {
		return etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord]{}, false, nil
	}
	defer clear(read.Entry.Value)
	record, err := backupruntime.DecodeBackupStagingDelivery(read.Entry.Value)
	return etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord]{
		Record:       record,
		Revision:     read.Entry.ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, err
}

// PublishBackupStagingDelivery seals decisions under the actual native Tasks.
// At most 32 distinct Task compares plus native Volume compares fit one transaction;
// the immutable claim-time index and procedure cannot change in place.
func (repository *TaskRepository) PublishBackupStagingDelivery(
	ctx context.Context,
	record backupruntime.BackupStagingDeliveryRecord,
	sources []BackupStagingSource,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if record.Ack != nil || executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan) != nil {
		return backupStagingConflict()
	}
	current, found, err := repository.ReadBackupStagingDelivery(ctx, record.AgentID)
	if err != nil {
		return err
	}
	if found && current.Record.AgentGeneration == record.AgentGeneration &&
		proto.Equal(current.Record.Inventory, record.Inventory) &&
		proto.Equal(current.Record.Plan, record.Plan) && sameBackupStagingTaskRevisions(current.Record.Tasks, sources) {
		// A retained plan may resume files only while the same native Task
		// authority still exists. A timeout cannot reuse an earlier Ready grant.
		conditions := []etcdstore.Condition{
			{Key: backupruntime.BackupStagingDeliveryKey(record.AgentID), ModRevision: current.Revision},
		}
		for _, reference := range current.Record.Tasks {
			if reference.RequiresTaskAuthority {
				conditions = append(
					conditions,
					etcdstore.Condition{
						Key:         taskjournal.TaskStorageKey(reference.TaskID),
						ModRevision: reference.Revision,
					},
				)
			}
		}
		for index, disposition := range current.Record.Plan.VolumeDispositions {
			if index >= len(current.Record.Inventory.VolumeRestores) {
				return backupStagingConflict()
			}
			source, err := repository.ReadBackupStagingSource(ctx, record.AgentID, record.AgentGeneration,
				current.Record.Inventory.VolumeRestores[index].RecoveryKeySha256)
			if err != nil || source.VolumeRestore == nil {
				return backupStagingConflict()
			}
			var revision int64
			switch {
			case disposition.GetResume() != nil:
				revision = disposition.GetResume().NativeRestoreModRevision
			case disposition.GetHold() != nil:
				revision = disposition.GetHold().NativeRestoreModRevision
			case disposition.GetCleanup() != nil:
				revision = disposition.GetCleanup().NativeRestoreModRevision
			default:
				return backupStagingConflict()
			}
			if source.VolumeRestore.Revision != revision {
				return backupStagingConflict()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: backupruntime.BackupRestoreKey(source.Task.Record.ID), ModRevision: revision})
		}
		current.Record.ProcessGeneration = record.ProcessGeneration
		return repository.writeBackupStagingDelivery(ctx, current.Record, conditions)
	}
	stageCount := len(record.Inventory.GetEntries())
	if len(sources) != stageCount+len(record.Inventory.GetVolumeRestores()) {
		return backupStagingConflict()
	}
	conditions := []etcdstore.Condition{{Key: backupruntime.BackupStagingDeliveryKey(record.AgentID)}}
	record.Tasks = nil
	seen := make(map[string]int)
	for index, source := range sources {
		key, err := source.Index.Record.RecoveryKey()
		var inventoriedKey []byte
		var volumeDisposition *agentpb.BackupVolumeRestoreRecoveryDisposition
		if index < stageCount {
			inventoriedKey = record.Inventory.Entries[index].RecoveryKeySha256
		} else {
			inventoriedKey = record.Inventory.VolumeRestores[index-stageCount].RecoveryKeySha256
			volumeDisposition = record.Plan.VolumeDispositions[index-stageCount]
		}
		if err != nil || source.Index.Revision <= 0 || source.Task.Revision <= 0 ||
			source.Index.Record.AgentID != record.AgentID ||
			source.Index.Record.AgentGeneration != record.AgentGeneration ||
			!bytes.Equal(key, inventoriedKey) ||
			source.Task.Record.ID != source.Index.Record.TaskID ||
			source.Task.Record.PlanHash != source.Index.Record.PlanSHA256 {
			return backupStagingConflict()
		}
		requiresAuthority := false
		var required *agentpb.BackupRecoveryRequired
		if index < stageCount {
			required = record.Plan.Dispositions[index].GetRecoveryRequired()
			requiresAuthority = record.Plan.Dispositions[index].GetResumePrepared() != nil || required != nil
		}
		if required != nil {
			if source.TerminalRestore == nil ||
				source.TerminalRestore.State != backupruntime.BackupRestoreRecoveryRequired ||
				required.NativeRestoreModRevision != source.Task.Revision {
				return backupStagingConflict()
			}
			digest, err := backupruntime.BackupRestoreTerminalDomainDigest(*source.TerminalRestore)
			if err != nil || digest != hex.EncodeToString(required.NativeRestoreSha256) {
				return backupStagingConflict()
			}
			// The immutable terminal Restore and its Task share this revision.
			// Keeping the Task compare retains their receipt/procedure authority
			// without duplicating compares for every recovered representation.
		}
		if volumeDisposition != nil {
			if source.VolumeRestore == nil || source.VolumeRestore.Revision <= 0 {
				return backupStagingConflict()
			}
			digest, err := backupruntime.BackupRestoreTerminalDomainDigest(source.VolumeRestore.Record)
			if err != nil {
				return err
			}
			var revision int64
			var expected []byte
			switch {
			case volumeDisposition.GetResume() != nil:
				revision = volumeDisposition.GetResume().NativeRestoreModRevision
				expected = volumeDisposition.GetResume().NativeRestoreSha256
				requiresAuthority = true
			case volumeDisposition.GetHold() != nil:
				revision = volumeDisposition.GetHold().NativeRestoreModRevision
				expected = volumeDisposition.GetHold().NativeRestoreSha256
				requiresAuthority = true
			case volumeDisposition.GetCleanup() != nil:
				revision = volumeDisposition.GetCleanup().NativeRestoreModRevision
				expected = volumeDisposition.GetCleanup().NativeRestoreSha256
				requiresAuthority = !taskjournal.IsTerminalTaskStatus(source.Task.Record.Status)
			default:
				return backupStagingConflict()
			}
			if revision != source.VolumeRestore.Revision || digest != hex.EncodeToString(expected) {
				return backupStagingConflict()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: backupruntime.BackupRestoreKey(source.Task.Record.ID), ModRevision: revision})
		}
		if referenceIndex, exists := seen[source.Task.Record.ID]; exists {
			if record.Tasks[referenceIndex].Revision != source.Task.Revision {
				return backupStagingConflict()
			}
			record.Tasks[referenceIndex].RequiresTaskAuthority = record.Tasks[referenceIndex].RequiresTaskAuthority ||
				requiresAuthority
		} else {
			seen[source.Task.Record.ID] = len(record.Tasks)
			record.Tasks = append(record.Tasks, backupruntime.BackupStagingTaskReference{
				TaskID: source.Task.Record.ID, Revision: source.Task.Revision,
				RequiresTaskAuthority: requiresAuthority,
			})
			conditions = append(conditions, etcdstore.Condition{Key: taskjournal.TaskStorageKey(source.Task.Record.ID), ModRevision: source.Task.Revision})
		}
	}
	if found {
		conditions[0].ModRevision = current.Revision
		if current.Record.AgentGeneration != record.AgentGeneration &&
			!current.Record.AllowsGenerationReplacement(record) {
			return backupStagingConflict()
		}
		if current.Record.Ack == nil && !proto.Equal(current.Record.Plan, record.Plan) {
			previous, err := executionplan.BackupStagingRecoveryPlanSHA256(current.Record.Plan)
			if err != nil || !proto.Equal(current.Record.Inventory, record.Inventory) ||
				!bytes.Equal(previous, record.Plan.SupersedesPlanSha256) {
				return backupStagingConflict()
			}
		}
	} else if len(record.Plan.SupersedesPlanSha256) != 0 {
		return backupStagingConflict()
	}
	return repository.writeBackupStagingDelivery(ctx, record, conditions)
}

func sameBackupStagingTaskRevisions(
	references []backupruntime.BackupStagingTaskReference,
	sources []BackupStagingSource,
) bool {
	for _, reference := range references {
		found := false
		for _, source := range sources {
			if source.Task.Record.ID == reference.TaskID {
				if source.Task.Revision != reference.Revision {
					return false
				}
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (repository *TaskRepository) writeBackupStagingDelivery(
	ctx context.Context,
	record backupruntime.BackupStagingDeliveryRecord,
	conditions []etcdstore.Condition,
) error {
	encoded, err := backupruntime.EncodeBackupStagingDelivery(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: conditions[0].Key, Value: encoded}},
	)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return backupStagingConflict()
	}
	return nil
}

func (repository *TaskRepository) ApplyBackupStagingDelivery(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	process [16]byte,
	ack *agentpb.BackupStagingRecoveryAck,
) (*agentpb.BackupStagingRecoveryAckReceipt, error) {
	digest, err := executionplan.BackupStagingRecoveryAckSHA256(ack)
	if err != nil {
		return nil, err
	}
	current, found, err := repository.ReadBackupStagingDelivery(ctx, agentID)
	if err != nil {
		return nil, err
	}
	if !found || current.Record.AgentGeneration != agentGeneration || current.Record.ProcessGeneration != process {
		return nil, backupStagingConflict()
	}
	next := current.Record
	if next.Ack != nil && !proto.Equal(next.Ack, ack) {
		return nil, backupStagingConflict()
	}
	next.Ack = proto.CloneOf(ack)
	encoded, err := backupruntime.EncodeBackupStagingDelivery(next)
	if err != nil {
		return nil, err
	}
	defer clear(encoded)
	if current.Record.Ack == nil {
		cleanup, err := repository.preparePostgresBackingGuardStagingCleanup(ctx, current)
		if err != nil {
			return nil, err
		}
		defer cleanup.Clear()
		conditions := []etcdstore.Condition{
			{Key: backupruntime.BackupStagingDeliveryKey(agentID), ModRevision: current.Revision},
		}
		conditions = append(conditions, cleanup.Conditions...)
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: backupruntime.BackupStagingDeliveryKey(agentID), Value: encoded},
		}
		mutations = append(mutations, cleanup.Mutations...)
		if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
			return nil, err
		}
		result, err := repository.store.Transact(
			ctx,
			conditions,
			mutations,
		)
		etcdstore.ClearValues(result.FailureReads)
		if err != nil {
			return nil, err
		}
		if !result.Succeeded {
			return nil, backupStagingConflict()
		}
	}
	return &agentpb.BackupStagingRecoveryAckReceipt{
		ProcessGeneration: append([]byte(nil), process[:]...),
		InventorySha256:   append([]byte(nil), ack.InventorySha256...),
		AppliedPlanSha256: append([]byte(nil), ack.AppliedPlanSha256...),
		RecoveryAckSha256: digest,
	}, nil
}
