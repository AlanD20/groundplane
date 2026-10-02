package backupconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConfigTransferGuard returns the native live Task, sealed procedure, claim,
// timeout and Environment-lock compares for this exact source at one MVCC view.
type ConfigTransferGuard func(context.Context, ConfigTransferOwner, int64) ([]etcdstore.Condition, error)

type ConfigRestoreTransferGuard func(context.Context, ConfigRestoreTransferOwner, int64) ([]etcdstore.Condition, error)

type ConfigTransferRepository struct {
	store        captureSnapshotStore
	guard        ConfigTransferGuard
	restoreGuard ConfigRestoreTransferGuard
}

func NewConfigTransferRepository(
	store captureSnapshotStore,
	guard ConfigTransferGuard,
	restoreGuard ConfigRestoreTransferGuard,
) *ConfigTransferRepository {
	return &ConfigTransferRepository{store: store, guard: guard, restoreGuard: restoreGuard}
}

func (repository *ConfigTransferRepository) ReadConfigTransferCursor(
	ctx context.Context,
	taskID, assignmentID, stepID, executionID string,
	revision int64,
) (etcdstore.Versioned[ConfigTransferCursor], bool, error) {
	var zero etcdstore.Versioned[ConfigTransferCursor]
	if etcdstore.ValidateContext(ctx) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		ids.Validate(ids.KindAssignment, assignmentID) != nil || ids.Validate(ids.KindStep, stepID) != nil ||
		ids.Validate(ids.KindOperation, "op_"+executionID) != nil || revision < 0 {
		return zero, false, captureSnapshotConflict()
	}
	binding := backupconfigtransfer.Binding{
		TaskID:       taskID,
		AssignmentID: assignmentID,
		StepID:       stepID,
		ExecutionID:  executionID,
	}
	key := ConfigTransferCursorKey(binding)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return zero, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 ||
		(revision > 0 && read.ReadRevision != revision) {
		return zero, false, captureSnapshotConflict()
	}
	zero.ReadRevision = read.ReadRevision
	defer etcdstore.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return zero, false, nil
	}
	cursor, err := DecodeConfigTransferCursor(value.Value)
	if err != nil || value.Key != key || value.ModRevision <= 0 ||
		ConfigTransferCursorKey(cursor.Owner.Binding) != key {
		return zero, false, captureSnapshotConflict()
	}
	return etcdstore.Versioned[ConfigTransferCursor]{
		Record:       cursor,
		Revision:     value.ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, nil
}

// CommitConfigTransferCredit atomically retains the exact accepted credit and
// moves its cursor. Replay returns the original immutable receipt; it never
// applies another grant. The channel must first match it against actually sent
// frames; this repository provides persistence and native authority fencing.
func (repository *ConfigTransferRepository) CommitConfigTransferCredit(
	ctx context.Context,
	owner ConfigTransferOwner,
	credit *agentpb.BackupConfigCredit,
) (int64, error) {
	if owner.Binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE {
		return 0, captureSnapshotConflict()
	}
	return repository.commitConfigTransferCredit(ctx, owner, credit, nil)
}

func (repository *ConfigTransferRepository) commitConfigTransferCredit(
	ctx context.Context,
	owner ConfigTransferOwner,
	credit *agentpb.BackupConfigCredit,
	restore *ConfigRestoreTransferBatch,
) (int64, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if ValidateConfigTransferOwner(owner) != nil || repository.guard == nil {
		return 0, captureSnapshotConflict()
	}
	owned, err := backupconfigtransfer.ValidateCredit(owner.Binding, credit)
	if err != nil {
		return 0, err
	}
	current, found, err := repository.ReadConfigTransferCursor(ctx, owner.Binding.TaskID, owner.Binding.AssignmentID,
		owner.Binding.StepID, owner.Binding.ExecutionID, 0)
	if err != nil {
		return 0, err
	}
	if found && current.Record.Owner != owner {
		return 0, captureSnapshotConflict()
	}
	conditions, err := repository.guard(ctx, owner, current.ReadRevision)
	if err != nil {
		return 0, err
	}
	if restore != nil {
		if repository.restoreGuard == nil || restore.Owner.Transfer != owner {
			return 0, captureSnapshotConflict()
		}
		more, err := repository.restoreGuard(ctx, restore.Owner, current.ReadRevision)
		if err != nil {
			return 0, err
		}
		conditions = append(conditions, more...)
	}
	key := ConfigTransferCreditKey(owner.Binding, owned.CreditSequence)
	keys := []string{key}
	if found {
		keys = append(keys, ConfigTransferCreditKey(owner.Binding, current.Record.LastCreditSequence))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return 0, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) {
		return 0, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	if previous := read.Values[0]; previous != nil {
		record, err := DecodeConfigTransferCredit(previous.Value)
		if err != nil || previous.Key != key || previous.Version != 1 || !found || record.Owner != owner ||
			owned.CreditSequence > current.Record.LastCreditSequence || !proto.Equal(record.Credit, owned) {
			return 0, captureSnapshotConflict()
		}
		if restore != nil {
			if err := repository.verifyConfigRestoreBatch(ctx, *restore, owned, current.ReadRevision); err != nil {
				return 0, err
			}
		}
		return previous.ModRevision, nil
	}
	next := ConfigTransferCursor{Owner: owner, LastCreditSequence: owned.CreditSequence}
	if found {
		if current.Record.LastCreditSequence == ^uint64(0) ||
			owned.CreditSequence != current.Record.LastCreditSequence+1 {
			return 0, captureSnapshotConflict()
		}
		last := read.Values[1]
		if last == nil || last.Key != keys[1] || last.Version != 1 || last.ModRevision <= 0 ||
			last.ModRevision > current.Revision {
			return 0, captureSnapshotConflict()
		}
		previous, err := DecodeConfigTransferCredit(last.Value)
		if err != nil || previous.Owner != owner ||
			previous.Credit.CreditSequence != current.Record.LastCreditSequence ||
			owned.CommittedRecordSequence <= previous.Credit.CommittedRecordSequence ||
			(current.Record.MetadataAcceptedCreditSequence != 0 && owned.NextOrdinal < previous.Credit.NextOrdinal) {
			return 0, captureSnapshotConflict()
		}
		next.MetadataAcceptedCreditSequence = current.Record.MetadataAcceptedCreditSequence
	} else if owned.CreditSequence != 1 || owned.CommittedRecordSequence != 0 || owned.GetMetadataCredit() == nil {
		return 0, captureSnapshotConflict()
	}
	if owned.GetMetadataAccepted() != nil {
		if next.MetadataAcceptedCreditSequence != 0 || owned.CommittedRecordSequence == 0 || owned.NextOrdinal != 1 {
			return 0, captureSnapshotConflict()
		}
		next.MetadataAcceptedCreditSequence = owned.CreditSequence
	} else if (owned.GetValueCredit() != nil) != (next.MetadataAcceptedCreditSequence != 0) {
		return 0, captureSnapshotConflict()
	}
	cursorValue, err := EncodeConfigTransferCursor(next)
	if err != nil {
		return 0, err
	}
	creditValue, err := EncodeConfigTransferCredit(ConfigTransferCredit{Owner: owner, Credit: owned})
	if err != nil {
		return 0, err
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: ConfigTransferCursorKey(owner.Binding), Value: cursorValue},
		{Type: etcdstore.MutationPut, Key: key, Value: creditValue},
	}
	defer etcdstore.ClearMutationValues(mutations)
	conditions = append(conditions, etcdstore.Condition{Key: mutations[0].Key, ModRevision: current.Revision},
		etcdstore.Condition{Key: key})
	if restore != nil {
		previousSequence := uint64(0)
		if found {
			previous, err := DecodeConfigTransferCredit(read.Values[1].Value)
			if err != nil {
				return 0, err
			}
			previousSequence = previous.Credit.CommittedRecordSequence
		}
		moreConditions, moreMutations, err := prepareConfigRestoreRecords(*restore, previousSequence, owned)
		if err != nil {
			return 0, err
		}
		defer etcdstore.ClearMutationValues(moreMutations)
		conditions = append(conditions, moreConditions...)
		mutations = append(mutations, moreMutations...)
		budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
		if err != nil || !budget.Fits() || budget.Bytes > MaximumBackupConfigBatchMutationBytes {
			return 0, captureSnapshotConflict()
		}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded || result.Revision <= 0 {
		return 0, captureSnapshotConflict()
	}
	return result.Revision, nil
}
