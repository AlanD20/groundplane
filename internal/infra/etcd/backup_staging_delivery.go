package etcd

import (
	"bytes"
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
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

// PublishBackupStagingDelivery seals decisions with their observed revisions.
// Volume decisions retain every native work guard; the transaction budget is
// checked without dropping guards or splitting authority across commits.
func (repository *TaskRepository) PublishBackupStagingDelivery(
	ctx context.Context,
	record backupruntime.BackupStagingDeliveryRecord,
	guards []etcdstore.Condition,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if record.Ack != nil || executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan) != nil {
		return backupStagingConflict()
	}
	var err error
	record.Guards, err = normalizeBackupStagingGuards(guards)
	if err != nil {
		return err
	}
	current, found, err := repository.ReadBackupStagingDelivery(ctx, record.AgentID)
	if err != nil {
		return err
	}
	if found && current.Record.HostID != record.HostID {
		return backupStagingConflict()
	}
	if found && current.Record.AgentGeneration == record.AgentGeneration &&
		proto.Equal(current.Record.Inventory, record.Inventory) &&
		proto.Equal(current.Record.Plan, record.Plan) && slices.Equal(current.Record.Guards, record.Guards) {
		// A retained plan may resume files only while the same native Task
		// authority still exists. A timeout cannot reuse an earlier Ready grant.
		conditions := []etcdstore.Condition{
			{Key: backupruntime.BackupStagingDeliveryKey(record.AgentID), ModRevision: current.Revision},
		}
		conditions = append(conditions, record.Guards...)
		current.Record.ProcessGeneration = record.ProcessGeneration
		return repository.writeBackupStagingDelivery(ctx, current.Record, conditions)
	}
	conditions := []etcdstore.Condition{{Key: backupruntime.BackupStagingDeliveryKey(record.AgentID)}}
	conditions = append(conditions, record.Guards...)
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
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: backupruntime.BackupStagingDeliveryKey(record.AgentID), Value: encoded}}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		return err
	}
	result, err := repository.store.Transact(
		ctx,
		conditions,
		mutations,
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
	identity workexecution.Identity,
	process [16]byte,
	ack *agentpb.BackupStagingRecoveryAck,
) (*agentpb.BackupStagingRecoveryAckReceipt, error) {
	if identity.Validate() != nil || identity.Executor != workexecution.Agent {
		return nil, backupStagingConflict()
	}
	agentID := identity.AgentID
	digest, err := executionplan.BackupStagingRecoveryAckSHA256(ack)
	if err != nil {
		return nil, err
	}
	current, found, err := repository.ReadBackupStagingDelivery(ctx, agentID)
	if err != nil {
		return nil, err
	}
	if !found || current.Record.HostID != identity.HostID || current.Record.AgentGeneration != identity.Generation || current.Record.ProcessGeneration != process {
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
		conditions = append(conditions, current.Record.Guards...)
		conditions = append(conditions, cleanup.Conditions...)
		conditions, err = normalizeBackupStagingGuards(conditions)
		if err != nil {
			return nil, err
		}
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

func normalizeBackupStagingGuards(guards []etcdstore.Condition) ([]etcdstore.Condition, error) {
	owned := append([]etcdstore.Condition(nil), guards...)
	sort.Slice(owned, func(i, j int) bool { return owned[i].Key < owned[j].Key })
	result := owned[:0]
	for _, guard := range owned {
		if guard.Key == "" || guard.ModRevision < 0 || guard.Prefix {
			return nil, backupStagingConflict()
		}
		if len(result) > 0 && result[len(result)-1].Key == guard.Key {
			if result[len(result)-1] != guard {
				return nil, backupStagingConflict()
			}
			continue
		}
		result = append(result, guard)
	}
	return result, nil
}

func backupStagingConflict() error {
	return errs.New(errs.KindStateConflict, "Backup staging native authority is unavailable or changed")
}
