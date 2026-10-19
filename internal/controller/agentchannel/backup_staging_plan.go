package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupStagingStore retains native startup transfer authority independently
// of the retired Task assignment/event store.
type BackupStagingStore interface {
	ReadBackupStagingDelivery(
		context.Context,
		string,
	) (etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord], bool, error)
	PublishBackupStagingDelivery(
		context.Context,
		backupruntime.BackupStagingDeliveryRecord,
		[]etcdstore.Condition,
	) error
	ApplyBackupStagingDelivery(
		context.Context,
		workexecution.Identity,
		[16]byte,
		*agentpb.BackupStagingRecoveryAck,
	) (*agentpb.BackupStagingRecoveryAckReceipt, error)
}

func (s *Server) resolveBackupStagingPlan(ctx context.Context, store BackupStagingStore, process ProcessAuthentication,
	identity workexecution.Identity, inventory *agentpb.BackupStagingInventory,
) (*agentpb.BackupStagingRecoveryPlan, error) {
	if identity.Validate() != nil || identity.Executor != workexecution.Agent {
		return nil, errs.New(errs.KindStateConflict, "Backup recovery identity does not match the authenticated Agent")
	}
	digest, err := executionplan.BackupStagingInventorySHA256(inventory)
	if err != nil {
		return nil, err
	}
	current, found, err := store.ReadBackupStagingDelivery(ctx, identity.AgentID)
	if err != nil {
		return nil, err
	}
	record := backupruntime.BackupStagingDeliveryRecord{
		HostID:            identity.HostID,
		AgentID:           identity.AgentID,
		AgentGeneration:   identity.Generation,
		ProcessGeneration: process.ProcessGeneration,
		Inventory:         proto.CloneOf(inventory),
		Plan:              &agentpb.BackupStagingRecoveryPlan{InventorySha256: digest},
	}
	// Always classify against current native state. Even unchanged staged bytes
	// may belong to a Task that timed out while helper inspection blocked Ack.
	var guards []etcdstore.Condition
	if s.backupWorkReports == nil {
		return nil, errs.New(errs.KindInternal, "native Backup recovery is not configured")
	}
	for _, entry := range inventory.Entries {
		disposition, sourceGuards, err := s.backupWorkReports.ResolveSourceRecovery(ctx, identity, entry)
		if err != nil {
			return nil, err
		}
		guards = append(guards, sourceGuards...)
		record.Plan.Dispositions = append(record.Plan.Dispositions, disposition)
	}
	for _, entry := range inventory.VolumeRestores {
		if s.backupWorkReports == nil {
			return nil, errs.New(errs.KindInternal, "native Backup recovery is not configured")
		}
		disposition, volumeGuards, err := s.backupWorkReports.ResolveVolumeRecovery(ctx, identity, entry)
		if err != nil {
			return nil, err
		}
		guards = append(guards, volumeGuards...)
		record.Plan.VolumeDispositions = append(record.Plan.VolumeDispositions, disposition)
	}
	if found && current.Record.AgentGeneration == identity.Generation &&
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
	if err := store.PublishBackupStagingDelivery(ctx, record, guards); err != nil {
		return nil, err
	}
	return record.Plan, nil
}
