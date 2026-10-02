package agentchannel

import (
	"bytes"
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Existing channel tests model a fresh Agent. Exercise its empty startup
// exchange through the actual repository without mixing handshake frames into
// assertions about the operation under test.
type channelStartupExchange struct {
	started bool
	pending []*agentpb.AgentMessage
}

func (exchange *channelStartupExchange) next() *agentpb.AgentMessage {
	if len(exchange.pending) == 0 {
		return nil
	}
	message := exchange.pending[0]
	exchange.pending = exchange.pending[1:]
	return message
}

func (exchange *channelStartupExchange) receive(message *agentpb.AgentMessage) (*agentpb.AgentMessage, error) {
	if message.GetReady() == nil || exchange.started {
		return message, nil
	}
	inventory := &agentpb.BackupStagingInventory{}
	digest, err := executionplan.BackupStagingInventorySHA256(inventory)
	if err != nil {
		return nil, err
	}
	planDigest, err := executionplan.BackupStagingRecoveryPlanSHA256(
		&agentpb.BackupStagingRecoveryPlan{InventorySha256: digest},
	)
	if err != nil {
		return nil, err
	}
	exchange.started = true
	exchange.pending = []*agentpb.AgentMessage{
		{
			Payload: &agentpb.AgentMessage_BackupStagingRecoveryAck{
				BackupStagingRecoveryAck: &agentpb.BackupStagingRecoveryAck{
					InventorySha256: digest, AppliedPlanSha256: planDigest,
				},
			},
		}, message,
	}
	return &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupStagingInventory{BackupStagingInventory: inventory},
	}, nil
}

func channelStartupResponse(message *agentpb.ControllerMessage) (bool, error) {
	if plan := message.GetBackupStagingRecoveryPlan(); plan != nil {
		return true, executionplan.ValidateBackupStagingRecoveryPlan(&agentpb.BackupStagingInventory{}, plan)
	}
	if receipt := message.GetBackupStagingRecoveryAckReceipt(); receipt != nil {
		digest, err := executionplan.BackupStagingRecoveryAckSHA256(&agentpb.BackupStagingRecoveryAck{
			InventorySha256: receipt.InventorySha256, AppliedPlanSha256: receipt.AppliedPlanSha256,
		})
		if err != nil || !bytes.Equal(digest, receipt.RecoveryAckSha256) {
			return true, errs.New(errs.KindInternal, "invalid fixture startup receipt")
		}
		return true, nil
	}
	return false, nil
}

type channelStartupStore struct {
	once       sync.Once
	repository *etcd.TaskRepository
	err        error
}

func (store *channelStartupStore) open() (*etcd.TaskRepository, error) {
	store.once.Do(func() { store.repository, store.err = etcd.NewTaskRepository(newChannelMemoryStore()) })
	return store.repository, store.err
}

func (store *channelStartupStore) ReadBackupStagingSource(
	ctx context.Context,
	agent string,
	generation uint64,
	key []byte,
) (etcd.BackupStagingSource, error) {
	repository, err := store.open()
	if err != nil {
		return etcd.BackupStagingSource{}, err
	}
	return repository.ReadBackupStagingSource(ctx, agent, generation, key)
}

func (store *channelStartupStore) ReadBackupStagingDelivery(
	ctx context.Context,
	agent string,
) (keyvalue.Versioned[backupruntime.BackupStagingDeliveryRecord], bool, error) {
	repository, err := store.open()
	if err != nil {
		return keyvalue.Versioned[backupruntime.BackupStagingDeliveryRecord]{}, false, err
	}
	return repository.ReadBackupStagingDelivery(ctx, agent)
}

func (store *channelStartupStore) PublishBackupStagingDelivery(
	ctx context.Context,
	record backupruntime.BackupStagingDeliveryRecord,
	sources []etcd.BackupStagingSource,
) error {
	repository, err := store.open()
	if err != nil {
		return err
	}
	return repository.PublishBackupStagingDelivery(ctx, record, sources)
}

func (store *channelStartupStore) ApplyBackupStagingDelivery(
	ctx context.Context,
	agent string,
	generation uint64,
	process [16]byte,
	ack *agentpb.BackupStagingRecoveryAck,
) (*agentpb.BackupStagingRecoveryAckReceipt, error) {
	repository, err := store.open()
	if err != nil {
		return nil, err
	}
	return repository.ApplyBackupStagingDelivery(ctx, agent, generation, process, ack)
}

func (store *channelStartupStore) GetTaskAssignment(ctx context.Context, task string) (etcd.TaskAssignment, error) {
	repository, err := store.open()
	if err != nil {
		return etcd.TaskAssignment{}, err
	}
	return repository.GetTaskAssignment(ctx, task)
}
