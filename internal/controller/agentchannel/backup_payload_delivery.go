package agentchannel

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// One producer owns all bulk sources of a native Backup attempt.
type backupPayloadProducer struct {
	attempt  *agentpb.WorkAttemptIdentity
	exchange *volumeManifestExchange
	configs  *configTransferExchange
	start    chan struct{}
	once     sync.Once
	cancel   context.CancelFunc
}

// Install the inbox before sending the assignment, but do not open a durable
// transfer until Connect has sent the native execution-start acknowledgement.
func (delivery *assignmentPayloadDelivery) prepareBackupPayloads(stream agentpb.AgentChannel_ConnectServer,
	assignment *agentpb.BackupWorkAssignment, authorization agentregistration.WorkAuthorization,
	send func(agentpb.AgentChannel_ConnectServer, agentregistration.WorkAuthorization, *agentpb.BackupWorkAssignment, *volumeManifestExchange, *configTransferExchange) error,
) (func(), error) {
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, authorization.Identity())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(delivery.ctx, owned.Attempt.Deadline.AsTime())
	exchange, err := newVolumeManifestExchange(ctx, owned, authorization.Identity())
	if err != nil {
		cancel()
		return nil, err
	}
	configs, err := newConfigTransferExchange(ctx, owned, authorization.Identity())
	if err != nil {
		cancel()
		exchange.close()
		return nil, err
	}
	if len(exchange.slots)+len(configs.slots)+len(configs.restores) == 0 {
		cancel()
		exchange.close()
		configs.close()
		return func() {}, nil
	}
	return delivery.launchBackupPayloads(stream, ctx, cancel, owned.Attempt, exchange, configs,
		func(stream agentpb.AgentChannel_ConnectServer) error {
			return send(stream, authorization, owned, exchange, configs)
		})
}

func (delivery *assignmentPayloadDelivery) launchBackupPayloads(stream agentpb.AgentChannel_ConnectServer,
	ctx context.Context, cancel context.CancelFunc, attempt *agentpb.WorkAttemptIdentity,
	exchange *volumeManifestExchange, configs *configTransferExchange,
	send func(agentpb.AgentChannel_ConnectServer) error,
) (func(), error) {
	closeTransfers := func() {
		exchange.close()
		if configs != nil {
			configs.close()
		}
	}
	producer := &backupPayloadProducer{attempt: attempt, exchange: exchange, configs: configs, start: make(chan struct{}), cancel: cancel}
	id := attempt.AttemptId
	delivery.mu.Lock()
	if delivery.ctx.Err() != nil || delivery.backupPayloads[id] != nil || len(delivery.active)+len(delivery.backupPayloads) >= delivery.limit {
		delivery.mu.Unlock()
		cancel()
		closeTransfers()
		return nil, errs.New(errs.KindStateConflict, "Backup payload delivery capacity is unavailable")
	}
	delivery.backupPayloads[id] = producer
	delivery.workers.Add(1)
	delivery.mu.Unlock()
	go func() {
		defer delivery.workers.Done()
		defer cancel()
		defer closeTransfers()
		defer func() { delivery.mu.Lock(); delete(delivery.backupPayloads, id); delivery.mu.Unlock() }()
		select {
		case <-ctx.Done():
			return
		case <-producer.start:
		}
		queued := &assignmentPayloadStream{AgentChannel_ConnectServer: stream, delivery: delivery, ctx: ctx}
		if err := send(queued); err != nil && ctx.Err() == nil {
			select {
			case delivery.failures <- err:
			default:
			}
		}
	}()
	return cancel, nil
}

func (delivery *assignmentPayloadDelivery) startBackupPayloads(ack *agentpb.WorkStartAck) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	producer := delivery.backupPayloads[ack.GetAttempt().GetAttemptId()]
	if producer == nil {
		return nil
	}
	if !proto.Equal(producer.attempt, ack.Attempt) || ack.StartedAt == nil || ack.StartedAt.CheckValid() != nil {
		return errs.New(errs.KindStateConflict, "Backup payload start differs from its assigned attempt")
	}
	producer.once.Do(func() { close(producer.start) })
	return nil
}

func (delivery *assignmentPayloadDelivery) finishBackupPayloads(message *agentpb.AgentMessage) {
	var attempt *agentpb.WorkAttemptIdentity
	if request := message.GetBackupWorkCompletionRequest(); request != nil {
		attempt = request.Attempt
	}
	if request := message.GetBackupWorkFailureRequest(); request != nil {
		attempt = request.Attempt
	}
	if request := message.GetBackupRecoveryRequest(); request != nil {
		attempt = request.Attempt
	}
	if attempt == nil {
		return
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if producer := delivery.backupPayloads[attempt.AttemptId]; producer != nil && proto.Equal(producer.attempt, attempt) {
		producer.cancel()
	}
}

func (server *Server) sendBackupPayloads(stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization, assignment *agentpb.BackupWorkAssignment, exchange *volumeManifestExchange, configs *configTransferExchange,
) error {
	for ordinal, step := range assignment.Input.Steps {
		if step.GetCapture().GetConfig() != nil {
			if err := server.sendBackupConfigCapture(stream, authorization, assignment, uint32(ordinal), configs); err != nil {
				return err
			}
		}
		if step.GetRestore().GetConfig() != nil {
			if err := server.receiveBackupConfigRestore(stream, authorization, assignment, uint32(ordinal), configs); err != nil {
				return err
			}
		}
		if step.GetCapture().GetVolume() != nil {
			if err := server.receiveBackupVolumeManifest(stream, authorization, assignment, uint32(ordinal), agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE, exchange); err != nil {
				return err
			}
		}
		if step.GetRestore().GetVolume() != nil {
			if err := server.sendBackupVolumeRestoreNew(stream, authorization, assignment, uint32(ordinal), exchange); err != nil {
				return err
			}
			if err := server.receiveBackupVolumeManifest(stream, authorization, assignment, uint32(ordinal), agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD, exchange); err != nil {
				return err
			}
		}
	}
	return nil
}
