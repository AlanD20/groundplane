package agentchannel

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const maximumAssignmentPayloadFrameBytes = 64 << 10

type assignmentPayloadFrame struct {
	ctx     context.Context
	message *agentpb.ControllerMessage
	result  chan error
}

// assignmentPayloadDelivery has one pending frame per admitted assignment.
// Producers wait for that frame's Send result before submitting another, so
// FIFO delivery bounds memory and rotates among concurrent bulk transfers.
// Only Connect touches the underlying stream; producers cannot write to it.
type assignmentPayloadDelivery struct {
	ctx            context.Context
	cancel         context.CancelFunc
	frames         chan *assignmentPayloadFrame
	failures       chan error
	limit          int
	mu             sync.Mutex
	active         map[string]*assignmentPayloadProducer
	backupPayloads map[string]*backupPayloadProducer
	workers        sync.WaitGroup
}

func newAssignmentPayloadDelivery(ctx context.Context, limit int32) *assignmentPayloadDelivery {
	owned, cancel := context.WithCancel(ctx)
	return &assignmentPayloadDelivery{ctx: owned, cancel: cancel,
		frames: make(chan *assignmentPayloadFrame, int(limit)), failures: make(chan error, 1),
		limit: int(limit), active: make(map[string]*assignmentPayloadProducer), backupPayloads: make(map[string]*backupPayloadProducer)}
}

type assignmentPayloadProducer struct {
	cancel context.CancelFunc
}

func (delivery *assignmentPayloadDelivery) stop(assignmentID string) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if producer := delivery.active[assignmentID]; producer != nil {
		producer.cancel()
	}
}

func (delivery *assignmentPayloadDelivery) acceptConfigCredit(credit *agentpb.BackupConfigCredit) error {
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Config transfer credit is missing")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	producer := delivery.backupPayloads[credit.GetAttempt().GetAttemptId()]
	if producer == nil || producer.configs == nil {
		return errs.New(errs.KindStateConflict, "Config capture assignment is not active")
	}
	return producer.configs.accept(credit)
}

func (delivery *assignmentPayloadDelivery) acceptConfigRestoreFrame(frame *agentpb.BackupConfigTransfer) error {
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Config restore frame is missing")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	producer := delivery.backupPayloads[frame.GetAttempt().GetAttemptId()]
	if producer == nil || producer.configs == nil {
		return errs.New(errs.KindStateConflict, "Config restore assignment is not active")
	}
	return producer.configs.acceptRestoreFrame(frame)
}

func (delivery *assignmentPayloadDelivery) acceptVolumeManifestFrame(
	frame *agentpb.BackupVolumeManifestTransfer,
) error {
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest frame is missing")
	}
	delivery.mu.Lock()
	producer := delivery.volumeProducerLocked(frame.TransferId)
	delivery.mu.Unlock()
	if producer == nil {
		return errs.New(errs.KindStateConflict, "Volume manifest assignment is not active")
	}
	return producer.exchange.acceptFrame(frame)
}

func (delivery *assignmentPayloadDelivery) acceptVolumeManifestCredit(
	credit *agentpb.BackupVolumeManifestAckCredit,
) error {
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest credit is missing")
	}
	delivery.mu.Lock()
	producer := delivery.volumeProducerLocked(credit.TransferId)
	delivery.mu.Unlock()
	if producer == nil {
		return errs.New(errs.KindStateConflict, "Volume manifest assignment is not active")
	}
	return producer.exchange.acceptCredit(credit)
}

// Transfer identity belongs to the original captured source even when its
// current executor is a recovery attempt. The registered bounded inbox owns
// routing; each frame is still checked against that inbox's exact binding.
func (delivery *assignmentPayloadDelivery) volumeProducerLocked(transferID string) *backupPayloadProducer {
	var found *backupPayloadProducer
	for _, producer := range delivery.backupPayloads {
		if producer.exchange.slots[transferID] != nil {
			if found != nil {
				return nil
			}
			found = producer
		}
	}
	return found
}

func (delivery *assignmentPayloadDelivery) resize(limit int32) bool {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if len(delivery.active)+len(delivery.backupPayloads) != 0 {
		return false
	}
	delivery.limit = int(limit)
	return true
}

func (delivery *assignmentPayloadDelivery) close() {
	delivery.cancel()
	delivery.workers.Wait()
	for {
		select {
		case frame := <-delivery.frames:
			clearAssignmentPayloadFrame(frame.message)
		default:
			return
		}
	}
}

func (delivery *assignmentPayloadDelivery) deliver(
	stream agentpb.AgentChannel_ConnectServer,
	frame *assignmentPayloadFrame,
) error {
	defer clearAssignmentPayloadFrame(frame.message)
	err := frame.ctx.Err()
	if err != nil {
		frame.result <- err
		return nil
	}
	err = stream.Send(frame.message)
	frame.result <- err
	return err
}

type assignmentPayloadStream struct {
	agentpb.AgentChannel_ConnectServer
	delivery *assignmentPayloadDelivery
	ctx      context.Context
}

func (stream *assignmentPayloadStream) Context() context.Context { return stream.ctx }

func (stream *assignmentPayloadStream) Send(message *agentpb.ControllerMessage) error {
	if message == nil || proto.Size(message) > maximumAssignmentPayloadFrameBytes ||
		executionplan.RejectUnknown(message) != nil {
		return errs.New(errs.KindInternal, "assignment payload frame is invalid")
	}
	if err := stream.ctx.Err(); err != nil {
		return err
	}
	frame := &assignmentPayloadFrame{ctx: stream.ctx, message: proto.CloneOf(message), result: make(chan error, 1)}
	select {
	case <-stream.ctx.Done():
		clearAssignmentPayloadFrame(frame.message)
		return stream.ctx.Err()
	case stream.delivery.frames <- frame:
	}
	// Once queued, Connect owns the clone even when this producer is canceled.
	// The caller may clear its source buffer without racing the actual Send.
	select {
	case <-stream.ctx.Done():
		return stream.ctx.Err()
	case err := <-frame.result:
		return err
	}
}

func clearAssignmentPayloadFrame(message *agentpb.ControllerMessage) {
	if chunk := message.GetMaterializationTransfer().GetChunk(); chunk != nil {
		clear(chunk.Content)
		chunk.Content = nil
	}
	if chunk := message.GetManagedConfigTransfer().GetChunk(); chunk != nil {
		clear(chunk.Content)
		chunk.Content = nil
	}
	if chunk := message.GetBackupConfigTransfer().GetValueChunk(); chunk != nil {
		clear(chunk.Content)
		chunk.Content = nil
	}
}
