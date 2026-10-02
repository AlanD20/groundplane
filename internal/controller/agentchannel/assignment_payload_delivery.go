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
	ctx      context.Context
	cancel   context.CancelFunc
	frames   chan *assignmentPayloadFrame
	failures chan error
	limit    int
	mu       sync.Mutex
	active   map[string]*assignmentPayloadProducer
	workers  sync.WaitGroup
}

func newAssignmentPayloadDelivery(ctx context.Context, limit int32) *assignmentPayloadDelivery {
	owned, cancel := context.WithCancel(ctx)
	return &assignmentPayloadDelivery{ctx: owned, cancel: cancel,
		frames: make(chan *assignmentPayloadFrame, int(limit)), failures: make(chan error, 1),
		limit: int(limit), active: make(map[string]*assignmentPayloadProducer)}
}

type assignmentPayloadProducer struct {
	cancel  context.CancelFunc
	credits *configTransferExchange
}

func (delivery *assignmentPayloadDelivery) start(
	stream agentpb.AgentChannel_ConnectServer,
	assignment *agentpb.TaskAssignment,
	release func(),
	send func(agentpb.AgentChannel_ConnectServer, *agentpb.TaskAssignment, *configTransferExchange) error,
) error {
	delivery.mu.Lock()
	_, exists := delivery.active[assignment.AssignmentId]
	if delivery.ctx.Err() != nil || exists || len(delivery.active) >= delivery.limit {
		delivery.mu.Unlock()
		release()
		return errs.New(errs.KindStateConflict, "assignment payload delivery is not available")
	}
	owned := proto.CloneOf(assignment)
	ctx, cancel := context.WithDeadline(delivery.ctx, owned.ExecutionDeadline.AsTime())
	credits := newConfigTransferExchange(ctx, owned)
	delivery.active[owned.AssignmentId] = &assignmentPayloadProducer{cancel: cancel, credits: credits}
	delivery.workers.Add(1)
	delivery.mu.Unlock()
	go func() {
		defer delivery.workers.Done()
		defer release()
		defer clearScriptAssignmentArtifacts(owned.ScriptArtifacts)
		defer clearBackingHookPlanSecrets(owned.Plan)
		defer func() {
			delivery.mu.Lock()
			delete(delivery.active, owned.AssignmentId)
			delivery.mu.Unlock()
		}()
		defer cancel()
		defer credits.close()
		queued := &assignmentPayloadStream{AgentChannel_ConnectServer: stream, delivery: delivery, ctx: ctx}
		if err := send(queued, owned, credits); err != nil && ctx.Err() == nil {
			select {
			case delivery.failures <- err:
			default:
				// The first failure ends this connection and preserves the durable
				// assignment. Later failures cannot replace its cause.
			}
		}
	}()
	return nil
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
	producer := delivery.active[credit.AssignmentId]
	if producer == nil {
		return errs.New(errs.KindStateConflict, "Config capture assignment is not active")
	}
	return producer.credits.accept(credit)
}

func (delivery *assignmentPayloadDelivery) acceptConfigRestoreFrame(frame *agentpb.BackupConfigTransfer) error {
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Config restore frame is missing")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	producer := delivery.active[frame.AssignmentId]
	if producer == nil {
		return errs.New(errs.KindStateConflict, "Config restore assignment is not active")
	}
	return producer.credits.acceptRestoreFrame(frame)
}

func (delivery *assignmentPayloadDelivery) acceptVolumeManifestFrame(
	frame *agentpb.BackupVolumeManifestTransfer,
) error {
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest frame is missing")
	}
	delivery.mu.Lock()
	producer := delivery.active[frame.AssignmentId]
	delivery.mu.Unlock()
	if producer == nil || producer.credits.volumes == nil {
		return errs.New(errs.KindStateConflict, "Volume manifest assignment is not active")
	}
	return producer.credits.volumes.acceptFrame(frame)
}

func (delivery *assignmentPayloadDelivery) acceptVolumeManifestCredit(
	credit *agentpb.BackupVolumeManifestAckCredit,
) error {
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest credit is missing")
	}
	delivery.mu.Lock()
	producer := delivery.active[credit.AssignmentId]
	delivery.mu.Unlock()
	if producer == nil || producer.credits.volumes == nil {
		return errs.New(errs.KindStateConflict, "Volume manifest assignment is not active")
	}
	return producer.credits.volumes.acceptCredit(credit)
}

func (delivery *assignmentPayloadDelivery) resize(limit int32) bool {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if len(delivery.active) != 0 {
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
	if chunk := message.GetBackupSecretSlotTransfer().GetChunk(); chunk != nil {
		clear(chunk.Content)
		chunk.Content = nil
	}
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
