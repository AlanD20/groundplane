package checkpointmailbox

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"sync"
)

type volumeCheckpointWaiter struct {
	request   *agentpb.VolumeRemovalCheckpointRequest
	ack       chan *agentpb.VolumeRemovalCheckpointAck
	abandoned bool
}

type VolumeInbox struct {
	mu      sync.Mutex
	pending map[string]*volumeCheckpointWaiter
}

func NewVolumeInbox() *VolumeInbox {
	return &VolumeInbox{pending: make(map[string]*volumeCheckpointWaiter)}
}

// Register owns correlation state for an already validated checkpoint request.
func (inbox *VolumeInbox) Register(
	owned *agentpb.VolumeRemovalCheckpointRequest,
) (<-chan *agentpb.VolumeRemovalCheckpointAck, func(), error) {
	waiter := &volumeCheckpointWaiter{request: owned, ack: make(chan *agentpb.VolumeRemovalCheckpointAck, 1)}
	inbox.mu.Lock()
	if inbox.pending[owned.RequestId] != nil {
		inbox.mu.Unlock()
		return nil, nil, errs.New(errs.KindStateConflict, "agent: Volume checkpoint is already pending")
	}
	inbox.pending[owned.RequestId] = waiter
	inbox.mu.Unlock()
	return waiter.ack, func() {
		inbox.mu.Lock()
		waiter.abandoned = true
		inbox.mu.Unlock()
	}, nil
}
func (inbox *VolumeInbox) Accept(ack *agentpb.VolumeRemovalCheckpointAck) error {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	waiter := inbox.pending[ack.RequestId]
	if waiter == nil {
		return errs.New(errs.KindStateConflict, "agent: unexpected Volume checkpoint acknowledgement")
	}
	if err := executionplan.ValidateVolumeRemovalCheckpointAck(ack, waiter.request); err != nil {
		return err
	}
	delete(inbox.pending, ack.RequestId)
	if !waiter.abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.VolumeRemovalCheckpointAck)
	}
	return nil
}
