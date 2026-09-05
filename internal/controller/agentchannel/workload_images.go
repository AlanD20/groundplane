package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type imageCommand struct {
	request *agentpb.ResolveWorkloadImages
	ctx     context.Context
	result  chan imageReply
}

type imageReply struct {
	result *agentpb.WorkloadImageResolutionResult
	err    error
}

// ResolveWorkloadImages is ephemeral machine preflight, not a Task. It captures
// one exact connection and never automatically retries on a replacement.
func (r *Registry) ResolveWorkloadImages(
	ctx context.Context,
	agentID string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "image resolution context is required")
	}
	ctx, cancel := context.WithTimeout(ctx, workloadimage.Timeout)
	defer cancel()
	r.mu.Lock()
	state := r.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) {
		r.mu.Unlock()
		return nil, imageUnavailable()
	}
	if state.activeImage != nil {
		r.mu.Unlock()
		return nil, errs.New(errs.KindWorkloadImageResolutionBusy, "Agent image resolution is busy")
	}
	id, ok := state.imageCounter.Next()
	if !ok {
		r.mu.Unlock()
		return nil, imageUnavailable()
	}
	request := &agentpb.ResolveWorkloadImages{RequestId: id, Selectors: selectors}
	if err := workloadimage.ValidateRequest(request); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	command := &imageCommand{request: proto.CloneOf(request), ctx: ctx, result: make(chan imageReply, 1)}
	state.activeImage = command
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		if state.activeImage == command {
			state.activeImage = nil
		}
		r.mu.Unlock()
	}()
	select {
	case state.imageCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, imageUnavailable()
	}
	select {
	case reply := <-command.result:
		r.mu.Lock()
		current := r.agents[agentID] == state && state.online && !state.revoked && !closed(state.done)
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !current {
			return nil, imageUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, imageUnavailable()
	}
}

func closed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func imageUnavailable() error {
	return errs.New(errs.KindWorkloadImageResolutionUnavailable, "Agent image resolution is unavailable")
}

func (s *Session) imageCommandCurrent(command *imageCommand) bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return s.registry.agents[s.agentID] == s.state && s.state.online && !s.state.revoked &&
		!closed(s.state.done) && s.state.activeImage == command && command.ctx.Err() == nil
}

func (s *Session) acceptImageResult(message *agentpb.AgentMessage) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	active := s.state.activeImage
	result := message.GetWorkloadImageResolutionResult()
	if s.registry.agents[s.agentID] != s.state || !s.state.online || s.state.revoked || closed(s.state.done) ||
		active == nil || active.ctx.Err() != nil || result == nil || result.RequestId != active.request.RequestId {
		return
	}
	err := workloadimage.ValidateResult(active.request, result)
	if proto.Size(message) > workloadimage.MaximumEnvelopeBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		err = errs.New(errs.KindValidationFailed, "invalid image resolution envelope")
	}
	reply := imageReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}
