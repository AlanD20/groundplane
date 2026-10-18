package agent

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DNSConnectionChecker interface {
	CheckDNSConnections(context.Context, *agentpb.CheckDNSConnections) (*agentpb.DNSConnectionResult, error)
}

func (client *Client) SetDNSConnectionChecker(checker DNSConnectionChecker) error {
	if client == nil || checker == nil {
		return errs.New(errs.KindValidationFailed, "Agent DNS connection checker is required")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.started {
		return errs.New(errs.KindStateConflict, "Agent DNS connection checker cannot change after start")
	}
	client.dnsChecks = checker
	return nil
}

type dnsCheckSession struct {
	checker   DNSConnectionChecker
	outputs   chan *agentpb.DNSConnectionResult
	requestID string
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func newDNSCheckSession(checker DNSConnectionChecker) *dnsCheckSession {
	return &dnsCheckSession{checker: checker, outputs: make(chan *agentpb.DNSConnectionResult, 1)}
}

func (session *dnsCheckSession) start(
	ctx context.Context,
	request *agentpb.CheckDNSConnections,
) (*agentpb.DNSConnectionResult, error) {
	if err := dnsconnectioncheck.ValidateRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session.checker == nil || session.requestID != "" {
		return unavailableDNSCheck(request), nil
	}
	owned := proto.CloneOf(request)
	ctx, cancel := context.WithTimeout(ctx, dnsconnectioncheck.ExchangeTimeout)
	session.requestID, session.cancel = owned.RequestId, cancel
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		defer cancel()
		result, err := session.checker.CheckDNSConnections(ctx, owned)
		if err != nil || ctx.Err() != nil || dnsconnectioncheck.ValidateResult(owned, result) != nil {
			result = unavailableDNSCheck(owned)
		} else {
			result = proto.CloneOf(result)
		}
		session.outputs <- result
	}()
	return nil, nil
}

// Join the old worker before a replacement starts. No result from the canceled
// exchange may be mistaken for the next request, even on the same connection.
func (session *dnsCheckSession) close() {
	if session.cancel != nil {
		session.cancel()
	}
	session.workers.Wait()
	session.requestID, session.cancel = "", nil
	select {
	case <-session.outputs:
	default:
	}
}

func (session *dnsCheckSession) handle(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
) (bool, error) {
	request, cancel := message.GetCheckDnsConnections(), message.GetCancelDnsConnections()
	if request == nil && cancel == nil {
		return false, nil
	}
	if proto.Size(message) > dnsconnectioncheck.MaximumRequestBytes+16 || executionplan.RejectUnknown(message) != nil {
		return true, errs.New(errs.KindValidationFailed, "DNS check envelope is invalid")
	}
	if cancel != nil {
		if err := dnsconnectioncheck.ValidateCancel(cancel); err != nil {
			return true, err
		}
		if session.requestID == cancel.RequestId {
			session.close()
		}
		return true, nil
	}
	result, err := session.start(ctx, request)
	if err != nil || result == nil {
		return true, err
	}
	return true, sendDNSCheck(ctx, stream, result)
}

func sendDNSCheck(ctx context.Context, stream agentStream, result *agentpb.DNSConnectionResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_DnsConnectionResult{DnsConnectionResult: result}},
	)
}

func unavailableDNSCheck(input *agentpb.CheckDNSConnections) *agentpb.DNSConnectionResult {
	return &agentpb.DNSConnectionResult{RequestId: input.RequestId, ComponentId: input.Action.ComponentId,
		ArtifactId: input.Action.ArtifactId, RenderGeneration: input.Action.Generation,
		ObservedAt: timestamppb.New(time.Now().UTC()), Failure: "runtime_unavailable"}
}
