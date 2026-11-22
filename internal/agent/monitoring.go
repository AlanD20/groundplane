package agent

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoringwire"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// MonitoringReader reads the frozen serving targets without Task authority.
type MonitoringReader interface {
	Collect(context.Context, *agentpb.CollectMonitoringSamples) (*agentpb.MonitoringSamplesResult, error)
}

func (c *Client) SetMonitoringReader(observer MonitoringReader) error {
	if c == nil || observer == nil {
		return errs.New(errs.KindValidationFailed, "agent: monitoring reader is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errs.New(errs.KindStateConflict, "agent: monitoring reader cannot change after start")
	}
	c.monitoring = observer
	return nil
}

// The stream loop alone owns admission and writes. Close and explicit cancel
// join the read before its slot can be reused; the worker never touches a Task.
type monitoringSession struct {
	observer  MonitoringReader
	outputs   chan *agentpb.MonitoringSamplesResult
	requestID string
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func newMonitoringSession(observer MonitoringReader) *monitoringSession {
	return &monitoringSession{observer: observer, outputs: make(chan *agentpb.MonitoringSamplesResult, 1)}
}

func (session *monitoringSession) start(
	ctx context.Context, request *agentpb.CollectMonitoringSamples,
) (*agentpb.MonitoringSamplesResult, error) {
	if err := monitoringwire.ValidateRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session.requestID != "" || session.observer == nil {
		return unavailableMonitoring(request), nil
	}
	owned := proto.CloneOf(request)
	ctx, cancel := context.WithTimeout(ctx, monitoringwire.Timeout)
	session.requestID, session.cancel = owned.RequestId, cancel
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		defer cancel()
		result, err := session.observer.Collect(ctx, owned)
		if err != nil || ctx.Err() != nil || monitoringwire.ValidateResult(owned, result) != nil {
			result = unavailableMonitoring(owned)
		} else {
			// The stream takes ownership after the observer call returns.
			result = proto.CloneOf(result)
		}
		session.outputs <- result // One admitted worker, one buffered output.
	}()
	return nil, nil
}

func (session *monitoringSession) Close() error {
	if session.cancel != nil {
		session.cancel()
	}
	session.workers.Wait()
	session.requestID, session.cancel = "", nil
	select {
	case <-session.outputs:
	default:
	}
	return nil
}

func unavailableMonitoring(request *agentpb.CollectMonitoringSamples) *agentpb.MonitoringSamplesResult {
	result := &agentpb.MonitoringSamplesResult{RequestId: request.RequestId}
	for _, target := range request.Targets {
		result.Services = append(
			result.Services,
			&agentpb.MonitoringServiceSample{
				ServiceId:   target.ServiceId,
				ReleaseId:   target.ReleaseId,
				Unavailable: true,
			},
		)
	}
	for _, target := range request.Volumes {
		result.Volumes = append(
			result.Volumes,
			&agentpb.MonitoringVolumeSample{VolumeId: target.VolumeId, ObservedAtUnixNano: time.Now().UnixNano()},
		)
	}
	return result
}

func (session *monitoringSession) handle(
	ctx context.Context, stream agentStream, message *agentpb.ControllerMessage,
) (bool, error) {
	request, cancel := message.GetCollectMonitoringSamples(), message.GetCancelMonitoringSamples()
	if request == nil && cancel == nil {
		return false, nil
	}
	if proto.Size(message) > monitoringwire.MaximumRequestBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		return true, errs.New(errs.KindValidationFailed, "agent: invalid monitoring sampling envelope")
	}
	if cancel != nil {
		if err := monitoringwire.ValidateCancel(cancel); err != nil {
			return true, err
		}
		if session.requestID == cancel.RequestId {
			return true, session.Close()
		}
		return true, nil
	}
	result, err := session.start(ctx, request)
	if err != nil || result == nil {
		return true, err
	}
	return true, sendMonitoringSamples(ctx, stream, result)
}

func sendMonitoringSamples(ctx context.Context, stream agentStream, result *agentpb.MonitoringSamplesResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_MonitoringSamplesResult{
		MonitoringSamplesResult: result,
	}})
}
