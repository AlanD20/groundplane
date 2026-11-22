package agent

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type monitoringReaderFunc func(context.Context, *agentpb.CollectMonitoringSamples) (*agentpb.MonitoringSamplesResult, error)

func (read monitoringReaderFunc) Collect(
	ctx context.Context,
	request *agentpb.CollectMonitoringSamples,
) (*agentpb.MonitoringSamplesResult, error) {
	return read(ctx, request)
}

// Cancellation must join the Docker reader before its slot is reusable or an
// upgrade can claim there are no remaining auxiliary operations.
func TestMonitoringCancellationJoinsReadBeforeReusingSlot(t *testing.T) {
	started, joined := make(chan struct{}), make(chan struct{})
	session := newMonitoringSession(
		monitoringReaderFunc(
			func(ctx context.Context, request *agentpb.CollectMonitoringSamples) (*agentpb.MonitoringSamplesResult, error) {
				close(started)
				<-ctx.Done()
				close(joined)
				return nil, ctx.Err()
			},
		),
	)
	request := &agentpb.CollectMonitoringSamples{
		RequestId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Targets:   serviceReadRequest().Targets,
	}
	result, err := session.start(t.Context(), request)
	if err != nil || result != nil {
		t.Fatalf("read admission: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reader never started")
	}
	if result, err := session.start(t.Context(), request); err != nil || result == nil ||
		!result.Services[0].Unavailable {
		t.Fatal("concurrent read did not remain bounded")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-joined:
	default:
		t.Fatal("Close returned before reader joined")
	}
	if session.requestID != "" {
		t.Fatal("cancelled slot remains active")
	}
}
