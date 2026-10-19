package agent

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blockingImageResolver struct {
	started chan struct{}
	stopped chan struct{}
}

func (resolver *blockingImageResolver) Resolve(
	ctx context.Context,
	_ *agentpb.ResolveWorkloadImages,
) (*agentpb.WorkloadImageResolutionResult, error) {
	close(resolver.started)
	<-ctx.Done()
	close(resolver.stopped)
	return nil, ctx.Err()
}

func imageRequest() *agentpb.ResolveWorkloadImages {
	return &agentpb.ResolveWorkloadImages{RequestId: "1234567890abcdef1234567890abcdef",
		Selectors: []*agentpb.WorkloadImageSelector{{Selector: &agentpb.WorkloadImageSelector_RequestedReference{
			RequestedReference: "app:dev",
		}}},
	}
}

// QA: SVC-01, HOST-05; in-memory image-worker admission/join only, not Docker cancellation or reconnect fencing.
// Rationale: image inspection must not occupy the stream writer or survive its
// session; a second active batch must not start another Docker worker.
func TestImageSessionBoundsAndJoinsWorker(t *testing.T) {
	resolver := &blockingImageResolver{started: make(chan struct{}), stopped: make(chan struct{})}
	session := newImageSession(context.Background(), resolver)
	defer session.close()
	if err := session.start(imageRequest()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("image worker did not start")
	}
	if err := session.start(imageRequest()); err == nil {
		t.Fatal("accepted concurrent resolution")
	}
	session.close()
	select {
	case <-resolver.stopped:
	default:
		t.Fatal("session teardown did not join resolver")
	}
}
