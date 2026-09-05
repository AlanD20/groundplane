package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type blockingImageResolver struct {
	started chan struct{}
	stopped chan struct{}
}

// Rationale: outer protobuf envelope bytes also count toward the closed image
// request; ignored unknown fields must not bypass the inner request validator.
func TestClientRejectsUnknownImageEnvelope(t *testing.T) {
	message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{
		ResolveWorkloadImages: imageRequest(),
	}}
	message.ProtoReflect().SetUnknown([]byte{0xF8, 0x07, 0x01})
	stream := newFakeStream(configMessage(60, 2), message)
	client := newTestClient(t, bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes), stream)
	err := client.Run(context.Background())
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("unknown image envelope error=%v", err)
	}
}

type successfulImageResolver struct{}

func (successfulImageResolver) Resolve(
	_ context.Context,
	request *agentpb.ResolveWorkloadImages,
) (*agentpb.WorkloadImageResolutionResult, error) {
	return &agentpb.WorkloadImageResolutionResult{RequestId: request.RequestId,
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{{Selector: proto.CloneOf(request.Selectors[0]),
				LocalImageId: "sha256:" + strings.Repeat("a", 64)}},
		}},
	}, nil
}

type imageCaptureStream struct {
	agentStream
	results chan *agentpb.WorkloadImageResolutionResult
}

func (stream *imageCaptureStream) Send(message *agentpb.AgentMessage) error {
	if result := message.GetWorkloadImageResolutionResult(); result != nil {
		stream.results <- proto.CloneOf(result)
		return nil
	}
	return stream.agentStream.Send(message)
}

// Rationale: the authenticated client must dispatch and return a real image
// exchange without allocating a Task or consuming worker-pool capacity.
func TestClientResolvesWorkloadImagesOnAuthenticatedStream(t *testing.T) {
	request := imageRequest()
	stream := newFakeStream(configMessage(60, 2), &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{ResolveWorkloadImages: request},
	})
	client := newTestClient(t, bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes), stream)
	if err := client.SetWorkloadImageResolver(successfulImageResolver{}); err != nil {
		t.Fatal(err)
	}
	connect := client.connect
	results := make(chan *agentpb.WorkloadImageResolutionResult, 1)
	client.connect = func(ctx context.Context, path string) (agentStream, io.Closer, error) {
		connected, closer, err := connect(ctx, path)
		return &imageCaptureStream{agentStream: connected, results: results}, closer, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	select {
	case result := <-results:
		if err := workloadimage.ValidateResult(request, result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not return image resolution")
	}
	if err := client.SetWorkloadImageResolver(successfulImageResolver{}); err == nil {
		t.Fatal("changed resolver after client start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not stop")
	}
	sent := stream.sentMessages()
	if len(sent) != 2 || sent[0].GetAuthenticate() == nil || sent[1].GetReady().GetCapacity() != 2 {
		t.Fatalf("unexpected Task/capacity effects: %v", sent)
	}
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
