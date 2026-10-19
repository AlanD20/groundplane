//go:build agenthostacceptance

package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-89 / OBS-01/02 / SVC-01: read-only image and observation queries travel
// over the actual Client's authenticated Unix-gRPC session without allocating
// execution capacity. Resolver/observer results are supplied, not Docker state.
// Failure must produce unavailable, not a disconnected or stalled stream.
func TestNativeClientQueriesPreserveCapacityAndStream(t *testing.T) {
	for _, mode := range []string{"image", "unknown image envelope", "observation", "image busy",
		"observer failure", "malformed observation", "unconfigured observer"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeWorkersHostFixture(t, 2)
			token := bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes)
			images := &blockingImageResolver{started: make(chan struct{}), stopped: make(chan struct{})}
			results := make(chan error, 1)
			server := &nativeClientProtocolServer{
				results: results,
				connect: func(stream agentpb.AgentChannel_ConnectServer) error {
					if err := nativeClientHandshake(stream, f.workers.identity, token, 2); err != nil {
						return err
					}
					if mode == "image busy" {
						if err := stream.Send(nativeClientImageQuery()); err != nil {
							return err
						}
						select {
						case <-images.started:
						case <-time.After(3 * time.Second):
							return errs.New(errs.KindStateConflict, "image query did not enter its isolated worker")
						}
					}
					query := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ObserveServices{
						ObserveServices: serviceReadRequest()}}
					if mode == "image" || mode == "unknown image envelope" {
						query = nativeClientImageQuery()
					}
					if mode == "unknown image envelope" {
						query.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
					}
					if err := stream.Send(query); err != nil {
						return err
					}
					response, err := stream.Recv()
					if mode == "unknown image envelope" {
						if err == nil || response != nil {
							return errs.New(errs.KindStateConflict, "unknown image envelope reached the resolver")
						}
						return nil
					}
					if err != nil {
						return err
					}
					if mode == "image" {
						resolved := response.GetWorkloadImageResolutionResult()
						if err := workloadimage.ValidateResult(query.GetResolveWorkloadImages(), resolved); err != nil {
							return err
						}
						if len(resolved.GetSuccess().GetResolutions()) != 1 ||
							resolved.GetSuccess().GetResolutions()[0].LocalImageId != "sha256:"+strings.Repeat(
								"a",
								64,
							) {
							return errs.New(
								errs.KindStateConflict,
								"Client lost the requested image's resolved identity",
							)
						}
					} else if err := nativeClientCheckObservation(query.GetObserveServices(), response.GetServiceObservationResult(), mode); err != nil {
						return err
					}
					// A drained config update must remain legal: a read-only request
					// cannot leave a hidden execution slot occupied after its reply.
					if err := stream.Send(nativeClientConfig(2)); err != nil {
						return err
					}
					ready, err := stream.Recv()
					if err != nil || ready.GetReady() == nil || ready.GetReady().Capacity != 2 {
						return errs.New(
							errs.KindStateConflict,
							"query consumed execution capacity or damaged its stream",
						)
					}
					return stream.Send(
						&agentpb.ControllerMessage{
							Payload: &agentpb.ControllerMessage_Shutdown{Shutdown: &agentpb.Shutdown{}},
						},
					)
				},
			}
			client := newNativeProtocolClient(t, f, server, token)
			if mode == "image busy" {
				if err := client.SetWorkloadImageResolver(images); err != nil {
					t.Fatal(err)
				}
			} else if mode == "image" {
				if err := client.SetWorkloadImageResolver(nativeClientImageResolver{}); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "unconfigured observer" {
				if err := client.SetServiceObserver(serviceObserverFunc(func(ctx context.Context, request *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error) {
					if mode == "observer failure" {
						return nil, errs.New(errs.KindInternal, "private Docker diagnostic")
					}
					result, err := successfulServiceRead(ctx, request)
					if mode == "malformed observation" {
						result.RequestId = "wrong"
					}
					return result, err
				})); err != nil {
					t.Fatal(err)
				}
			}
			err := client.Run(t.Context())
			if mode == "unknown image envelope" {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("unknown image envelope: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := <-results; err != nil {
				t.Fatal(err)
			}
			if mode == "image busy" {
				select {
				case <-images.stopped:
				default:
					t.Fatal("Client shutdown did not join its image query")
				}
			}
		})
	}
}

func nativeClientCheckObservation(
	request *agentpb.ObserveServices,
	result *agentpb.ServiceObservationResult,
	mode string,
) error {
	if err := serviceobservation.ValidateResult(request, result); err != nil {
		return err
	}
	unavailable := mode != "observation" && mode != "image busy"
	row := result.Observations[0]
	if row.GetUnavailable() != unavailable || !unavailable && row.GetReplicas().GetHealthy() != 1 {
		return errs.New(errs.KindStateConflict, "Client did not preserve observation outcome")
	}
	return nil
}

func nativeClientImageQuery() *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{ResolveWorkloadImages: imageRequest()},
	}
}

type nativeClientImageResolver struct{}

func (nativeClientImageResolver) Resolve(
	_ context.Context,
	request *agentpb.ResolveWorkloadImages,
) (*agentpb.WorkloadImageResolutionResult, error) {
	return &agentpb.WorkloadImageResolutionResult{RequestId: request.RequestId,
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{{Selector: proto.CloneOf(request.Selectors[0]),
				LocalImageId: "sha256:" + strings.Repeat("a", 64)}},
		}}}, nil
}
