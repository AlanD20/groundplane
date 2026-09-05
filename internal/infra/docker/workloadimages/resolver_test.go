package workloadimages

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
)

type imageEngine struct {
	calls    []string
	id       string
	deadline bool
	err      error
}

func (engine *imageEngine) ImageInspect(
	ctx context.Context,
	selector string,
	_ ...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	engine.calls = append(engine.calls, selector)
	deadline, present := ctx.Deadline()
	engine.deadline = present && time.Until(deadline) <= workloadimage.Timeout
	return client.ImageInspectResult{InspectResponse: image.InspectResponse{ID: engine.id}}, engine.err
}

// Rationale: failures expose only the closed ordinal/kind, not Docker details;
// a cancelled request must not inspect Docker or return usable image identity.
func TestResolveObservationFailuresAndCancellation(t *testing.T) {
	request := &agentpb.ResolveWorkloadImages{RequestId: "1234567890abcdef1234567890abcdef",
		Selectors: []*agentpb.WorkloadImageSelector{{Selector: &agentpb.WorkloadImageSelector_RequestedReference{
			RequestedReference: "app:dev",
		}}},
	}
	for _, test := range []struct {
		err  error
		kind agentpb.WorkloadImageResolutionFailureKind
	}{
		{errdefs.ErrNotFound, agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_NOT_FOUND},
		{errors.New("private Docker diagnostic"), agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_OBSERVATION_FAILED},
		{nil, agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_OBSERVATION_FAILED},
	} {
		engine := &imageEngine{err: test.err}
		resolver, err := New(engine)
		if err != nil {
			t.Fatal(err)
		}
		result, err := resolver.Resolve(context.Background(), request)
		if err != nil || workloadimage.ValidateResult(request, result) != nil ||
			result.GetFailure().GetKind() != test.kind {
			t.Fatalf("failure result=%v error=%v", result, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err = resolver.Resolve(ctx, request)
		if !errors.Is(err, context.Canceled) || result != nil || len(engine.calls) != 1 {
			t.Fatalf("cancellation result=%v error=%v calls=%v", result, err, engine.calls)
		}
	}
}

// Rationale: local builds have no RepoDigests; immutable-id checks must inspect
// the supplied id itself and fail closed if Docker returns different bytes.
func TestResolveLocalBuildAndHistoricalIdentity(t *testing.T) {
	id := "sha256:" + strings.Repeat("a", 64)
	engine := &imageEngine{id: id}
	resolver, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.ResolveWorkloadImages{RequestId: "1234567890abcdef1234567890abcdef",
		Selectors: []*agentpb.WorkloadImageSelector{
			{Selector: &agentpb.WorkloadImageSelector_RequestedReference{RequestedReference: "app:dev"}},
			{Selector: &agentpb.WorkloadImageSelector_LocalImageId{LocalImageId: id}},
		},
	}
	result, err := resolver.Resolve(context.Background(), request)
	if err != nil || workloadimage.ValidateResult(request, result) != nil || result.GetSuccess() == nil {
		t.Fatalf("resolve=%v error=%v", result, err)
	}
	if len(engine.calls) != 2 || engine.calls[0] != "app:dev" || engine.calls[1] != id || !engine.deadline {
		t.Fatalf("calls=%v bounded=%v", engine.calls, engine.deadline)
	}
	engine.id = "sha256:" + strings.Repeat("b", 64)
	result, err = resolver.Resolve(context.Background(), request)
	if err != nil || result.GetFailure().GetSelectorOrdinal() != 1 || result.GetSuccess() != nil ||
		result.GetFailure().
			GetKind() !=
			agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_IDENTITY_MISMATCH {
		t.Fatalf("mismatch=%v error=%v", result, err)
	}
}

// Rationale: validate the complete batch before inspecting even its first image.
func TestResolveRejectsInvalidBatchBeforeDocker(t *testing.T) {
	engine := &imageEngine{}
	resolver, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.ResolveWorkloadImages{RequestId: "1234567890abcdef1234567890abcdef",
		Selectors: []*agentpb.WorkloadImageSelector{
			{Selector: &agentpb.WorkloadImageSelector_RequestedReference{RequestedReference: "app:dev"}}, nil,
		},
	}
	if _, err := resolver.Resolve(context.Background(), request); err == nil || len(engine.calls) != 0 {
		t.Fatalf("invalid batch error=%v calls=%v", err, engine.calls)
	}
}
