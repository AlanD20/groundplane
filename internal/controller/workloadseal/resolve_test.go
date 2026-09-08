package workloadseal

import (
	"context"
	"fmt"
	"strings"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type imageResolver struct {
	calls     int
	selectors []*agentpb.WorkloadImageSelector
	mutate    func(*agentpb.WorkloadImageResolutionResult)
}

func (resolver *imageResolver) ResolveWorkloadImages(
	_ context.Context,
	_ string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolver.calls++
	resolver.selectors = selectors
	values := make([]*agentpb.WorkloadImageResolution, len(selectors))
	for index, selector := range selectors {
		id := selector.GetLocalImageId()
		if id == "" {
			id = "sha256:" + strings.Repeat("a", 64)
		}
		values[index] = &agentpb.WorkloadImageResolution{Selector: proto.CloneOf(selector), LocalImageId: id}
	}
	result := &agentpb.WorkloadImageResolutionResult{
		RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{
			Success: &agentpb.WorkloadImageResolutions{Resolutions: values},
		},
	}
	if resolver.mutate != nil {
		resolver.mutate(result)
	}
	return result, nil
}

// Rationale: no partial seal or changed historical identity can escape a failed batch.
func TestResolveRejectsInvalidOrFailedResult(t *testing.T) {
	historical := domain.WorkloadSeal{
		RequestedReference: "app:old",
		LocalImageID:       "sha256:" + strings.Repeat("b", 64),
		ReplicaCount:       3,
	}
	for name, mutate := range map[string]func(*agentpb.WorkloadImageResolutionResult){
		"partial": func(result *agentpb.WorkloadImageResolutionResult) { result.GetSuccess().Resolutions = nil },
		"changed historical ID": func(result *agentpb.WorkloadImageResolutionResult) {
			result.GetSuccess().Resolutions[0].LocalImageId = "sha256:" + strings.Repeat("c", 64)
		},
		"wrong selector": func(result *agentpb.WorkloadImageResolutionResult) { result.GetSuccess().Resolutions[0].Selector = nil },
		"failure": func(result *agentpb.WorkloadImageResolutionResult) {
			ordinal := uint32(0)
			result.Outcome = &agentpb.WorkloadImageResolutionResult_Failure{Failure: &agentpb.WorkloadImageResolutionFailure{SelectorOrdinal: &ordinal, Kind: agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_NOT_FOUND}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := &imageResolver{mutate: mutate}
			seals, err := Resolve(context.Background(), resolver, "agent", []Selection{{Historical: &historical}})
			if err == nil || seals != nil {
				t.Fatal("failed batch yielded usable seals")
			}
		})
	}
}

// Rationale: one batch deduplicates image lookups without conflating replica counts
// or re-resolving historical tags that may now name a different image.
func TestResolvePreservesHistoricalSealsAndReplicaCounts(t *testing.T) {
	historical := domain.WorkloadSeal{
		RequestedReference: "app:old",
		LocalImageID:       "sha256:" + strings.Repeat("b", 64),
		ReplicaCount:       3,
	}
	resolver := &imageResolver{}
	seals, err := Resolve(context.Background(), resolver, "agent", []Selection{
		{Requested: &Requested{Reference: "app:new", Replicas: 2}},
		{Requested: &Requested{Reference: "app:new", Replicas: 4}},
		{Historical: &historical},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || len(resolver.selectors) != 2 ||
		resolver.selectors[1].GetLocalImageId() != historical.LocalImageID {
		t.Fatal("expected one unique candidate/prior batch")
	}
	if seals[0].ReplicaCount != 2 || seals[1].ReplicaCount != 4 || seals[2] != historical {
		t.Fatal("seal authority changed")
	}
}

// Rationale: invalid and oversized publications fail before contacting the Agent.
func TestResolveRejectsInvalidBatchBeforeLookup(t *testing.T) {
	oversized := make([]Selection, 65)
	for i := range oversized {
		oversized[i].Requested = &Requested{Reference: fmt.Sprintf("app:v%d", i), Replicas: 1}
	}
	for _, selections := range [][]Selection{
		oversized, {{Requested: &Requested{Reference: "app:v1"}}},
		{{Historical: &domain.WorkloadSeal{RequestedReference: "app:old", ReplicaCount: 1}}},
		{{Requested: &Requested{Reference: "app", Replicas: 1}}},
		{{}},
	} {
		resolver := &imageResolver{}
		if _, err := Resolve(context.Background(), resolver, "agent", selections); err == nil || resolver.calls != 0 {
			t.Fatal("invalid batch reached lookup")
		}
	}
}
