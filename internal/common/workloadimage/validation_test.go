package workloadimage

import (
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const requestID = "1234567890abcdef1234567890abcdef"

func named(value string) *agentpb.WorkloadImageSelector {
	return &agentpb.WorkloadImageSelector{Selector: &agentpb.WorkloadImageSelector_RequestedReference{
		RequestedReference: value,
	}}
}

// Rationale: the 64-selector ceiling and protobuf size/unknown-field boundary
// apply to the whole request, including valid individually bounded selectors.
func TestRequestBatchCeilingAndUnknownFields(t *testing.T) {
	request := &agentpb.ResolveWorkloadImages{RequestId: requestID}
	for index := 0; index < MaximumSelectors; index++ {
		request.Selectors = append(request.Selectors, named("app:v"+strconv.Itoa(index)))
	}
	if err := ValidateRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Selectors = append(request.Selectors, named("app:overflow"))
	if ValidateRequest(request) == nil {
		t.Fatal("accepted 65 selectors")
	}
	request.Selectors = request.Selectors[:1]
	request.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
	if ValidateRequest(request) == nil {
		t.Fatal("accepted unknown request field")
	}
	request.ProtoReflect().SetUnknown(nil)
	request.Selectors[0] = named("app@sha256:" + strings.Repeat("a", 64))
	if err := ValidateRequest(request); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sha256:", "sha256:" + strings.Repeat("A", 64), "sha512:" + strings.Repeat("a", 64)} {
		request.Selectors[0] = &agentpb.WorkloadImageSelector{
			Selector: &agentpb.WorkloadImageSelector_LocalImageId{LocalImageId: id},
		}
		if ValidateRequest(request) == nil {
			t.Fatalf("accepted invalid local id %q", id)
		}
	}
}

// Rationale: both peers reject malformed or oversized selectors before Docker
// sees them, while locally built tagged images need no registry metadata.
func TestRequestBounds(t *testing.T) {
	for _, test := range []struct {
		name      string
		selectors []*agentpb.WorkloadImageSelector
		valid     bool
	}{
		{"local build", []*agentpb.WorkloadImageSelector{named("app:dev")}, true},
		{"registry port", []*agentpb.WorkloadImageSelector{named("localhost:5000/app:dev")}, true},
		{"missing tag", []*agentpb.WorkloadImageSelector{named("app")}, false},
		{"port not tag", []*agentpb.WorkloadImageSelector{named("localhost:5000/app")}, false},
		{"duplicate", []*agentpb.WorkloadImageSelector{named("app:dev"), named("app:dev")}, false},
		{"empty", nil, false},
		{"nil", []*agentpb.WorkloadImageSelector{nil}, false},
		{"oversized", []*agentpb.WorkloadImageSelector{named(strings.Repeat("a", 513) + ":dev")}, false},
		{"non ASCII", []*agentpb.WorkloadImageSelector{named("app:é")}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRequest(&agentpb.ResolveWorkloadImages{RequestId: requestID, Selectors: test.selectors})
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v error=%v", test.valid, err)
			}
		})
	}
	for _, id := range []string{"", strings.Repeat("0", 32), strings.ToUpper(requestID), requestID + "0"} {
		if ValidateRequest(
			&agentpb.ResolveWorkloadImages{
				RequestId: id,
				Selectors: []*agentpb.WorkloadImageSelector{named("app:dev")},
			},
		) == nil {
			t.Fatalf("accepted invalid correlation %q", id)
		}
	}
}

// Rationale: a response cannot change selector order, omit ordinal zero, or
// substitute another local image for an immutable historical selector.
func TestResultIdentityAndFailurePresence(t *testing.T) {
	id := "sha256:" + strings.Repeat("a", 64)
	selector := &agentpb.WorkloadImageSelector{Selector: &agentpb.WorkloadImageSelector_LocalImageId{LocalImageId: id}}
	request := &agentpb.ResolveWorkloadImages{
		RequestId: requestID,
		Selectors: []*agentpb.WorkloadImageSelector{selector},
	}
	result := &agentpb.WorkloadImageResolutionResult{RequestId: requestID,
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{{Selector: selector, LocalImageId: id}},
		}},
	}
	if err := ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*agentpb.WorkloadImageResolutionResult){
		func(r *agentpb.WorkloadImageResolutionResult) { r.RequestId = strings.Repeat("b", 32) },
		func(r *agentpb.WorkloadImageResolutionResult) { r.GetSuccess().Resolutions = nil },
		func(r *agentpb.WorkloadImageResolutionResult) {
			r.GetSuccess().Resolutions[0].LocalImageId = "sha256:" + strings.Repeat("b", 64)
		},
		func(r *agentpb.WorkloadImageResolutionResult) {
			r.GetSuccess().Resolutions[0].Selector = named("app:dev")
		},
	} {
		changed := proto.CloneOf(result)
		mutate(changed)
		if ValidateResult(request, changed) == nil {
			t.Fatal("accepted invalid resolution")
		}
	}
	failure := &agentpb.WorkloadImageResolutionFailure{
		Kind: agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_NOT_FOUND,
	}
	result.Outcome = &agentpb.WorkloadImageResolutionResult_Failure{Failure: failure}
	if ValidateResult(request, result) == nil {
		t.Fatal("accepted missing ordinal")
	}
	ordinal := uint32(0)
	failure.SelectorOrdinal = &ordinal
	if err := ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	ordinal = 1
	if ValidateResult(request, result) == nil {
		t.Fatal("accepted out-of-range ordinal")
	}
}
