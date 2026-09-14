package blueprintrelease

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: SVC-15/JOURNEY-02 Blueprint recovery must carry the exact
// acknowledged per-Service runtime and its receipt revision into publication.
func TestBlueprintNativePredecessorBindsAcknowledgedReceipt(t *testing.T) {
	releaseID, retainedID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	current, retained := []byte("receipt-current-with-entry-file"), []byte("receipt-retained-slot")
	serving := domain.Intent{
		ID: releaseID, PriorServingReleaseID: retainedID,
		Strategy: domain.StrategyBlueGreen, Slot: domain.SlotBlue,
	}
	snapshot := predecessorSnapshot{
		serving: &serving,
		native:  etcd.BlueprintNativePredecessorCapture{ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	runtime := servicelifecycle.AcknowledgedRuntimeCapture{
		Release: etcd.ServiceLifecycleRelease{
			ServingReleaseID: releaseID,
			Current:          etcd.ReleaseRenderInput{CandidateTarget: domain.WorkloadBlue},
		},
		RuntimeRevision: 71, CurrentArtifact: current, RetainedPriorArtifact: retained,
		RetainedPriorReleaseID: retainedID,
	}
	bound, err := bindNativePredecessor(snapshot, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if bound.native.RuntimeRevision != 71 || bound.native.RetainedPriorReleaseID != retainedID ||
		!bytes.Equal(bound.native.CurrentArtifact, current) ||
		!bytes.Equal(bound.native.RetainedPriorArtifact, retained) {
		t.Fatal("Blueprint predecessor changed acknowledged receipt authority")
	}

	foreign := runtime
	foreign.RetainedPriorReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if _, err := bindNativePredecessor(snapshot, foreign); err == nil {
		t.Fatal("Blueprint predecessor accepted a foreign retained receipt")
	}
}
