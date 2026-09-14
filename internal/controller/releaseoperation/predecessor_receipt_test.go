package releaseoperation

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: SVC-15/JOURNEY-02 ordinary recovery must bind exactly the
// acknowledged runtime selected at the planning revision, never rerender the
// historical Release after Attach or Entry changed its native bytes.
func TestOrdinaryPredecessorBindsAcknowledgedRuntimeBytes(t *testing.T) {
	current, retained := []byte("receipt-current-with-new-binding"), []byte("receipt-retained-slot")
	captured := servicelifecycle.AcknowledgedRuntimeCapture{
		Release: etcd.ServiceLifecycleRelease{
			ServingReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Current:          etcd.ReleaseRenderInput{CandidateTarget: domain.WorkloadBlue},
		},
		RuntimeRevision: 41, CurrentArtifact: current, RetainedPriorArtifact: retained,
	}
	render := &etcd.ReleaseRenderInput{
		ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", PriorTarget: domain.WorkloadBlue,
	}
	if err := bindServingRuntime(render, captured.Release.ServingReleaseID, captured); err != nil {
		t.Fatal(err)
	}
	if render.PriorRuntime == nil || !bytes.Equal(render.PriorRuntime.CurrentArtifact, current) ||
		!bytes.Equal(render.PriorRuntime.RetainedPriorArtifact, retained) {
		t.Fatal("ordinary predecessor changed acknowledged runtime bytes")
	}

	mismatched := captured
	mismatched.Release.Current.CandidateTarget = domain.WorkloadGreen
	if err := bindServingRuntime(&etcd.ReleaseRenderInput{PriorTarget: domain.WorkloadBlue},
		captured.Release.ServingReleaseID, mismatched); err == nil {
		t.Fatal("ordinary predecessor accepted a foreign acknowledged target")
	}
}
