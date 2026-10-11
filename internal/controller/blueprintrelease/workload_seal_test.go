package blueprintrelease

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: a pre-claim resolution cannot authorize changed candidate input
// after identity allocation or subsequent catalog reads.
func TestSealedCandidateBindsNameReferenceAndReplicaCount(t *testing.T) {
	seal := domain.WorkloadSeal{
		RequestedReference: "app:v1",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       2,
	}
	workloads := map[string]domain.WorkloadSeal{"api": seal}
	valid := core.Service{Name: "api", Image: "app:v1", Replicas: 2}
	got, err := sealedCandidate(workloads, valid)
	if err != nil || got != seal {
		t.Fatalf("matching candidate: seal=%+v error=%v", got, err)
	}
	for name, candidate := range map[string]core.Service{
		"name":      {Name: "worker", Image: "app:v1", Replicas: 2},
		"reference": {Name: "api", Image: "app:v2", Replicas: 2},
		"replicas":  {Name: "api", Image: "app:v1", Replicas: 3},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := sealedCandidate(workloads, candidate); err == nil {
				t.Fatal("changed candidate accepted old seal")
			}
		})
	}
	delete(workloads, "api")
	if _, err := sealedCandidate(workloads, valid); err == nil {
		t.Fatal("candidate without preflight accepted")
	}
}
