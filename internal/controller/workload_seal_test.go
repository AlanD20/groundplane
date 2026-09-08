package controller

import (
	"strings"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func releaseTestWorkload(reference string) domain.WorkloadSeal {
	return domain.WorkloadSeal{
		RequestedReference: reference,
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       1,
	}
}

// Rationale: changing a mutable tag or authored count after publication must
// not change the exact image or replicas rendered for a sealed Release.
func TestApplySealedWorkloadPinsImageAndReplicas(t *testing.T) {
	oldCount := 9
	oldDeploy := &composetypes.DeployConfig{Replicas: &oldCount}
	service := composetypes.ServiceConfig{Image: "app:changed", Deploy: oldDeploy, PullPolicy: "always"}
	seal := domain.WorkloadSeal{
		RequestedReference: "app:original",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       2,
	}
	if err := applySealedWorkload(&service, seal); err != nil {
		t.Fatal(err)
	}
	if service.Image != seal.LocalImageID || *service.Deploy.Replicas != 2 || service.PullPolicy != "never" {
		t.Fatalf("unsealed workload: %#v", service)
	}
	if *oldDeploy.Replicas != 9 {
		t.Fatal("mutated source projection")
	}
	seal.ReplicaCount = 0
	if applySealedWorkload(&service, seal) == nil {
		t.Fatal("defaulted missing durable count")
	}
}
