package composeobserver

import (
	"context"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Rationale: Docker's containerd store identifies a container's image by the
// selected manifest. Observation must verify and retain that actual local ID,
// not replace it with the manifest's different compiled configuration digest.
func TestObserveManagedContainerdChildIdentity(t *testing.T) {
	plan := managedObserverPlan(t, "docker.io/library/caddy")
	artifact, service := plan.Artifacts[0], plan.Artifacts[0].Services[0]
	labels := dockerLabels(plan)
	labels[composeProjectLabel] = artifact.ProjectName
	id := strings.Repeat("a", 64)
	child := "sha256:" + strings.Repeat("02", 32)
	engine := &fakeEngine{
		containers: []container.Summary{{ID: id, Labels: labels}},
		inspects: map[string]client.ContainerInspectResult{id: {Container: container.InspectResponse{
			ID: id, Name: "/managed-1", Image: child,
			ImageManifestDescriptor: &ocispec.Descriptor{
				Digest: digest.Digest(child), MediaType: ocispec.MediaTypeImageManifest, Size: 1917,
			},
			Config: &container.Config{Image: service.ImageReference, Labels: labels},
			State:  &container.State{Status: container.StateRunning, Health: &container.Health{Status: "healthy"}},
		}}},
	}
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := observer.Observe(context.Background(), plan, artifact.ArtifactId)
	if err != nil || len(observed.GetContainers()) != 1 || observed.Containers[0].ImageId != child {
		t.Fatalf("containerd managed image observation: %v, %v", observed, err)
	}
}
