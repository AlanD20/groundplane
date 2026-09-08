package composeobserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Rationale: authentic labels and an unchanged OCI reference cannot prove the
// running Component when Docker reports a different actual image/config ID.
func TestObserveManagedComponentRequiresActualConfigIdentity(t *testing.T) {
	for _, repository := range []string{"docker.io/library/caddy", "docker.io/cloudflare/cloudflared"} {
		t.Run(repository, func(t *testing.T) {
			for _, actual := range []string{"sha256:" + strings.Repeat("03", 32), "sha256:" + strings.Repeat("04", 32), ""} {
				t.Run(actual, func(t *testing.T) {
					plan := managedObserverPlan(t, repository)
					artifact, service := plan.Artifacts[0], plan.Artifacts[0].Services[0]
					labels := dockerLabels(plan)
					labels[composeProjectLabel] = artifact.ProjectName
					id := strings.Repeat("a", 64)
					engine := &fakeEngine{
						containers: []container.Summary{{ID: id, Labels: labels}},
						inspects: map[string]client.ContainerInspectResult{id: {Container: container.InspectResponse{
							ID: id, Name: "/managed-1", Image: actual,
							Config: &container.Config{Image: service.ImageReference, Labels: labels},
							State: &container.State{
								Status: container.StateRunning,
								Health: &container.Health{Status: "healthy"},
							},
						}}},
					}
					observer, err := NewWithEngine(engine)
					if err != nil {
						t.Fatal(err)
					}
					observed, err := observer.Observe(context.Background(), plan, artifact.ArtifactId)
					if actual != "sha256:"+strings.Repeat("03", 32) {
						if err == nil || observed != nil {
							t.Fatalf("unproved Component returned healthy evidence: %v, %v", observed, err)
						}
						return
					}
					if err != nil || len(observed.GetContainers()) != 1 || observed.Containers[0].ImageId != actual {
						t.Fatalf("sealed Component observation: %v, %v", observed, err)
					}
				})
			}
		})
	}
}

func managedObserverPlan(t *testing.T, repository string) *agentpb.ExecutionPlan {
	t.Helper()
	plan := observerPlan(t)
	plan.PlanHash = nil
	artifact, service := plan.Artifacts[0], plan.Artifacts[0].Services[0]
	artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	artifact.OwnerId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact.ProjectName = "gp-" + strings.ToLower(artifact.OwnerId)
	artifact.AuthorizedVolumeDir = "/var/lib/groundplane/volumes/" + artifact.OwnerId
	service.OwnerComponentId = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	service.ImageRepository, service.ImageReference = repository, repository+"@sha256:"+strings.Repeat("02", 32)
	service.ImageIndexDigest, service.ImageChildDigest = bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	service.ImageConfigDigest = bytes.Repeat([]byte{3}, 32)
	service.ImageOs, service.ImageArchitecture = "linux", "amd64"
	for key, value := range map[string]string{
		"com.groundplane.component-id":        service.OwnerComponentId,
		"com.groundplane.environment-id":      artifact.OwnerId,
		"com.groundplane.image-index-digest":  "sha256:" + strings.Repeat("01", 32),
		"com.groundplane.image-child-digest":  "sha256:" + strings.Repeat("02", 32),
		"com.groundplane.image-config-digest": "sha256:" + strings.Repeat("03", 32),
		"com.groundplane.image-platform":      "linux/amd64",
	} {
		service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: value})
	}
	sort.Slice(
		service.ExpectedLabels,
		func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
	)
	artifact.CanonicalYaml = []byte("services:\n  api:\n    image: " + service.ImageReference + "\n")
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}
