package executionplan

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: an unchanged OCI child reference cannot authorize a missing or
// changed config identity for any Environment-managed Component workload.
func TestEnvironmentComponentServiceBindsImageConfig(t *testing.T) {
	for _, repository := range []string{"docker.io/library/caddy", "docker.io/cloudflare/cloudflared"} {
		t.Run(repository, func(t *testing.T) {
			for _, mutation := range []string{"none", "missing config", "changed config"} {
				t.Run(mutation, func(t *testing.T) {
					plan := validPlan()
					artifact := plan.Artifacts[0]
					artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
					artifact.OwnerId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					service := artifact.Services[0]
					service.OwnerComponentId = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					service.ImageRepository = repository
					service.ImageIndexDigest = bytes.Repeat([]byte{1}, sha256.Size)
					service.ImageChildDigest = bytes.Repeat([]byte{2}, sha256.Size)
					service.ImageConfigDigest = bytes.Repeat([]byte{3}, sha256.Size)
					service.ImageReference = repository + "@sha256:" + strings.Repeat("02", sha256.Size)
					service.ImageOs, service.ImageArchitecture = "linux", "amd64"
					service.ExpectedLabels = append(
						service.ExpectedLabels,
						&agentpb.LabelPair{Key: labelComponentID, Value: service.OwnerComponentId},
						&agentpb.LabelPair{Key: labelEnvironmentID, Value: artifact.OwnerId},
						&agentpb.LabelPair{
							Key:   labelImageConfigDigest,
							Value: "sha256:" + strings.Repeat("03", sha256.Size),
						},
						&agentpb.LabelPair{
							Key:   labelImageChildDigest,
							Value: "sha256:" + strings.Repeat("02", sha256.Size),
						},
						&agentpb.LabelPair{
							Key:   labelImageIndexDigest,
							Value: "sha256:" + strings.Repeat("01", sha256.Size),
						},
						&agentpb.LabelPair{Key: labelImagePlatform, Value: "linux/amd64"},
					)
					sort.Slice(
						service.ExpectedLabels,
						func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
					)
					if mutation == "missing config" {
						service.ImageConfigDigest = nil
					}
					if mutation == "changed config" {
						service.ImageConfigDigest = bytes.Repeat([]byte{4}, sha256.Size)
					}
					err := validateServices(plan, artifact)
					if (err == nil) != (mutation == "none") {
						t.Fatalf("image config %s: %v", mutation, err)
					}
				})
			}
		})
	}
}
