package executionplan

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: Caddy reload crosses the Docker authority boundary, so its sealed
// plan must bind one generated Environment Service and one exact Caddyfile
// digest without accepting a path, container id, executable, or arguments.
func TestCaddyConfigApplyPlanHasClosedIdentity(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	planID := ids.NewAt(ids.KindPlan, now, 3)
	artifactID := ids.NewAt(ids.KindConfig, now, 4)
	tenantID := ids.NewAt(ids.KindTenant, now, 6)
	projectID := ids.NewAt(ids.KindProject, now, 7)
	yaml := []byte("services:\n  caddy:\n    image: caddy:2.11.4-alpine\n")
	yamlDigest := sha256.Sum256(yaml)
	caddyfileDigest := sha256.Sum256([]byte("http:// { respond 404 }\n"))
	plan := &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, TargetId: environmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID), CanonicalYaml: yaml,
			YamlSha256:          yamlDigest[:],
			AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
			Services: []*agentpb.ComposeService{{
				ServiceId: serviceID, ComposeName: "caddy", ExpectedReplicas: 1,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.environment-id", Value: environmentID},
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: serviceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: ids.NewAt(ids.KindStep, now, 5), TimeoutSeconds: 120,
			Payload: &agentpb.ExecutionStep_CaddyConfigApply{CaddyConfigApply: &agentpb.CaddyConfigApply{
				ArtifactId: artifactID, ServiceId: serviceID, CaddyfileSha256: caddyfileDigest[:],
			}},
		}},
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	plan.Steps[0].GetCaddyConfigApply().CaddyfileSha256 = []byte("operator-selected")
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(arbitrary digest) error = nil")
	}
}
