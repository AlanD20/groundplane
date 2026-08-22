package executionplan

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestAuthorizeVolumeDirectoriesUsesTrustedPolicyOutsidePlanHash(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	tenantID := ids.NewAt(ids.KindTenant, at, 2)
	projectID := ids.NewAt(ids.KindProject, at, 3)
	artifactID := ids.NewAt(ids.KindConfig, at, 4)
	yaml := []byte("services: {}\n")
	yamlHash := sha256.Sum256(yaml)
	plan, err := Seal(&agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: ids.NewAt(ids.KindPlan, at, 5), RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: environmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
			CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			AuthorizedVolumeDir: "/srv/groundplane-volumes/" + tenantID + "/" + projectID + "/" + environmentID,
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: ids.NewAt(ids.KindStep, at, 6), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifactID, WholeProject: true,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := AuthorizeVolumeDirectories(plan, "/srv/groundplane-volumes"); err != nil {
		t.Fatalf("AuthorizeVolumeDirectories() error = %v", err)
	}
	if err := AuthorizeVolumeDirectories(plan, "/var/lib/groundplane/vol"); err == nil {
		t.Fatal("AuthorizeVolumeDirectories() accepted a path outside trusted policy")
	}
}
