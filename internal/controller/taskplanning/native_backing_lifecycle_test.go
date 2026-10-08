package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// SVC-02/03: a freshly provisioned database has no Release. Lifecycle must
// preserve its original ownership and reject forged/mixed runtime authority.
func TestNativeBackingLifecyclePinsProvisionedRuntime(t *testing.T) {
	reader, creation := blueprintPlanTestState(t)
	projection := reader.projection
	projection.Volumes = nil
	service := &projection.DesiredServices[0]
	service.Desired.Adapter, service.Desired.AdapterVersion = "mysql", "8.4"
	service.Desired.Image = "mysql:8.4@sha256:" + strings.Repeat("a", 64)
	service.BackingNetworkID = projection.DesiredZones[0].Desired.ID
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {Name: "api", Image: service.Desired.Image,
			Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}}}},
		Networks: composetypes.Networks{"frontend": {}},
	}
	var err error
	projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: ids.New(ids.KindConfig),
		ProjectOwnerKind: composerender.ComposeProjectOwnerBacking, ProjectID: reader.project.ID,
		EnvironmentID: projection.EnvironmentID, PlanID: creation.PlanID, RenderGeneration: projection.RenderGeneration,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, projection),
	})
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(projection.ComposeArtifact)
	projection.BackingRuntime = &projectionrecord.BackingRuntimeReceipt{
		ServiceID: service.Desired.ID, ArtifactSHA256: hex.EncodeToString(digest[:]),
		LocalImageID: "sha256:" + strings.Repeat("b", 64), Adapter: "mysql",
		TaskID: creation.ID, PlanID: creation.PlanID, PlanHash: strings.Repeat("c", 64),
		AgentID: ids.New(ids.KindAgent), AssignmentID: ids.New(ids.KindAssignment),
		ExecutionEpoch: 1, RenderGeneration: projection.RenderGeneration, AcknowledgedAt: creation.CreatedAt,
	}
	input := releaserender.ServiceLifecycleRenderInput{
		PlanID: ids.New(ids.KindPlan), ServiceID: service.Desired.ID,
		ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
		EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir, ArtifactID: artifact.ArtifactId,
		NativeBacking: &projection, AppliedRenderGeneration: projection.RenderGeneration, AppliedProjectionRevision: 20,
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []taskjournal.TaskType{taskjournal.TaskStop, taskjournal.TaskStart, taskjournal.TaskDestroy} {
		task := etcd.TaskRecord{ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
			PlanID: input.PlanID, Target: input.ServiceID, Type: action, Executor: taskjournal.TaskExecutorAgent,
			TimeoutSeconds: 120, Status: taskjournal.TaskStatusPending, CreatedAt: time.Now().UTC()}
		prepared, err := resolver.PrepareServiceLifecycleTask(t.Context(), task, input, []string{ids.New(ids.KindStep)})
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		plan, err := resolver.buildServiceLifecyclePlan(t.Context(), prepared, input)
		if err != nil {
			t.Fatal(err)
		}
		member := plan.Artifacts[0].Services[0]
		if len(plan.Artifacts) != 1 || len(plan.Steps) != 1 || !proto.Equal(member, artifact.Services[0]) ||
			plan.ServiceLifecycleProcedure.Sources[0].SourcePlanId != creation.PlanID ||
			plan.ServiceLifecycleProcedure.Sources[0].SourceRenderGeneration != projection.RenderGeneration {
			t.Fatal("lifecycle changed provisioned runtime ownership or selected another workload")
		}
		if !bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte(service.Desired.Image)) {
			t.Fatal("lifecycle changed the provisioned image")
		}
	}
	mixed := input
	mixed.Release = &releaserender.ServiceLifecycleRelease{}
	if _, err := releaserender.EncodeServiceLifecycleRenderInput(mixed); err == nil {
		t.Fatal("mixed runtime authority accepted")
	}
	changed := projectionrecord.CloneEnvironmentComposeProjection(projection)
	changed.BackingRuntime.ArtifactSHA256 = strings.Repeat("d", 64)
	input.NativeBacking = &changed
	if _, err := releaserender.EncodeServiceLifecycleRenderInput(input); err == nil {
		t.Fatal("changed native receipt accepted")
	}
}
