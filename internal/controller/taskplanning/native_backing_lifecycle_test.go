package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// SVC-02/03/TASK-73: a freshly provisioned database has no Release. Lifecycle must
// preserve its original ownership and reject forged/mixed runtime authority.
func TestNativeBackingLifecyclePinsProvisionedRuntime(t *testing.T) {
	source := nativeRetainedRenderFixture(t, domain.StrategyRecreate)
	projection := source.Projection
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
	volumeDir, err := environmentpath.Derive(environmentpath.DefaultVolumeRoot, environmentpath.Scope{
		ProjectKind: environmentpath.ProjectKindBacking, ProjectID: source.ProjectID, EnvironmentID: source.EnvironmentID})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Minute)
	finished := at.Add(time.Second)
	creation := workexecution.Attempt{
		ID:         ids.New(ids.KindAttempt),
		TaskID:     ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem),
		Number:     1,
		Direction:  workexecution.Forward,
		AssignedAt: at,
		StartedAt:  &at,
		Deadline:   at.Add(time.Hour),
		Executor: workexecution.Identity{
			Executor:   workexecution.Agent,
			HostID:     ids.New(ids.KindHost),
			AgentID:    ids.New(ids.KindAgent),
			Generation: 1,
		},
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("c", 64)},
	}
	projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: ids.New(ids.KindConfig),
		ProjectOwnerKind: composerender.ComposeProjectOwnerBacking, ProjectID: source.ProjectID,
		EnvironmentID: projection.EnvironmentID, PlanID: creation.Input.ID, RenderGeneration: projection.RenderGeneration,
		AuthorizedVolumeDir: volumeDir,
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
	stageID, err := backingprovision.StageID(creation.WorkItemID, backingprovision.VerifyRuntime)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := workexecution.Checkpoint{AttemptID: creation.ID, Sequence: 8, StageID: stageID,
		Evidence: creation.Input, RecordedAt: finished}
	observation := backingruntimefact.Observation{ProjectName: artifact.ProjectName, ObservedAt: finished,
		ContainerID: strings.Repeat("d", 64), ServiceID: service.Desired.ID, ImageReference: service.Desired.Image,
		LocalImageID: "sha256:" + strings.Repeat(
			"b",
			64,
		), State: agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
		Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE, Labels: artifact.Services[0].ExpectedLabels}
	result, err := backingprovision.CompletionEvidence(creation, checkpoint, observation)
	if err != nil {
		t.Fatal(err)
	}
	creation.FinishedAt, creation.Outcome, creation.Result = &finished, workexecution.Succeeded, &result
	projection.BackingRuntime = &projectionrecord.BackingRuntimeReceipt{
		ServiceID: service.Desired.ID, ArtifactSHA256: hex.EncodeToString(digest[:]),
		LocalImageID: "sha256:" + strings.Repeat("b", 64), Adapter: "mysql",
		Attempt: creation, Checkpoint: checkpoint, Observation: observation,
		RenderGeneration: projection.RenderGeneration, AcknowledgedAt: finished,
	}
	input := releaserender.ServiceLifecycleRenderInput{
		PlanID: ids.New(ids.KindPlan), ServiceID: service.Desired.ID,
		ProjectID: source.ProjectID, ProjectSlug: source.ProjectSlug,
		EnvironmentID: source.EnvironmentID, EnvironmentName: source.EnvironmentName,
		AuthorizedVolumeDir: volumeDir, ArtifactID: artifact.ArtifactId,
		NativeBacking: &projection, AppliedRenderGeneration: projection.RenderGeneration, AppliedProjectionRevision: 20,
	}
	for _, action := range []workexecution.Action{workexecution.ActionStop, workexecution.ActionStart, workexecution.ActionDestroy} {
		capture := servicelifecyclework.Input{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
			PlanID: input.PlanID, ServiceID: input.ServiceID, ServiceRevision: 1, Action: action, HostID: creation.Executor.HostID,
			Scope: workexecution.Scope{
				ProjectID:     source.ProjectID,
				EnvironmentID: source.EnvironmentID,
			}, Runtime: &input, CreatedAt: time.Now().UTC()}
		work, err := servicelifecycle.BuildWorkInput(capture)
		if err != nil {
			t.Fatal(err)
		}
		if len(work.Members) != 1 {
			t.Fatal("Backing lifecycle selected more than its provisioned workload")
		}
		member := work.Members[0].Artifact.Services[0]
		if !proto.Equal(member, artifact.Services[0]) || work.Members[0].Ownership.SourcePlanId != creation.Input.ID ||
			work.Members[0].Ownership.SourceRenderGeneration != projection.RenderGeneration {
			t.Fatal("lifecycle changed provisioned runtime ownership or selected another workload")
		}
		if !bytes.Contains(work.Members[0].Artifact.CanonicalYaml, []byte(service.Desired.Image)) {
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
