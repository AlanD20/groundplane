package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func mustAttachRuntimeArtifact(t *testing.T, input testattachrender.AttachTaskRenderInput) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(input.RuntimeProjection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	return artifact
}

func capturedAttachRenderInputFixture(t *testing.T) testattachrender.AttachTaskRenderInput {
	t.Helper()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	runtime := desiredTopologyProjectionFixture(t)
	consumerID := runtime.DesiredServices[0].Desired.ID
	return testattachrender.AttachTaskRenderInput{
		PlanID: ids.NewAt(ids.KindPlan, now, 1), AttachID: ids.NewAt(ids.KindAttach, now, 2),
		AttachName: "database", TenantID: ids.NewAt(ids.KindTenant, now, 3), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 4), ProjectSlug: "project",
		EnvironmentID: runtime.EnvironmentID, EnvironmentName: "main",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/main",
		BackingServiceID:    ids.NewAt(ids.KindService, now, 5),
		BackingProjectID:    ids.NewAt(ids.KindProject, now, 6), AdapterKey: "custom",
		DesiredRevisionID: runtime.RevisionID, ArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		RenderGeneration: runtime.RenderGeneration, EnvironmentEpochRevision: 1,
		RuntimeProjection: runtime, RunningServiceIDs: []string{consumerID},
		Services: testattachrender.AttachTaskServiceSnapshots(runtime.DesiredServices),
		Networks: testattachrender.AttachTaskOwnedNetworkSnapshots(runtime.DesiredZones),
		Volumes:  runtime.Volumes, VolumeMounts: runtime.VolumeMounts,
		NetworkJoins: []testattachrender.AttachTaskNetworkJoin{{
			NetworkID: ids.NewAt(ids.KindNetwork, now, 8), ServiceIDs: []string{consumerID},
		}},
		ConsumerServiceIDs:     []string{consumerID},
		ServiceDependencyPlans: runtime.ServiceDependencyPlans.Clone(),
	}
}

// AssertAttachRuntimeRoundTrip crosses capture and durable encoding in the
// Controller renderer fixture without adding a production persistence API.
func AssertAttachRuntimeRoundTrip(
	t *testing.T,
	runtime testenvironmentprojection.EnvironmentComposeProjection,
	epoch int64,
	running []string,
) {
	t.Helper()
	input := capturedAttachRenderInputFixture(t)
	input.EnvironmentID, input.DesiredRevisionID = runtime.EnvironmentID, runtime.RevisionID
	input.RenderGeneration, input.EnvironmentEpochRevision = runtime.RenderGeneration, epoch
	input.RuntimeProjection = runtime
	input.ArtifactID = mustAttachRuntimeArtifact(t, input).ArtifactId
	input.Services, input.Networks = testattachrender.AttachTaskServiceSnapshots(
		runtime.DesiredServices,
	), testattachrender.AttachTaskOwnedNetworkSnapshots(
		runtime.DesiredZones,
	)
	input.Volumes, input.VolumeMounts = runtime.Volumes, runtime.VolumeMounts
	input.ServiceDependencyPlans = runtime.ServiceDependencyPlans.Clone()
	input.ConsumerServiceIDs = []string{runtime.DesiredServices[0].Desired.ID}
	input.RunningServiceIDs = running
	input.NetworkJoins[0].ServiceIDs = input.ConsumerServiceIDs
	encoded, err := testattachrender.EncodeAttachTaskRenderInput(input)
	if err != nil {
		t.Fatal("captured Attach runtime cannot be persisted", err)
	}
	decoded, err := testattachrender.DecodeAttachTaskRenderInput(encoded)
	if err != nil || !proto.Equal(mustAttachRuntimeArtifact(t, input), mustAttachRuntimeArtifact(t, decoded)) {
		t.Fatalf("captured Attach runtime changed through persistence: %v", err)
	}
}
