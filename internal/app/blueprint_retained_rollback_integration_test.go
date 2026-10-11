package app

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: component-only runtime retention must keep rollback C/2 even though the
// untouched acknowledged environment still contains original Blueprint A/2.
func TestComponentOnlyRetentionAfterNativeRollback(t *testing.T) {
	testBlueprintExecutedArtifact(
		t,
		true,
		false,
		func(f *ExecutedArtifactFixture, _ *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, intent domain.Intent, _ *agentpb.ComposeArtifact) {
			current, _ := f.SeedRetainedRollback(t, original, intent)
			project := &composetypes.Project{
				Services: composetypes.Services{
					"api": {
						Name:        "api",
						Image:       original.CandidateWorkload.RequestedReference,
						NetworkMode: "none",
						Expose:      []string{"8080"},
					},
				},
			}
			artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
				Project:             project,
				ArtifactID:          ids.New(ids.KindConfig),
				ProjectOwnerKind:    testcomposerender.ComposeProjectOwnerTenant,
				TenantID:            current.TenantID,
				ProjectID:           current.ProjectID,
				EnvironmentID:       current.EnvironmentID,
				PlanID:              ids.New(ids.KindPlan),
				RenderGeneration:    5,
				AuthorizedVolumeDir: current.AuthorizedVolumeDir,
				Identities: testcomposeidentity.Snapshot{
					Services: []testcomposeidentity.Resource{
						{ID: current.ServiceID, Name: current.ServiceName},
					},
				},
			},
			)
			if err != nil {
				t.Fatal(err)
			}
			fragments := renderNativeRuntimeFixture(t, testreleaserender.ServiceLifecycleRelease{
				ServingReleaseID: current.ReleaseID,
				Current:          current,
			})
			f.SeedEntryAcknowledgedRuntime(t, current, fragments)
			acknowledged := f.AcknowledgedRuntime(t, current.ServiceID)
			mixed, err := testtaskplanning.RetainBlueprintNativeRuntimeSources(
				artifact,
				[]serviceruntimerecord.Record{acknowledged.Record},
				[]string{current.ServiceID},
			)
			if err != nil {
				t.Fatal(err)
			}
			workloadCount, proxyCount := 0, 0
			for _, member := range mixed.Services {
				if member.ServiceId != current.ServiceID {
					continue
				}
				if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
					proxyCount++
					continue
				}
				workloadCount++
				if member.ExpectedReplicas != 2 || member.ImageReference != current.CandidateWorkload.LocalImageID {
					t.Fatal("rollback sealed workload was not retained")
				}
				found := false
				for _, label := range member.ExpectedLabels {
					if label.Key == "com.groundplane.release-id" && label.Value == current.ReleaseID {
						found = true
					}
				}
				if !found {
					t.Fatal("retention resurrected original or deploy Release")
				}
			}
			if workloadCount != 1 || proxyCount != 1 {
				t.Fatalf("retained members workloads=%d proxies=%d", workloadCount, proxyCount)
			}
			assertRetainedBlueGreenRendering(t, artifact, original, current, f)
		},
	)
}

func assertRetainedBlueGreenRendering(
	t *testing.T,
	desired *agentpb.ComposeArtifact,
	prior, current testreleaserender.ReleaseRenderInput,
	fixture *ExecutedArtifactFixture,
) {
	t.Helper()
	prior.Strategy, prior.Slot, prior.CandidateTarget = domain.StrategyBlueGreen, domain.SlotBlue, domain.WorkloadBlue
	prior.CandidateWorkload.ReplicaCount = 1
	prior.Projection.DesiredServices[0].Desired.Replicas = 1
	current.Strategy, current.Slot, current.CandidateTarget = domain.StrategyBlueGreen, domain.SlotGreen, domain.WorkloadGreen
	current.CandidateWorkload.ReplicaCount = 1
	current.Projection.DesiredServices[0].Desired.Replicas = 1
	current.PriorStrategy, current.PriorSlot, current.PriorTarget = domain.StrategyBlueGreen, domain.SlotBlue, domain.WorkloadBlue
	current.PriorArtifactID = ""
	current.PriorWorkload = nil
	config, err := domain.RenderProxyConfig(
		current.ProxyImplementation,
		current.ServiceName,
		current.ReleaseID,
		current.CandidateTarget,
		current.ProxyGeneration,
		current.ProxyPorts,
	)
	if err != nil {
		t.Fatal(err)
	}
	current.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
	for _, source := range []*testreleaserender.ReleaseRenderInput{&prior, &current} {
		source.Projection.NormalizedCompose = bytes.ReplaceAll(
			source.Projection.NormalizedCompose,
			[]byte("replicas: 2"),
			[]byte("replicas: 1"),
		)
		source.Projection.NormalizedCompose = bytes.ReplaceAll(
			source.Projection.NormalizedCompose,
			[]byte("scale: 2"),
			[]byte("scale: 1"),
		)
	}
	fragments := renderNativeRuntimeFixture(
		t, testreleaserender.ServiceLifecycleRelease{
			ServingReleaseID:      current.ReleaseID,
			Current:               current,
			PriorServingReleaseID: prior.ReleaseID,
			RetainedPrior:         &prior,
		},
	)
	if len(fragments) != 2 || len(fragments[0].Services) != 2 || len(fragments[1].Services) != 1 {
		t.Fatal("sealed lifecycle renderer did not select current, inactive and one proxy")
	}
	fixture.SeedEntryAcknowledgedRuntime(t, current, fragments)
	acknowledged := fixture.AcknowledgedRuntime(t, current.ServiceID)
	mixed, err := testtaskplanning.RetainBlueprintNativeRuntimeSources(
		desired,
		[]serviceruntimerecord.Record{acknowledged.Record},
		[]string{current.ServiceID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(mixed.Services) != 3 {
		t.Fatal("multi-source merge erased a blue/green member")
	}
	found := map[string]int{}
	for _, member := range mixed.Services {
		for _, label := range member.ExpectedLabels {
			if label.Key == "com.groundplane.release-id" {
				found[label.Value]++
			}
		}
	}
	if found[current.ReleaseID] == 0 || found[prior.ReleaseID] == 0 {
		t.Fatal("blue/green sealed Release identities were not retained")
	}
}
