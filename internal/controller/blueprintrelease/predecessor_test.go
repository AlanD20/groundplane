package blueprintrelease

import (
	"context"
	"strings"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestBlueprintPredecessorPreservesHistoricalImageAndLatestAppliedArtifact(t *testing.T) {
	environmentID, serviceID := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	priorID, candidateID := ids.New(ids.KindDeployment), ids.New(ids.KindDeployment)
	historicalArtifactID, appliedArtifactID := ids.New(ids.KindConfig), ids.New(ids.KindConfig)
	old := domain.WorkloadSeal{
		RequestedReference: "app:v1",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       2,
	}
	serving := domain.Intent{
		ID:                priorID,
		EnvironmentID:     environmentID,
		ServiceID:         serviceID,
		CandidateWorkload: old,
		Strategy:          domain.StrategyRecreate,
	}
	historical := testreleaserender.ReleaseRenderInput{
		ReleaseID:         priorID,
		EnvironmentID:     environmentID,
		ServiceID:         serviceID,
		ArtifactID:        historicalArtifactID,
		CandidateWorkload: old,
		Strategy:          domain.StrategyRecreate,
		CandidateTarget:   domain.WorkloadSingleton,
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: appliedArtifactID,
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID,
		Services: []*agentpb.ComposeService{
			{
				ServiceId:        serviceID,
				Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ImageReference:   old.LocalImageID,
				ExpectedReplicas: 2,
				ExpectedLabels:   []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: priorID}},
			},
			{
				ServiceId:        serviceID,
				Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ImageReference:   "proxy:1",
				ExpectedReplicas: 1,
			},
		},
	}
	encode := func() []byte {
		value, err := proto.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	newRender := func() testreleaserender.ReleaseRenderInput {
		return testreleaserender.ReleaseRenderInput{
			ReleaseID:     candidateID,
			EnvironmentID: environmentID,
			ServiceID:     serviceID,
			CandidateWorkload: domain.WorkloadSeal{
				RequestedReference: "app:v2",
				LocalImageID:       "sha256:" + strings.Repeat("b", 64),
				ReplicaCount:       2,
			},
			Strategy:        domain.StrategyRecreate,
			CandidateTarget: domain.WorkloadSingleton,
		}
	}
	intent := domain.Intent{ID: candidateID, PriorServingReleaseID: priorID, PriorSuccessfulReleaseID: priorID}
	render := newRender()
	if err := bindPredecessor(&render, &intent, serving, historical, testenvironmentprojection.EnvironmentComposeProjection{EnvironmentID: environmentID, ComposeArtifact: encode()}); err != nil {
		t.Fatal(err)
	}
	if render.PriorWorkload == nil || *render.PriorWorkload != old || render.PriorArtifactID != appliedArtifactID ||
		render.PriorArtifactID == historicalArtifactID ||
		render.PriorTarget != domain.WorkloadSingleton ||
		render.PriorStrategy != domain.StrategyRecreate {
		t.Fatalf("incorrect predecessor: %+v", render)
	}
	if intent.PriorServingReleaseID != priorID || intent.PriorSuccessfulReleaseID != priorID ||
		render.CandidateWorkload.LocalImageID == old.LocalImageID {
		t.Fatal("candidate or predecessor identity was replaced")
	}
	// A no-candidate Apply can advance the applied artifact without creating a
	// new serving Release; restoration must use that exact artifact, not the old render ID.
	for _, field := range []string{"local image", "replicas", "release label", "history"} {
		t.Run(field, func(t *testing.T) {
			altered := proto.Clone(artifact).(*agentpb.ComposeArtifact)
			previous := historical
			switch field {
			case "local image":
				altered.Services[0].ImageReference = "sha256:" + strings.Repeat("c", 64)
			case "replicas":
				altered.Services[0].ExpectedReplicas = 1
			case "release label":
				altered.Services[0].ExpectedLabels[0].Value = candidateID
			case "history":
				previous.CandidateWorkload.LocalImageID = "sha256:" + strings.Repeat("c", 64)
			}
			value, err := proto.Marshal(altered)
			if err != nil {
				t.Fatal(err)
			}
			next := newRender()
			if err := bindPredecessor(&next, &intent, serving, previous, testenvironmentprojection.EnvironmentComposeProjection{EnvironmentID: environmentID, ComposeArtifact: value}); err == nil {
				t.Fatal("divergent predecessor was accepted")
			}
		})
	}
	t.Run("frozen proxy image", func(t *testing.T) {
		previous := historical
		previous.ProxyPorts, previous.ProxyGeneration, previous.ProxyConfigDigest = []uint16{
			8080,
		}, 7, strings.Repeat(
			"d",
			64,
		)
		previous.ProxyImage = &domain.ProxyImage{
			Repository:  "docker.io/library/caddy",
			IndexDigest: strings.Repeat("a", 64),
			Platform: componentsdk.OCIPlatform{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat("b", 64),
				ConfigDigest: strings.Repeat("c", 64),
			},
		}
		next := newRender()
		if err := bindPredecessor(&next, &intent, serving, previous, testenvironmentprojection.EnvironmentComposeProjection{EnvironmentID: environmentID, ComposeArtifact: encode()}); err != nil {
			t.Fatal(err)
		}
		if next.ProxyImage == nil || *next.ProxyImage != *previous.ProxyImage ||
			next.ProxyImage == previous.ProxyImage ||
			next.PriorProxyGeneration != 7 ||
			next.PriorProxyDigest != previous.ProxyConfigDigest {
			t.Fatal("historical proxy authority was replaced or aliased")
		}
		previous.ProxyImage = nil
		if err := bindPredecessor(&next, &intent, serving, previous, testenvironmentprojection.EnvironmentComposeProjection{EnvironmentID: environmentID, ComposeArtifact: encode()}); err == nil {
			t.Fatal("missing historical proxy image was accepted")
		}
	})
}

func TestBlueprintFirstCandidateDoesNotInventPredecessor(t *testing.T) {
	render := testreleaserender.ReleaseRenderInput{}
	intent := domain.Intent{}
	if err := (&Service{}).preparePredecessor(context.Background(), PrepareInput{}, testblueprints.EnvironmentBlueprintServiceChange{}, &render, &intent); err != nil {
		t.Fatal(err)
	}
	if render.PriorWorkload != nil || render.PriorArtifactID != "" || intent.PriorServingReleaseID != "" ||
		intent.PriorSuccessfulReleaseID != "" {
		t.Fatal("first candidate invented prior authority")
	}
}

func TestBlueprintPredecessorCaptureRejectsPostPreflightDrift(t *testing.T) {
	baseline := predecessorSnapshot{
		planning: testreleasequeries.ReleasePlanningService{ProjectionRevision: 10},
		applied: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Revision: 11, Record: testenvironmentprojection.EnvironmentComposeProjection{ComposeArtifact: []byte("captured")},
		},
		appliedPresent: true,
	}
	if err := baseline.matches(baseline.planning, baseline.applied, true); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"absent to serving", "successful", "projection revision", "applied revision", "applied bytes", "applied absence"} {
		t.Run(field, func(t *testing.T) {
			planning, applied, present := baseline.planning, baseline.applied, true
			switch field {
			case "absent to serving":
				planning.Projection.ServingReleaseID = ids.New(ids.KindDeployment)
			case "successful":
				planning.Projection.CurrentSuccessfulReleaseID = ids.New(ids.KindDeployment)
			case "projection revision":
				planning.ProjectionRevision++
			case "applied revision":
				applied.Revision++
			case "applied bytes":
				applied.Record.ComposeArtifact = []byte("changed")
			case "applied absence":
				present = false
			}
			if err := baseline.matches(planning, applied, present); err == nil {
				t.Fatal("post-preflight predecessor drift was accepted")
			}
		})
	}
}
