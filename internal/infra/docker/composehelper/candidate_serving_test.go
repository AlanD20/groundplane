package composehelper

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: serving restoration consumes the exact deterministic artifact
// sealed at claim; a digest-valid identity without those bytes is insufficient.
func TestOpenServingPredecessorRequiresExactClaimSealedArtifact(t *testing.T) {
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	predecessor := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, ProjectName: "gp-serving", CanonicalYaml: []byte("services:\n  api: {}\n"),
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: releaseID}},
		}, {
			ServiceId: helperServiceID, ComposeName: "api-green", ExpectedReplicas: 1, Slot: "green",
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		}},
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(predecessor)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	request := &agentpb.ComposeHelperRequest{RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
		Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
		ServingPredecessor: &agentpb.ReleaseServingPredecessorAuthority{
			ComposeArtifact: encoded, ComposeArtifactSha256: digest[:],
		},
	}}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
		CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{ServiceId: helperServiceID},
	}}
	opened, service, openedRelease, target, err := openServingPredecessor(
		request, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
	)
	if err != nil || !proto.Equal(opened, predecessor) || service.GetServiceId() != helperServiceID ||
		openedRelease != releaseID || target != "singleton" {
		t.Fatalf("openServingPredecessor() = %#v, %#v, %q, %q, %v", opened, service, openedRelease, target, err)
	}
	request.RestorationAuthority.ServingPredecessor.ComposeArtifact[0] ^= 0xff
	if _, _, _, _, err := openServingPredecessor(
		request, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
	); err == nil {
		t.Fatal("openServingPredecessor() accepted changed artifact bytes")
	}
}

func TestServingPredecessorRejectsStoppedAndAmbiguousWorkloads(t *testing.T) {
	artifact := &agentpb.ComposeArtifact{ProjectName: "gp-serving"}
	service := &agentpb.ComposeService{ServiceId: helperServiceID, ExpectedReplicas: 1,
		ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}}
	for _, test := range []struct {
		name, containers, state string
	}{
		{name: "stopped", containers: "0123456789abcdef\n", state: `{"Running":false}`},
		{name: "ambiguous", containers: "0123456789abcdef\nfedcba9876543210\n", state: `{"Running":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if len(options.Args) >= 2 && options.Args[1] == "ls" {
					return runner.Result{Stdout: []byte(test.containers)}, nil
				}
				if len(options.Args) >= 4 && options.Args[3] == "{{json .Config.Labels}}" {
					return runner.Result{Stdout: []byte(`{"com.docker.compose.project":"gp-serving","com.groundplane.service-id":"` + helperServiceID + `","com.groundplane.release-id":"dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)}, nil
				}
				return runner.Result{Stdout: []byte(test.state)}, nil
			}
			if proven, err := servingPredecessorProven(context.Background(), fake, artifact, service); err == nil || proven {
				t.Fatalf("servingPredecessorProven() = %t, %v", proven, err)
			}
		})
	}
}
