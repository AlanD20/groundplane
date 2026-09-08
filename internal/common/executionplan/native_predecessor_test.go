package executionplan

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	nativeWitnessEnvironment = "env_01K4A1B2C3D4E5F6G7H8J9K0MN"
	nativeWitnessService     = "svc_01K4A1B2C3D4E5F6G7H8J9K0MP"
	nativeWitnessCandidate   = "dep_01K4A1B2C3D4E5F6G7H8J9K0MQ"
	nativeWitnessRelease     = "dep_01K4A1B2C3D4E5F6G7H8J9K0MR"
	nativeWitnessPrior       = "dep_01K4A1B2C3D4E5F6G7H8J9K0MS"
)

func TestSelectNativeRestorationMembersUsesExplicitAbsenceAndServingWitness(t *testing.T) {
	current := nativeWitnessArtifact(
		t,
		nativeWitnessRelease,
		"blue",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	retained := nativeWitnessArtifact(
		t,
		nativeWitnessPrior,
		"green",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	retained.ArtifactId = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MV"
	currentBytes := nativeWitnessBytes(t, current)
	retainedBytes := nativeWitnessBytes(t, retained)

	selected, err := SelectNativeRestorationMembers(nativeWitnessEnvironment, []CandidateServiceIdentity{{
		ServiceID: nativeWitnessService, ReleaseID: nativeWitnessCandidate,
	}}, []NativePredecessorWitness{{
		ServiceID: nativeWitnessService, CurrentArtifact: currentBytes, RetainedPriorArtifact: retainedBytes,
	}})
	if err != nil || len(selected) != 1 ||
		selected[0].Target != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
		t.Fatalf("native selection = %v, %v", selected, err)
	}

	selected, err = SelectNativeRestorationMembers(nativeWitnessEnvironment, []CandidateServiceIdentity{{
		ServiceID: nativeWitnessService, ReleaseID: nativeWitnessCandidate,
	}}, []NativePredecessorWitness{{ServiceID: nativeWitnessService}})
	if err != nil || len(selected) != 1 ||
		selected[0].Target != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
		t.Fatalf("explicit absence selection = %v, %v", selected, err)
	}
}

func TestValidateNativePredecessorWitnessRejectsForeignOrActiveRetainedRuntime(t *testing.T) {
	current := nativeWitnessArtifact(
		t,
		nativeWitnessRelease,
		"blue",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	retained := nativeWitnessArtifact(
		t,
		nativeWitnessPrior,
		"green",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ComposeArtifact, *agentpb.ComposeArtifact)
	}{
		{"foreign owner", func(a, _ *agentpb.ComposeArtifact) { a.OwnerId = "env_01K4A1B2C3D4E5F6G7H8J9K0MP" }},
		{"wrong service", func(a, _ *agentpb.ComposeArtifact) { a.Services[0].ServiceId = "svc_01K4A1B2C3D4E5F6G7H8J9K0MQ" }},
		{"retained same slot", func(_, r *agentpb.ComposeArtifact) {
			r.Services[0].Slot = "blue"
			r.Services[0].ExpectedLabels[1].Value = "blue"
		}},
		{"retained proxy", func(_, r *agentpb.ComposeArtifact) {
			r.Services = append(r.Services, &agentpb.ComposeService{ServiceId: nativeWitnessService, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, inactive := proto.CloneOf(current), proto.CloneOf(retained)
			test.mutate(candidate, inactive)
			if err := ValidateNativePredecessorWitness(nativeWitnessEnvironment, nativeWitnessService, nativeWitnessBytes(t, candidate), nativeWitnessBytes(t, inactive)); err == nil {
				t.Fatal("accepted invalid native predecessor witness")
			}
		})
	}
}

func nativeWitnessArtifact(
	t *testing.T,
	releaseID, slot string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeArtifact {
	t.Helper()
	return &agentpb.ComposeArtifact{
		ArtifactId: "cfg_01K4A1B2C3D4E5F6G7H8J9K0MT", OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: nativeWitnessEnvironment, ProjectName: "gp-" + strings.ToLower(nativeWitnessEnvironment),
		Services: []*agentpb.ComposeService{{
			ServiceId: nativeWitnessService, ComposeName: "api-" + slot, ExpectedReplicas: 1, HasHealthcheck: true,
			Role: role, Slot: slot, ImageReference: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "slot"},
				{Key: "com.groundplane.slot", Value: slot},
			},
		}},
	}
}

func nativeWitnessBytes(t *testing.T, artifact *agentpb.ComposeArtifact) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
