package agent

import (
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	strings "strings"
	testing "testing"
)

const (
	nativeAssignmentPriorArtifact    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	nativeAssignmentRetainedArtifact = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	nativeAssignmentPriorRelease     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	nativeAssignmentRetainedRelease  = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

// Rationale: native authority is closed per candidate. Missing, partial,
// foreign, duplicate, mismatched, and malformed witness data cannot acquire
// serving restoration authority.
func TestBlueprintNativeAssignmentRejectsIncompleteOrMismatchedWitness(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testtaskassignment.Assignment, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact)
	}{
		{"missing witness list", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.RestorationAuthority.NativePredecessors = nil
		}},
		{"wrong witness service", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.RestorationAuthority.NativePredecessors[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"partial prior reference", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorTarget = ""
		}},
		{"wrong prior release", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorReleaseId = nativeAssignmentRetainedRelease
		}},
		{"wrong prior target", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorTarget = "green"
		}},
		{"foreign current owner", func(a *testtaskassignment.Assignment, current, _ *agentpb.ComposeArtifact) {
			current.OwnerId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAX"
			sealNativeAssignmentArtifact(t, a.RestorationAuthority.NativePredecessors[0], current, nil)
		}},
		{"duplicate current workload", func(a *testtaskassignment.Assignment, current, _ *agentpb.ComposeArtifact) {
			current.Services = append(current.Services, proto.Clone(current.Services[0]).(*agentpb.ComposeService))
			sealNativeAssignmentArtifact(t, a.RestorationAuthority.NativePredecessors[0], current, nil)
		}},
		{"inactive retained same slot", func(a *testtaskassignment.Assignment, _, retained *agentpb.ComposeArtifact) {
			retained.Services[0].Slot = "blue"
			for _, label := range retained.Services[0].ExpectedLabels {
				if label.Key == "com.groundplane.slot" {
					label.Value = "blue"
				}
			}
			sealNativeAssignmentArtifact(t, a.RestorationAuthority.NativePredecessors[0], nil, retained)
		}},
		{"current bytes altered", func(a *testtaskassignment.Assignment, _, _ *agentpb.ComposeArtifact) {
			a.RestorationAuthority.NativePredecessors[0].CurrentArtifact[0] ^= 0xff
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assignment, current, retained := nativeServingAssignment(t)
			test.mutate(&assignment, current, retained)
			if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, assignment.Plan); err == nil {
				t.Fatal("accepted incomplete or mismatched native witness")
			}
		})
	}
}

// Rationale: an explicit absence witness remains absence even when A contains
// a historical native service; an inactive artifact without current C is not
// silently promoted to serving authority.
func TestBlueprintNativeAssignmentPreservesExplicitAbsence(t *testing.T) {
	assignment, _, _ := nativeServingAssignment(t)
	authority := assignment.RestorationAuthority
	authority.Candidates[0].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
	authority.NativePredecessors[0].CurrentArtifact = nil
	authority.NativePredecessors[0].RetainedPriorArtifact = nil
	member := assignment.Plan.CandidateReleaseProcedure.Members[0]
	member.ServingPredecessor.PriorArtifactId = ""
	member.ServingPredecessor.PriorReleaseId = ""
	member.ServingPredecessor.PriorTarget = ""
	member.ServingPredecessor.RetainedPriorArtifactId = ""
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, assignment.Plan); err != nil {
		t.Fatalf("explicit native absence rejected: %v", err)
	}
	authority.NativePredecessors[0].RetainedPriorArtifact = []byte{0x01}
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, assignment.Plan); err == nil {
		t.Fatal("inactive retained native artifact accepted as serving authority")
	}
}

func nativeServingAssignment(
	t *testing.T,
) (testtaskassignment.Assignment, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact) {
	t.Helper()
	assignment := configuredRestorationAssignment(t)
	assignment.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	member := assignment.Plan.CandidateReleaseProcedure.Members[0]
	current := nativeAssignmentArtifact(
		assignment.RestorationAuthority.EnvironmentId,
		assignment.Plan.CandidateReleaseProcedure.Members[0].ServiceId,
		nativeAssignmentPriorArtifact,
		nativeAssignmentPriorRelease,
		"blue",
	)
	retained := nativeAssignmentArtifact(assignment.RestorationAuthority.EnvironmentId, member.ServiceId,
		nativeAssignmentRetainedArtifact, nativeAssignmentRetainedRelease, "green")
	currentBytes := marshalNativeAssignmentArtifact(t, current)
	retainedBytes := marshalNativeAssignmentArtifact(t, retained)
	member.ServingPredecessor.PriorArtifactId = current.ArtifactId
	member.ServingPredecessor.PriorReleaseId = nativeAssignmentPriorRelease
	member.ServingPredecessor.PriorTarget = "blue"
	member.ServingPredecessor.RetainedPriorArtifactId = retained.ArtifactId
	authority := assignment.RestorationAuthority
	authority.Candidates[0].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR
	authority.NativePredecessors = []*agentpb.ReleaseNativePredecessorAuthority{{
		ServiceId: member.ServiceId, CurrentArtifact: currentBytes, RetainedPriorArtifact: retainedBytes,
	}}
	assignment.Plan.Artifacts = []*agentpb.ComposeArtifact{proto.CloneOf(current), proto.CloneOf(retained)}
	return assignment, current, retained
}

func nativeAssignmentArtifact(environmentID, serviceID, artifactID, releaseID, slot string) *agentpb.ComposeArtifact {
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api-" + slot, ExpectedReplicas: 1, HasHealthcheck: true,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, Slot: slot,
			ImageReference: "sha256:" + strings.Repeat("a", 64),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "slot"},
				{Key: "com.groundplane.slot", Value: slot},
			},
		}},
	}
	return artifact
}

func marshalNativeAssignmentArtifact(t *testing.T, artifact *agentpb.ComposeArtifact) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func sealNativeAssignmentArtifact(
	t *testing.T,
	witness *agentpb.ReleaseNativePredecessorAuthority,
	current, retained *agentpb.ComposeArtifact,
) {
	t.Helper()
	if current != nil {
		witness.CurrentArtifact = marshalNativeAssignmentArtifact(t, current)
	}
	if retained != nil {
		witness.RetainedPriorArtifact = marshalNativeAssignmentArtifact(t, retained)
	}
}
