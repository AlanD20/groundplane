package agent

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: an absence target does not waive the retained applied witness;
// corrupt or contradictory authority must reject before execution begins.
func TestCandidateAssignmentValidatesAbsenceWitness(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ReleaseRestorationAuthority)
	}{
		{"digest", func(a *agentpb.ReleaseRestorationAuthority) { a.AppliedPredecessor.ComposeArtifactSha256[0] ^= 1 }},
		{"key revision", func(a *agentpb.ReleaseRestorationAuthority) { a.AppliedPredecessor.KeyRevision = 0 }},
		{"revision identity", func(a *agentpb.ReleaseRestorationAuthority) { a.AppliedPredecessor.RevisionId = "invalid" }},
		{"generation", func(a *agentpb.ReleaseRestorationAuthority) { a.AppliedPredecessor.RenderGeneration = 0 }},
		{"wrong environment", func(a *agentpb.ReleaseRestorationAuthority) { a.EnvironmentId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{"malformed artifact", func(a *agentpb.ReleaseRestorationAuthority) { sealAssignmentWitness(a, []byte{0xff}) }},
		{"unknown artifact field", func(a *agentpb.ReleaseRestorationAuthority) {
			sealAssignmentWitness(a, append(a.AppliedPredecessor.ComposeArtifact, 0xf8, 0x7f, 1))
		}},
		{"noncanonical artifact", func(a *agentpb.ReleaseRestorationAuthority) {
			sealAssignmentWitness(a, append(a.AppliedPredecessor.ComposeArtifact, a.AppliedPredecessor.ComposeArtifact...))
		}},
		{"configured serving target", func(a *agentpb.ReleaseRestorationAuthority) {
			a.Candidates[0].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment := configuredRestorationAssignment(t)
			if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err != nil {
				t.Fatalf("valid configured witness: %v", err)
			}
			test.mutate(assignment.RestorationAuthority)
			if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err == nil {
				t.Fatal("accepted corrupt or contradictory applied witness")
			}
		})
	}
}

func configuredRestorationAssignment(t *testing.T) Assignment {
	t.Helper()
	const service = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const candidate = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const artifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const environment = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	assignment := Assignment{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		Plan: &agentpb.ExecutionPlan{PlanHash: bytes.Repeat([]byte{0x51}, 32),
			CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
				ServiceId: service, CandidateReleaseId: candidate, CandidateArtifactId: artifactID,
				CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
					ProbeStepId:      "absence-probe",
					CompensateStepId: "absence-compensate",
				},
				ServingPredecessor: &agentpb.ServingPredecessorRestoration{
					ProbeStepId:      "serving-probe",
					CompensateStepId: "serving-compensate",
				},
			}}},
		},
	}
	assignment.RestorationAuthority = &agentpb.ReleaseRestorationAuthority{
		TaskId: assignment.TaskID, OperationId: assignment.OperationID, PlanHash: assignment.Plan.PlanHash,
		EnvironmentId: environment, CandidateArtifactId: artifactID, AuthoritySha256: bytes.Repeat([]byte{0x61}, 32),
		Candidates: []*agentpb.ReleaseRestorationCandidate{{ServiceId: service, ReleaseId: candidate,
			Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE}},
		NativePredecessors: []*agentpb.ReleaseNativePredecessorAuthority{{ServiceId: service}},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
			KeyRevision: 17, RevisionId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", RenderGeneration: 1,
		},
	}
	witness, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment,
		Services: []*agentpb.ComposeService{{ServiceId: service}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sealAssignmentWitness(assignment.RestorationAuthority, witness)
	return assignment
}

func sealAssignmentWitness(authority *agentpb.ReleaseRestorationAuthority, encoded []byte) {
	digest := sha256.Sum256(encoded)
	authority.AppliedPredecessor.ComposeArtifact = encoded
	authority.AppliedPredecessor.ComposeArtifactSha256 = digest[:]
}

// Rationale: validating the received map must preserve a lawful mixed selection
// and reject a changed target without deriving new execution authority.
func TestCandidateAssignmentPreservesMixedSelection(t *testing.T) {
	assignment := configuredRestorationAssignment(t)
	authority := assignment.RestorationAuthority
	const servingService = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	member := proto.CloneOf(assignment.Plan.CandidateReleaseProcedure.Members[0])
	member.ServiceId = servingService
	assignment.Plan.CandidateReleaseProcedure.Members = append(
		assignment.Plan.CandidateReleaseProcedure.Members,
		member,
	)
	authority.Candidates = append(authority.Candidates, &agentpb.ReleaseRestorationCandidate{
		ServiceId: servingService, ReleaseId: member.CandidateReleaseId,
		Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
	})
	current := nativeAssignmentArtifact(authority.EnvironmentId, servingService,
		nativeAssignmentPriorArtifact, nativeAssignmentPriorRelease, "blue")
	assignment.Plan.Artifacts = append(assignment.Plan.Artifacts, current)
	member.ServingPredecessor.PriorArtifactId = current.ArtifactId
	member.ServingPredecessor.PriorReleaseId = nativeAssignmentPriorRelease
	member.ServingPredecessor.PriorTarget = "blue"
	authority.NativePredecessors = append(authority.NativePredecessors, &agentpb.ReleaseNativePredecessorAuthority{
		ServiceId: servingService, CurrentArtifact: marshalNativeAssignmentArtifact(t, current),
	})
	before := proto.CloneOf(authority)
	if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err != nil {
		t.Fatalf("mixed selection rejected: %v", err)
	}
	if !proto.Equal(before, authority) {
		t.Fatal("validation mutated sealed authority")
	}
	authority.Candidates[1].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
	if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err == nil {
		t.Fatal("serving witness accepted as absence")
	}
	authority.Candidates[1].Target = before.Candidates[1].Target
	authority.AppliedPredecessor = nil
	if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err != nil {
		t.Fatalf("native serving member required an applied artifact: %v", err)
	}
	authority.NativePredecessors = nil
	if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err == nil {
		t.Fatal("serving member accepted without native witness")
	}
}

// Rationale: a true first deployment has no applied witness and remains lawful.
func TestCandidateAssignmentAcceptsAbsentWitness(t *testing.T) {
	assignment := configuredRestorationAssignment(t)
	assignment.RestorationAuthority.AppliedPredecessor = nil
	if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err != nil {
		t.Fatal(err)
	}
}
