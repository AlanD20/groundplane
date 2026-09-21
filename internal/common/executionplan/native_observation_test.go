package executionplan

import (
	bytes "bytes"
	"crypto/sha256"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	testing "testing"
)

// Rationale: Blueprint serving restoration selects the immutable native C
// witness, while the acknowledged applied A witness remains byte-for-byte
// independent and is not used as a fallback.
func TestBlueprintNativeAssignmentSelectsCurrentWitnessOverAppliedArtifact(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	member := plan.CandidateReleaseProcedure.Members[0]
	current := nativePlanArtifactForTest(t, plan.Artifacts[0], "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"dep_01ARZ3NDEKTSV4RRFFQ69G5FAW", "green", 6)
	retained := nativePlanArtifactForTest(t, plan.Artifacts[0], "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		"dep_01ARZ3NDEKTSV4RRFFQ69G5FAX", "blue", 5)
	plan.Artifacts = append(plan.Artifacts, current, retained)
	member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
		ProbeStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB0", CompensateStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB1",
		PriorArtifactId: current.ArtifactId, PriorReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW", PriorTarget: "green",
		RetainedPriorArtifactId: retained.ArtifactId,
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	currentBytes := nativeWitnessBytes(t, current)
	retainedBytes := nativeWitnessBytes(t, retained)
	// The applied witness deliberately differs from the currently serving one.
	appliedDigest := sha256.Sum256(retainedBytes)
	authority := &agentpb.ReleaseRestorationAuthority{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationId: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EnvironmentId: current.OwnerId, PlanHash: sealed.PlanHash,
		CandidateArtifactId: member.CandidateArtifactId, AuthoritySha256: bytes.Repeat([]byte{0x61}, 32),
		Candidates: []*agentpb.ReleaseRestorationCandidate{{ServiceId: member.ServiceId,
			ReleaseId: member.CandidateReleaseId, Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR}},
		NativePredecessors: []*agentpb.ReleaseNativePredecessorAuthority{{ServiceId: member.ServiceId,
			CurrentArtifact: currentBytes, RetainedPriorArtifact: retainedBytes}},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{KeyRevision: 17,
			RevisionId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", RenderGeneration: 5,
			ComposeArtifact: retainedBytes, ComposeArtifactSha256: appliedDigest[:]},
	}
	appliedBefore := proto.CloneOf(authority.AppliedPredecessor)
	observation, err := NewRestorationObservation(
		sealed,
		authority,
		member.ServingPredecessor.ProbeStepId,
	)
	if err != nil {
		t.Fatalf("open native serving predecessor: %v", err)
	}
	if opened := observation.Artifact(); !proto.Equal(opened, current) {
		t.Fatalf("selected predecessor = %#v", opened)
	}
	if !bytes.Equal(
		authority.AppliedPredecessor.ComposeArtifact,
		appliedBefore.ComposeArtifact,
	) ||
		!bytes.Equal(
			authority.AppliedPredecessor.ComposeArtifactSha256,
			appliedBefore.ComposeArtifactSha256,
		) {
		t.Fatal("native selection changed the applied A witness")
	}
}
