package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: executor lookup consumes the selected member and must fail closed
// for absent or duplicate identities rather than inheriting another target.
func TestRestorationTargetForServiceIsExact(t *testing.T) {
	authority := &agentpb.ReleaseRestorationAuthority{Candidates: []*agentpb.ReleaseRestorationCandidate{
		{ServiceId: "old", Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR},
		{ServiceId: "new", Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE},
	}}
	if got := RestorationTargetForService(authority, "new"); got != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
		t.Fatalf("selected new target = %v", got)
	}
	if RestorationTargetForService(
		authority,
		"missing",
	) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED ||
		RestorationTargetForService(
			nil,
			"new",
		) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED {
		t.Fatal("missing authority member acquired a target")
	}
	authority.Candidates = append(authority.Candidates, proto.CloneOf(authority.Candidates[1]))
	if RestorationTargetForService(
		authority,
		"new",
	) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED {
		t.Fatal("duplicate member acquired a target")
	}
}
