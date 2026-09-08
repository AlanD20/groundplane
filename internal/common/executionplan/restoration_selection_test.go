package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: an applied record may contain both never-started and serving
// Services; selection must be per member and must not mutate the witness.
func TestSelectBlueprintRestorationMembersMixed(t *testing.T) {
	const first = "svc_01K4A1B2C3D4E5F6G7H8J9K0MN"
	const second = "svc_01K4A1B2C3D4E5F6G7H8J9K0MP"
	const release = "dep_01K4A1B2C3D4E5F6G7H8J9K0MQ"
	candidates := []CandidateServiceIdentity{
		{ServiceID: second, ReleaseID: release},
		{ServiceID: first, ReleaseID: release},
	}
	for _, test := range []struct {
		name      string
		artifact  *agentpb.ComposeArtifact
		wantFirst agentpb.ReleaseRestorationTarget
	}{
		{"no applied record", nil, agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE},
		{"configured only", &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{{ServiceId: first}}}, agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE},
		{"unrelated serving", &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{restorationSelectionWorkload("svc_01K4A1B2C3D4E5F6G7H8J9K0MR")}}, agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE},
		{"mixed", &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{restorationSelectionWorkload(first), {ServiceId: second}}}, agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := proto.CloneOf(test.artifact)
			got, err := SelectBlueprintRestorationMembers(candidates, test.artifact)
			if err != nil || len(got) != 2 {
				t.Fatalf("selection = %v, %v", got, err)
			}
			if got[0].ServiceID != first || got[0].Target != test.wantFirst || got[1].ServiceID != second ||
				got[1].Target != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
				t.Fatalf("wrong per-member selection: %v", got)
			}
			if !proto.Equal(before, test.artifact) || candidates[0].ServiceID != second {
				t.Fatal("selection mutated its immutable inputs")
			}
		})
	}
}

// Rationale: missing or contradictory runtime metadata must never become
// authority to remove a possibly serving workload as a first candidate.
func TestSelectBlueprintRestorationMembersRejectsAmbiguousRuntime(t *testing.T) {
	const serviceID = "svc_01K4A1B2C3D4E5F6G7H8J9K0MN"
	candidates := []CandidateServiceIdentity{{ServiceID: serviceID, ReleaseID: "dep_01K4A1B2C3D4E5F6G7H8J9K0MQ"}}
	for _, mutate := range []func(*agentpb.ComposeArtifact){
		func(a *agentpb.ComposeArtifact) { a.Services = append(a.Services, proto.CloneOf(a.Services[0])) },
		func(a *agentpb.ComposeArtifact) { a.Services[0].ExpectedLabels = nil },
		func(a *agentpb.ComposeArtifact) { a.Services[0].ImageReference = "" },
		func(a *agentpb.ComposeArtifact) { a.Services[0].ExpectedReplicas = 0 },
		func(a *agentpb.ComposeArtifact) { a.Services[0].ExpectedLabels[0].Value = candidates[0].ReleaseID },
		func(a *agentpb.ComposeArtifact) {
			a.Services[0].ExpectedLabels = append(a.Services[0].ExpectedLabels, proto.CloneOf(a.Services[0].ExpectedLabels[0]))
		},
		func(a *agentpb.ComposeArtifact) {
			a.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED
		},
		func(a *agentpb.ComposeArtifact) {
			a.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
		},
	} {
		artifact := &agentpb.ComposeArtifact{
			Services: []*agentpb.ComposeService{restorationSelectionWorkload(serviceID)},
		}
		mutate(artifact)
		if got, err := SelectBlueprintRestorationMembers(candidates, artifact); err == nil || got != nil {
			t.Fatalf("ambiguous predecessor accepted: %v, %v", got, err)
		}
	}
}

// Rationale: a target map cannot safely bind duplicate or malformed candidate
// identities, even when no applied artifact exists.
func TestSelectBlueprintRestorationMembersRejectsInvalidCandidates(t *testing.T) {
	valid := CandidateServiceIdentity{
		ServiceID: "svc_01K4A1B2C3D4E5F6G7H8J9K0MN",
		ReleaseID: "dep_01K4A1B2C3D4E5F6G7H8J9K0MQ",
	}
	for _, candidates := range [][]CandidateServiceIdentity{
		nil, {valid, valid}, {{ServiceID: "invalid", ReleaseID: valid.ReleaseID}}, {{ServiceID: valid.ServiceID}},
	} {
		if got, err := SelectBlueprintRestorationMembers(candidates, nil); err == nil || got != nil {
			t.Fatalf("invalid candidates accepted: %v, %v", got, err)
		}
	}
}

func restorationSelectionWorkload(serviceID string) *agentpb.ComposeService {
	return &agentpb.ComposeService{
		ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 1, ImageReference: "sha256:old",
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.release-id", Value: "dep_01K4A1B2C3D4E5F6G7H8J9K0MP"},
			{Key: "com.groundplane.runtime-role", Value: "singleton"},
		},
	}
}

// Rationale: only a paired, opposite uninstantiated blue-green slot may omit a
// Release id. An orphan, duplicate slot, or singleton omission is corruption.
func TestRestorationSelectionValidatesUninstantiatedSlot(t *testing.T) {
	const serviceID = "svc_01K4A1B2C3D4E5F6G7H8J9K0MN"
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ComposeArtifact)
		valid  bool
	}{
		{name: "paired slot", valid: true},
		{name: "orphan", mutate: func(a *agentpb.ComposeArtifact) { a.Services = a.Services[1:] }},
		{name: "no proxy", mutate: func(a *agentpb.ComposeArtifact) { a.Services = a.Services[:2] }},
		{name: "duplicate slot", mutate: func(a *agentpb.ComposeArtifact) {
			a.Services[1].Slot = "blue"
			a.Services[1].ExpectedLabels[1].Value = "blue"
		}},
		{name: "slot label mismatch", mutate: func(a *agentpb.ComposeArtifact) { a.Services[1].ExpectedLabels[1].Value = "blue" }},
		{name: "missing slot", mutate: func(a *agentpb.ComposeArtifact) { a.Services[1].Slot = "" }},
		{name: "duplicate placeholder", mutate: func(a *agentpb.ComposeArtifact) { a.Services = append(a.Services, proto.CloneOf(a.Services[1])) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			active := restorationSelectionWorkload(serviceID)
			active.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
			active.Slot = "blue"
			active.ExpectedLabels[1].Value = "slot"
			active.ExpectedLabels = append(
				active.ExpectedLabels,
				&agentpb.LabelPair{Key: "com.groundplane.slot", Value: "blue"},
			)
			inactive := proto.CloneOf(active)
			inactive.ComposeName, inactive.Slot = "api-green", "green"
			inactive.ExpectedLabels = []*agentpb.LabelPair{
				{Key: "com.groundplane.runtime-role", Value: "slot"}, {Key: "com.groundplane.slot", Value: "green"},
			}
			artifact := &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{active, inactive, {
				ServiceId: serviceID, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
			}}}
			if test.mutate != nil {
				test.mutate(artifact)
			}
			selected, err := SelectBlueprintRestorationMembers([]CandidateServiceIdentity{{
				ServiceID: serviceID, ReleaseID: "dep_01K4A1B2C3D4E5F6G7H8J9K0MQ",
			}}, artifact)
			if test.valid &&
				(err != nil || len(selected) != 1 || selected[0].Target != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR) {
				t.Fatalf("valid paired slot = %v, %v", selected, err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid slot authority accepted")
			}
		})
	}
}

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
