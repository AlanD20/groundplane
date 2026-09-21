package composeruntime

import (
	testing "testing"

	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

// Rationale: recreate recovery may prove a sealed blue-green predecessor, but only
// from one exact valid slot; missing, unsupported, or replicated slots are not authority.
func TestRecreateEvidenceExpectationAcceptsOnlyExactSealedPriorSlot(t *testing.T) {
	plan := &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{releaseTestArtifact("prior-artifact")}}
	expected, ok := recreateEvidenceExpectation(plan, "prior-artifact", "api", "prior-api", true)
	if !ok || expected.target != "green" {
		t.Fatalf("sealed prior expectation = %#v, valid = %t", expected, ok)
	}
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ComposeService)
	}{
		{name: "missing slot", mutate: func(service *agentpb.ComposeService) { service.Slot = "" }},
		{name: "unsupported slot", mutate: func(service *agentpb.ComposeService) { service.Slot = "red" }},
		{name: "replicated slot", mutate: func(service *agentpb.ComposeService) { service.ExpectedReplicas = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owned := proto.Clone(plan).(*agentpb.ExecutionPlan)
			test.mutate(owned.Artifacts[0].Services[1])
			if _, valid := recreateEvidenceExpectation(
				owned, "prior-artifact", "api", "prior-api", true,
			); valid {
				t.Fatal("invalid prior slot produced recreate evidence authority")
			}
		})
	}
}
