package workexecution

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SCRIPT-06: a duplicated or unbounded hook selection cannot become executable
// delivery; the full allowed capture must survive transport without aliasing.
func TestNativeReleaseDeliveryBoundsHookSelectionAndOwnsCapture(t *testing.T) {
	item, identity, at := readyNativeWork()
	_, attempt, err := Assign(item, identity, nativeAttemptID(item, at, "hook-delivery"), Forward,
		at.Add(time.Second), item.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	wireAttempt, err := EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	environmentID := ids.New(ids.KindEnvironment)
	raw := []byte("services: {}\n")
	digest := sha256.Sum256(raw)
	input := &agentpb.ReleaseWorkInput{
		OperationId: ids.New(ids.KindOperation), EnvironmentId: environmentID, ServiceId: item.TargetID,
		ReleaseId: ids.New(ids.KindDeployment), RenderGeneration: 1,
		Strategy:      agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		FailurePolicy: agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_SWITCH_BACK,
		Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId: environmentID, Generation: 1, Sha256: digest[:]},
		Candidate: &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig),
			OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
			ProjectName: "gp-" + strings.ToLower(environmentID), CanonicalYaml: raw, YamlSha256: digest[:]},
		Predecessor: &agentpb.ReleaseWorkPredecessor{Absent: true},
	}
	for range 16 {
		input.Hooks = append(input.Hooks, &agentpb.ReleaseWorkHook{
			Input:       &agentpb.WorkInputReference{InputId: ids.New(ids.KindPlan), Sha256: digest[:]},
			ExecutionId: ids.NewULID(), ScriptId: ids.New(ids.KindScript), Phase: "post-deploy", Order: 65535,
		})
	}
	input.Hooks[15].Phase = "on-failure"
	assignment := &agentpb.ReleaseWorkAssignment{Attempt: wireAttempt, Release: input}
	_, opened, err := DecodeReleaseDelivery(assignment, identity)
	if err != nil || len(opened.Hooks) != 16 || opened.Hooks[15].Phase != "on-failure" {
		t.Fatalf("maximum captured selection did not survive delivery: %v", err)
	}
	opened.Hooks[0].Input.Sha256[0] ^= 1
	opened.Hooks[0].ExecutionId = "changed"
	if input.Hooks[0].Input.Sha256[0] != digest[0] || input.Hooks[0].ExecutionId == "changed" {
		t.Fatal("delivery consumer mutated retained capture")
	}
	for _, test := range []struct {
		name   string
		change func(*agentpb.ReleaseWorkInput)
	}{
		{"missing failure policy", func(in *agentpb.ReleaseWorkInput) { in.FailurePolicy = 0 }},
		{"overflow", func(in *agentpb.ReleaseWorkInput) {
			extra := proto.CloneOf(in.Hooks[0])
			extra.ExecutionId, extra.ScriptId = ids.NewULID(), ids.New(ids.KindScript)
			extra.Input.InputId = ids.New(ids.KindPlan)
			in.Hooks = append(in.Hooks, extra)
		}},
		{"duplicate execution", func(in *agentpb.ReleaseWorkInput) { in.Hooks[1].ExecutionId = in.Hooks[0].ExecutionId }},
		{"missing binding", func(in *agentpb.ReleaseWorkInput) { in.Hooks[0].Input = nil }},
		{"mixed operation", func(in *agentpb.ReleaseWorkInput) { in.Hooks[0].Phase = "pre-rollback" }},
		{"manual execution", func(in *agentpb.ReleaseWorkInput) { in.Hooks[0].Phase = "manual" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := proto.CloneOf(assignment)
			test.change(changed.Release)
			if _, _, err := DecodeReleaseDelivery(changed, identity); err == nil {
				t.Fatal("invalid selection became native Release delivery")
			}
		})
	}
}
