package controller

import (
	"bytes"
	"encoding/hex"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a ledger revision cannot supply proxy recovery authority. A plan
// with generation-one rollback beside a generation-two native witness must be
// rejected before publication, not after a failed rollout mutates the host.
func TestReleasePlanRejectsConflictingNativeProxyRecovery(t *testing.T) {
	for _, field := range []string{"generation", "digest"} {
		t.Run(field, func(t *testing.T) {
			resolver, task, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
			member := &input.Members[0]
			if field == "generation" {
				member.Render.PriorProxyGeneration++
			} else {
				member.Render.PriorProxyDigest = member.Render.ProxyConfigDigest
			}
			if _, _, err := resolver.PrepareReleaseTask(t.Context(), task, input); err == nil {
				t.Fatal("contradictory recovery authority was accepted")
			}
		})
	}
}

// Rationale: changing the candidate's exposed port must not replace the exact
// historical proxy configuration used by both rollback and its terminal proof.
func TestReleasePlanUsesExactNativeProxyConfiguration(t *testing.T) {
	resolver, task, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	member := &input.Members[0]
	native := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(member.Render.PriorRuntime.CurrentArtifact, native); err != nil {
		t.Fatal(err)
	}
	var expected *agentpb.ComposeService
	for _, service := range native.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			expected = service
		}
	}
	if expected == nil {
		t.Fatal("fixture has no native proxy")
	}
	member.Render.ProxyPorts = []uint16{9090}
	candidate, err := domain.RenderProxyConfig(member.Render.ServiceName, member.Intent.ID,
		member.Render.CandidateTarget, member.Render.ProxyGeneration, member.Render.ProxyPorts)
	if err != nil {
		t.Fatal(err)
	}
	member.Render.ProxyConfigDigest = hex.EncodeToString(candidate.SHA256[:])
	_, plan, err := resolver.PrepareReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatal(err)
	}
	probe, compensate := plan.Steps[3].GetServiceProxyProbe(), plan.Steps[4].GetServiceProxyCompensate()
	if probe == nil || compensate == nil || !bytes.Equal(probe.ConfigJson, expected.ProxyConfigJson) ||
		!bytes.Equal(compensate.ConfigJson, expected.ProxyConfigJson) ||
		!bytes.Equal(probe.ConfigSha256, expected.ProxyConfigSha256) ||
		!bytes.Equal(compensate.ConfigSha256, expected.ProxyConfigSha256) {
		t.Fatal("recovery did not bind exact native proxy bytes")
	}
}
