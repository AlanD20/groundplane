package executionplan

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: access valid in inherited mode does not become an implicit grant
// when the same runner switches to the explicit context.
func TestExplicitScriptRejectsInheritedNetworkAndSecretSources(t *testing.T) {
	for _, resource := range []string{"network", "secret"} {
		t.Run(resource, func(t *testing.T) {
			plan := validManualScriptPlan(t)
			snapshot, projection := plan.ScriptRunnerSnapshots[0], plan.ScriptRunnerProjections[0]
			if resource == "network" {
				snapshot.Networks = []*agentpb.ScriptRunnerNetwork{{
					NetworkId: "net_01ARZ3NDEKTSV4RRFFQ69G5FAV", OwnerEnvironmentId: snapshot.EnvironmentId,
					Source: existingScriptSourceAuthorityForTest(12),
					RenderedAttachment: &agentpb.ScriptNetworkAttachment{
						DockerNetworkName: "gp_net_net_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					},
				}}
				projection.Networks = snapshot.Networks
			} else {
				digest := sha256.Sum256([]byte("test digest only"))
				snapshot.SecretValues = []*agentpb.ScriptRunnerSecretValue{{
					OwnerId: snapshot.ServiceId, ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", Digest: digest[:],
				}}
			}
			refreshScriptProjectionImageDigestForTest(t, plan)
			if _, err := Seal(plan); err != nil {
				t.Fatalf("valid inherited source rejected: %v", err)
			}
			plan = explicitScriptPlanForTest(t, plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("explicit runner accepted an inherited ambient source")
			}
		})
	}
}

// Rationale: updating the outer snapshot hash cannot hide a stale or absent
// digest of its immutable captured context.
func TestExplicitScriptRejectsStaleContextDigest(t *testing.T) {
	for _, digest := range [][]byte{nil, make([]byte, sha256.Size)} {
		plan := explicitScriptPlanForTest(t, validManualScriptPlan(t))
		plan.ScriptRunnerSnapshots[0].ExplicitExecution.ContextSha256 = digest
		refreshScriptProjectionImageDigestForTest(t, plan)
		if _, err := Seal(plan); err == nil {
			t.Fatal("stale explicit context digest accepted")
		}
	}
}

// Rationale: an explicit runner image is distinct from the real candidate image;
// separating them must retain rather than bypass the candidate Release binding.
func TestExplicitScriptImagesKeepIndependentReleaseAuthority(t *testing.T) {
	for _, test := range []struct {
		name  string
		build func(*testing.T) *agentpb.ExecutionPlan
	}{
		{"manual", validManualScriptPlan}, {"candidate", validBlueprintScriptReconcilePlan},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := explicitScriptPlanForTest(t, test.build(t))
			if _, err := Seal(plan); err != nil {
				t.Fatalf("valid explicit runner rejected: %v", err)
			}
			if test.name == "candidate" {
				plan.ScriptRunnerSnapshots[0].ExplicitExecution.ReleaseLocalImageId = "sha256:" + strings.Repeat(
					"c",
					64,
				)
				refreshScriptProjectionImageDigestForTest(t, plan)
				if _, err := Seal(plan); err == nil {
					t.Fatal("rehashing let the runner impersonate another consumer Release")
				}
			}
		})
	}
}

// Rationale: self-consistent hashes do not authorize ambient Service access or
// substitute for a captured explicit context and positive Script source revision.
func TestExplicitScriptRejectsRehashedAmbientProjection(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*agentpb.ResolvedRunnerSnapshot, *agentpb.ScriptRunnerProjection)
	}{
		{"missing source", func(snapshot *agentpb.ResolvedRunnerSnapshot, _ *agentpb.ScriptRunnerProjection) {
			snapshot.ExplicitExecution.ScriptModRevision = 0
		}},
		{"mutable image", func(snapshot *agentpb.ResolvedRunnerSnapshot, _ *agentpb.ScriptRunnerProjection) {
			snapshot.ExplicitExecution.Context.ImageReference = "example/setup:latest"
		}},
		{"ambient environment", func(_ *agentpb.ResolvedRunnerSnapshot, projection *agentpb.ScriptRunnerProjection) {
			projection.Environment = []*agentpb.ScriptStringPair{{Key: "UNDECLARED", Value: "not-a-secret"}}
		}},
		{"ambient runtime", func(_ *agentpb.ResolvedRunnerSnapshot, projection *agentpb.ScriptRunnerProjection) {
			projection.Runtime = "custom-runtime"
		}},
		{"ambient group", func(_ *agentpb.ResolvedRunnerSnapshot, projection *agentpb.ScriptRunnerProjection) {
			projection.GroupAdd = []string{"10"}
		}},
		{"working directory", func(_ *agentpb.ResolvedRunnerSnapshot, projection *agentpb.ScriptRunnerProjection) {
			projection.WorkingDir = "/app"
		}},
		{"different user", func(snapshot *agentpb.ResolvedRunnerSnapshot, _ *agentpb.ScriptRunnerProjection) {
			snapshot.ExplicitExecution.Context.User = "1234:0"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := explicitScriptPlanForTest(t, validManualScriptPlan(t))
			snapshot := plan.ScriptRunnerSnapshots[0]
			test.edit(snapshot, plan.ScriptRunnerProjections[0])
			digest, err := scriptMessageDigest(snapshot.ExplicitExecution.Context)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.ExplicitExecution.ContextSha256 = digest
			refreshScriptProjectionImageDigestForTest(t, plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("invalid explicit authority or ambient projection accepted")
			}
		})
	}
}

func explicitScriptPlanForTest(t *testing.T, plan *agentpb.ExecutionPlan) *agentpb.ExecutionPlan {
	t.Helper()
	snapshot, projection := plan.ScriptRunnerSnapshots[0], plan.ScriptRunnerProjections[0]
	context := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
	}
	digest, err := scriptMessageDigest(context)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
		Context: context, ContextSha256: digest, ScriptModRevision: 10, ReleaseLocalImageId: snapshot.LocalImageId,
	}
	snapshot.LocalImageId = "sha256:" + strings.Repeat("b", 64)
	projection.Image = snapshot.LocalImageId
	projection.WorkingDir = "/"
	refreshScriptProjectionImageDigestForTest(t, plan)
	return plan
}
