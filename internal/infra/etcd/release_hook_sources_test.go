package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: ordinary Release capture can refresh derived Attach memberships
// without changing the immutable Blueprint inputs. Hook discovery must accept
// that sealed capture, retain its source fences, and still reject substitutions.
func TestReleaseHookSourcesSeparateAuthoredInputsFromDerivedArtifact(t *testing.T) {
	for _, test := range []struct {
		name      string
		edit      func(*ReleaseRenderInput)
		reseal    bool
		wantError bool
	}{
		{name: "unchanged", reseal: true},
		{name: "recaptured artifact", edit: recaptureHookTestArtifact, reseal: true},
		{name: "unsealed artifact substitution", edit: recaptureHookTestArtifact, wantError: true},
		{name: "changed authored input", edit: func(render *ReleaseRenderInput) {
			render.Projection.NormalizedCompose = append(render.Projection.NormalizedCompose, '\n')
		}, reseal: true, wantError: true},
		{name: "changed generation", edit: func(render *ReleaseRenderInput) {
			render.Projection.RenderGeneration++
		}, reseal: true, wantError: true},
		{name: "invalid artifact digest", edit: func(render *ReleaseRenderInput) {
			artifact := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(render.Projection.ComposeArtifact, artifact); err != nil {
				panic(err)
			}
			artifact.YamlSha256 = make([]byte, sha256.Size)
			value, err := proto.Marshal(artifact)
			if err != nil {
				panic(err)
			}
			render.Projection.ComposeArtifact = value
		}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, sources, _, _, _ := manualScriptLifecycleFixture(t)
			intent := sources.Release.Intent
			rootArtifact := bytes.Clone(sources.DesiredProjection.Record.ComposeArtifact)
			render := ReleaseRenderInput{
				ReleaseID: intent.ID, PlanID: ids.New(ids.KindPlan), ArtifactID: intent.RenderInputID,
				ServiceID: intent.ServiceID, ServiceName: sources.Service.Record.Desired.Name,
				CandidateWorkload: intent.CandidateWorkload,
				Strategy:          domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
				CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
				TenantID: sources.Tenant.Record.ID, TenantSlug: sources.Tenant.Record.Slug,
				ProjectID: sources.Project.Record.ID, ProjectSlug: sources.Project.Record.Slug,
				EnvironmentID: sources.Environment.Record.ID, EnvironmentName: sources.Environment.Record.Name,
				AuthorizedVolumeDir:    sources.Environment.Record.VolumeDir,
				Projection:             cloneEnvironmentComposeProjection(sources.DesiredProjection.Record),
				ServiceDependencyPlans: sources.DesiredProjection.Record.ServiceDependencyPlans.Clone(),
			}
			original, err := EncodeReleaseRenderInput(render)
			if err != nil {
				t.Fatal(err)
			}
			intent.RenderInputDigest, err = domain.Digest(json.RawMessage(original))
			if err != nil {
				t.Fatal(err)
			}
			if test.edit != nil {
				test.edit(&render)
			}
			// Deliberately encode directly so the invalid-artifact case reaches
			// the durable reader, not just the writer's validator.
			raw, err := json.Marshal(render)
			if err != nil {
				t.Fatal(err)
			}
			if test.reseal {
				intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
				if err != nil {
					t.Fatal(err)
				}
			}
			intentBytes, err := encodeReleaseRecord("release-intent", intent)
			if err != nil {
				t.Fatal(err)
			}
			renderBytes, err := encodeReleaseRecord("release-render-input", json.RawMessage(raw))
			if err != nil {
				t.Fatal(err)
			}
			seed, err := store.Transact(ctx, nil, []Mutation{
				{Type: MutationPut, Key: releaseIntentStagingKey("", intent.ID), Value: intentBytes},
				{Type: MutationPut, Key: releaseRenderInputStagingKey("", intent.ID), Value: renderBytes},
			})
			if err != nil || !seed.Succeeded {
				t.Fatalf("stage hook sources: %v", err)
			}
			ledger := &ReleaseLedger{store: &releasePlanningTestStore{memoryHierarchyStore: store}}
			loaded, err := (&ScriptRepository{store: store}).LoadReleaseHookExecutionSources(
				ctx, ledger, sources.Script.Record.Desired.ID, intent.ID, seed.Revision,
			)
			if test.wantError {
				if err == nil {
					t.Fatal("substituted hook source was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("load sealed hook sources: %v", err)
			}
			if loaded.Release.Intent.ID != intent.ID || loaded.Revision != seed.Revision ||
				loaded.DesiredHead.Revision != sources.DesiredHead.Revision ||
				loaded.DesiredProjection.Revision != sources.DesiredProjection.Revision ||
				!bytes.Equal(loaded.DesiredProjection.Record.ComposeArtifact, rootArtifact) ||
				!bytes.Equal(loaded.RenderInput.Record.Projection.ComposeArtifact, render.Projection.ComposeArtifact) {
				t.Fatal("hook discovery substituted its sealed Release, immutable root or source fences")
			}
			if store.revision != seed.Revision {
				t.Fatal("hook discovery changed stored state")
			}
		})
	}
}

func recaptureHookTestArtifact(render *ReleaseRenderInput) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(render.Projection.ComposeArtifact, artifact); err != nil {
		panic(err)
	}
	artifact.CanonicalYaml = []byte("services: {}\nnetworks:\n  current-backing:\n    external: true\n")
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	value, err := proto.Marshal(artifact)
	if err != nil {
		panic(err)
	}
	render.Projection.ComposeArtifact = value
}
