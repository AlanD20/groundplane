package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (fixture *ExecutedArtifactFixture) AssertRetainedSourceRaces(
	t *testing.T,
	render testreleaserender.ReleaseRenderInput,
	mixed *agentpb.ComposeArtifact,
) {
	t.Helper()
	ctx := context.Background()
	for kindIndex, kind := range []string{"intent", "render", "projection", "applied"} {
		for pruneIndex, prune := range []bool{false, true} {
			scope, err := fixture.Ledger.LoadPlanningScope(ctx, render.EnvironmentID)
			if err != nil {
				t.Fatal(err)
			}
			planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{render.ServiceID})
			if err != nil {
				t.Fatal(err)
			}
			applied, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			serving, err := fixture.Ledger.ResolveServing(
				ctx,
				render.EnvironmentID,
				render.ServiceID,
				scope.ReadRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			current, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, render.ReleaseID, scope.ReadRevision)
			if err != nil {
				t.Fatal(err)
			}
			release := testreleaserender.ServiceLifecycleRelease{
				ServingReleaseID:   render.ReleaseID,
				ProjectionRevision: serving.ProjectionRevision,
				IntentRevision:     serving.IntentRevision,
				RenderRevision:     current.Revision,
				Current:            current.Record,
			}
			task := fixture.Task(t, int64(1450+kindIndex*2+pruneIndex))
			projection := retainedRaceProjection(t, fixture, task.ID, mixed)
			task.RenderGeneration = int32(projection.RenderGeneration)
			publication, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
				ctx,
				etcd.BlueprintReleasePublication{},
				task,
				applied,
				[]etcd.BlueprintRetainedRuntimeSource{{Planning: planning[0], Release: &release, Intent: &serving}},
				mixed,
			)
			if err != nil {
				t.Fatal(err)
			}
			key := map[string]string{
				"intent":     testreleases.ReleaseIntentStagingKey("", render.ReleaseID),
				"render":     testreleases.ReleaseRenderInputStagingKey("", render.ReleaseID),
				"projection": testreleases.ReleaseProjectionKey(render.ServiceID),
				"applied":    testenvironmentprojection.EnvironmentComposeProjectionStorageKey(render.EnvironmentID),
			}[kind]
			before, err := fixture.store.Get(ctx, key)
			if err != nil || before.Entry == nil {
				t.Fatalf("read %s: %v", kind, err)
			}
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: before.Entry.Value}
			if prune {
				mutation.Type = testkeyvalue.MutationDelete
				mutation.Value = nil
			}
			if _, err := fixture.store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
				t.Fatal(err)
			}
			assertRetainedFinalPublicationRejected(t, fixture, task, projection, publication, kind, prune)
			if _, err := fixture.store.Put(ctx, key, before.Entry.Value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Rationale: publication must atomically reject a replaced or pruned inactive
// render, including the Task and desired-head mutations owned by final publication.
func (fixture *ExecutedArtifactFixture) AssertRetainedInactiveRenderRaces(
	t *testing.T,
	prior, current testreleaserender.ReleaseRenderInput,
	mixed *agentpb.ComposeArtifact,
) {
	t.Helper()
	ctx := context.Background()
	for _, render := range []testreleaserender.ReleaseRenderInput{prior, current} {
		raw, err := testreleaserender.EncodeReleaseRenderInput(render)
		if err != nil {
			t.Fatal(err)
		}
		value, err := testreleases.EncodeReleaseRecord("release-render-input", json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.Put(ctx, testreleases.ReleaseRenderInputStagingKey("", render.ReleaseID), value); err != nil {
			t.Fatal(err)
		}
	}
	serving, err := fixture.Ledger.ResolveServing(ctx, current.EnvironmentID, current.ServiceID, 0)
	if err != nil {
		t.Fatal(err)
	}
	serving.Intent.Strategy, serving.Intent.Slot, serving.Intent.CandidateWorkload = current.Strategy, current.Slot, current.CandidateWorkload
	serving.Intent.PriorServingReleaseID = prior.ReleaseID
	raw, err := testreleaserender.EncodeReleaseRenderInput(current)
	if err != nil {
		t.Fatal(err)
	}
	serving.Intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	value, err := testreleases.EncodeReleaseRecord("release-intent", serving.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(ctx, testreleases.ReleaseIntentStagingKey("", current.ReleaseID), value); err != nil {
		t.Fatal(err)
	}
	for pruneIndex, prune := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "inactive-render-replaced", true: "inactive-render-pruned"}[prune],
			func(t *testing.T) {
				scope, err := fixture.Ledger.LoadPlanningScope(ctx, current.EnvironmentID)
				if err != nil {
					t.Fatal(err)
				}
				planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{current.ServiceID})
				if err != nil {
					t.Fatal(err)
				}
				applied, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
				if err != nil {
					t.Fatal(err)
				}
				serving, err := fixture.Ledger.ResolveServing(
					ctx,
					current.EnvironmentID,
					current.ServiceID,
					scope.ReadRevision,
				)
				if err != nil {
					t.Fatal(err)
				}
				active, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, current.ReleaseID, scope.ReadRevision)
				if err != nil {
					t.Fatal(err)
				}
				inactive, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, prior.ReleaseID, scope.ReadRevision)
				if err != nil {
					t.Fatal(err)
				}
				release := testreleaserender.ServiceLifecycleRelease{
					ServingReleaseID:            current.ReleaseID,
					ProjectionRevision:          serving.ProjectionRevision,
					IntentRevision:              serving.IntentRevision,
					RenderRevision:              active.Revision,
					Current:                     active.Record,
					PriorServingReleaseID:       prior.ReleaseID,
					RetainedPrior:               &inactive.Record,
					RetainedPriorRenderRevision: inactive.Revision,
				}
				task := fixture.Task(t, int64(1480+pruneIndex))
				projection := retainedRaceProjection(t, fixture, task.ID, mixed)
				task.RenderGeneration = int32(projection.RenderGeneration)
				publication, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
					ctx,
					etcd.BlueprintReleasePublication{},
					task,
					applied,
					[]etcd.BlueprintRetainedRuntimeSource{{Planning: planning[0], Release: &release, Intent: &serving}},
					mixed,
				)
				if err != nil {
					t.Fatal(err)
				}
				key := testreleases.ReleaseRenderInputStagingKey("", prior.ReleaseID)
				before, err := fixture.store.Get(ctx, key)
				if err != nil || before.Entry == nil {
					t.Fatalf("inactive source absent: %v", err)
				}
				mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: before.Entry.Value}
				if prune {
					mutation.Type = testkeyvalue.MutationDelete
					mutation.Value = nil
				}
				if _, err := fixture.store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
					t.Fatal(err)
				}
				assertRetainedFinalPublicationRejected(
					t,
					fixture,
					task,
					projection,
					publication,
					"inactive-render",
					prune,
				)
				if _, err := fixture.store.Put(ctx, key, before.Entry.Value); err != nil {
					t.Fatal(err)
				}
			},
		)
	}
}

func retainedRaceProjection(
	t *testing.T,
	fixture *ExecutedArtifactFixture,
	taskID string,
	mixed *agentpb.ComposeArtifact,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(
		t.Context(),
		fixture.Environment.Record.ID,
	)
	if err != nil || !found {
		t.Fatalf("read desired projection for retained race: found=%t error=%v", found, err)
	}
	projection := testenvironmentprojection.CloneEnvironmentComposeProjection(current.Record)
	projection.RevisionID = taskID
	projection.RenderGeneration++
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(mixed)
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact = value
	return projection
}

func assertRetainedFinalPublicationRejected(
	t *testing.T,
	fixture *ExecutedArtifactFixture,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	publication etcd.BlueprintReleasePublication,
	source string,
	prune bool,
) {
	t.Helper()
	beforeHead, found, err := fixture.Hierarchy.GetEnvironmentBlueprintHead(t.Context(), fixture.Environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("read desired head before %s race: found=%t error=%v", source, found, err)
	}
	result, err := fixture.tryPublish(t, task, projection, publication)
	if err != nil {
		t.Fatalf("%s prune=%v final publication: %v", source, prune, err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !errors.Is(conflict, errs.New(errs.KindStateConflict, "")) ||
		outcome == etcd.IdempotencyKnownApplied {
		t.Fatalf(
			"%s prune=%v escaped retained source CAS: outcome=%v conflict=%v error=%v",
			source,
			prune,
			outcome,
			conflict,
			classifyErr,
		)
	}
	afterHead, found, err := fixture.Hierarchy.GetEnvironmentBlueprintHead(t.Context(), fixture.Environment.Record.ID)
	if err != nil || !found || afterHead.Revision != beforeHead.Revision ||
		afterHead.Record.RevisionID != beforeHead.Record.RevisionID {
		t.Fatalf("%s prune=%v changed desired head: found=%t error=%v", source, prune, found, err)
	}
	if _, err := fixture.Tasks.GetTask(t.Context(), task.ID); !errors.Is(err, errs.New(errs.KindTaskNotFound, "")) {
		t.Fatalf("%s prune=%v partially published Task: %v", source, prune, err)
	}
}
