package etcd_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func proveEntryServingPublication(t *testing.T, fixture *etcd.ExecutedArtifactFixture,
	current etcd.Versioned[etcd.EnvironmentComposeProjection], candidate etcd.EnvironmentComposeProjection,
	task etcd.TaskRecord, race string,
) {
	t.Helper()
	ctx := t.Context()
	marker := fixture.EntryRuntimeMarker(t, task)
	desired, err := desiredstore.NewRepository(fixture.EntryRemovalStore())
	if err != nil {
		t.Fatal(err)
	}
	claim, err := desired.ClaimEnvironmentBlueprintStage(ctx, etcd.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: task.Target, CandidateRevisionID: task.ID, CandidateTaskID: task.ID,
		Locator: marker.Locator, Intent: marker.Intent, BaselineHeadRevision: current.Revision,
		SourceKind: etcd.EnvironmentBlueprintSourceMutation, RenderGeneration: candidate.RenderGeneration,
		ProjectionSchema: etcd.EnvironmentDesiredProjectionSchema, CreatedAt: task.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := etcd.EnvironmentBlueprintDependencyDigest(candidate)
	if err != nil {
		t.Fatal(err)
	}
	entry := candidate.Entries[0]
	if _, err := desired.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim, Projection: candidate, DependencyDigest: digest,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Entry: &etcd.EnvironmentEntryMutationAudit{
			Action: etcd.EnvironmentEntryMutationCreate, BaseRevisionID: current.Record.RevisionID,
			EntryID: entry.Entry.ID, Record: &entry,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	store := &entryRuntimePublicationStore{Store: fixture.EntryRemovalStore()}
	publicationRace := race == "before publication" || race == "at commit" ||
		race == "source before publication" || race == "source at commit"
	if race == "before publication" {
		fixture.AdvanceEntryRuntimeEpoch(t)
	} else if race == "at commit" {
		store.before = func() { fixture.AdvanceEntryRuntimeEpoch(t) }
	} else if race == "source before publication" {
		fixture.AdvanceAcknowledgedRuntime(t, task.EntryRuntime.Updates[0].ServiceID)
	} else if race == "source at commit" {
		store.before = func() { fixture.AdvanceAcknowledgedRuntime(t, task.EntryRuntime.Updates[0].ServiceID) }
	}
	publisher, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	publish := func() (etcd.IdempotencyTransactionResult, error) {
		return publisher.PublishEnvironmentDesiredRevisionWithTask(ctx, fixture.Project, fixture.Environment,
			current.Revision, claim,
			etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: task.Target, RevisionID: task.ID},
			candidate, nil, nil, nil, etcd.ReleaseGroupBlueprintPreparedMutation{},
			etcd.ComponentTaskPreparation{}, etcd.BlueprintAttachTaskPreparation{}, task, marker)
	}
	result, err := publish()
	if publicationRace {
		_, _, conflict, classifyErr := result.Classify()
		kind, _ := errs.KindOf(err)
		conflictKind, _ := errs.KindOf(conflict)
		if kind == errs.KindStateConflict || classifyErr == nil && conflictKind == errs.KindStateConflict {
			latest, found, readErr := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, task.Target)
			if readErr != nil || !found || latest.Revision != current.Revision {
				t.Fatal("rejected Entry capture changed desired state", readErr)
			}
			if _, readErr := fixture.Tasks.GetTask(ctx, task.ID); !errs.IsNotFound(readErr) {
				t.Fatal("rejected Entry capture published a Task", readErr)
			}
			return
		}
		t.Fatalf("Entry publication accepted stale serving capture: %v / %v / %v", err, conflict, classifyErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("Entry publication=%v, %v, %v", outcome, conflict, err)
	}
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", fixture.Hierarchy, nil)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := fixture.Tasks.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal("read published Entry Task", err)
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, stored.Record)
	if err != nil || hex.EncodeToString(plan.GetPlanHash()) != task.PlanHash {
		t.Fatal("published Entry plan did not reconstruct its captured runtime", err)
	}
	before := fixture.ReadRevision()
	if _, err := publish(); err != nil || fixture.ReadRevision() != before {
		t.Fatal("Entry publication replay wrote or failed", err)
	}
	completeEntryRuntimePublication(t, fixture, task, race)
}

type entryRuntimePublicationStore struct {
	etcd.Store
	before func()
}

func (store *entryRuntimePublicationStore) Transact(ctx context.Context, conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	if store.before != nil {
		before := store.before
		store.before = nil
		before()
	}
	return store.Store.Transact(ctx, conditions, mutations)
}
