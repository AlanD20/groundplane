package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	desiredstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-55: a never-applied Entry is retired in the same transaction as its
// already-settled Controller work. No Agent, host lease or fabricated attempt
// is needed, and replay cannot retire another value generation.
func TestNativeEntryDefinitionRemovalSettlesWithoutAgent(t *testing.T) {
	f := newNativeEntryDefinitionFixture(t)
	result, err := f.hierarchy.PublishEntryConfigurationWork(t.Context(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("Entry removal acceptance: %v, %v, %v", outcome, conflict, err)
	}
	queue, err := workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	view, err := queue.ReadWork(t.Context(), f.request.Task.ID, f.request.Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Item.Item.Phase != workexecution.Settled || view.Item.Item.Outcome != workexecution.Succeeded ||
		view.Item.Item.AttemptNumber != 0 || view.Item.Item.Input != nil || view.Task.Task.Progress.Succeeded != 1 {
		t.Fatal("definition-only removal manufactured host execution or remained unsettled")
	}
	projection, found, err := f.hierarchy.GetEnvironmentComposeProjection(t.Context(), f.request.Environment.Record.ID)
	if err != nil || !found || len(projection.Record.Entries) != 0 ||
		projection.Record.RevisionID != f.request.Task.ID {
		t.Fatalf("Entry removal did not publish its captured desired head: %v", err)
	}
	for _, key := range []string{entries.BlueprintEntryEnvironmentPrefix + f.entryID, f.valueKey} {
		read, err := f.store.Get(t.Context(), key)
		if err != nil || read.Entry != nil {
			t.Fatalf("Entry retirement retained %s: %v", key, err)
		}
	}
	beforeReplay := f.store.revision
	if _, err := f.hierarchy.PublishEntryConfigurationWork(t.Context(), f.request); err != nil {
		t.Fatalf("same removal did not replay after retirement: %v", err)
	}
	if f.store.revision != beforeReplay {
		t.Fatal("removal replay changed durable state")
	}
}

// TASK-55: definition-only settlement is unavailable if acknowledged files
// contain the target, or if the acknowledged head changes before atomic commit.
func TestNativeEntryDefinitionRemovalRejectsAppliedOrChangedConfiguration(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		name := "applied source"
		if concurrent {
			name = "new acknowledgement during commit"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeEntryDefinitionFixture(t)
			head := stageNativeEntryDefinitionConfiguration(t, f, !concurrent)
			publisher := f.hierarchy
			var race *nativeEntryDefinitionRaceStore
			if concurrent {
				race = &nativeEntryDefinitionRaceStore{
					memoryHierarchyStore: f.store,
					taskKey:              workqueue.TaskKey(f.request.Task.ID),
				}
				race.before = func() {
					if _, err := f.store.Put(t.Context(), runtimeconfiguration.HeadKey(f.request.Environment.Record.ID), head); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				publisher, err = etcd.NewHierarchyRepository(race)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				revision, err := f.store.Put(t.Context(), runtimeconfiguration.HeadKey(f.request.Environment.Record.ID), head)
				if err != nil {
					t.Fatal(err)
				}
				// Match the head revision so refusal proves the selected file
				// source check, not merely a stale acknowledgement number.
				f.request.DefinitionConfigurationRevision = revision
			}
			_, err := publisher.PublishEntryConfigurationWork(t.Context(), f.request)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || race != nil && race.before != nil {
				t.Fatalf("unsafe definition-only removal was not fenced at its intended boundary: %v", err)
			}
			read, err := f.store.Get(t.Context(), workqueue.TaskKey(f.request.Task.ID))
			if err != nil || read.Entry != nil {
				t.Fatalf("rejected Entry removal published execution: %v", err)
			}
			current, found, err := f.hierarchy.GetEnvironmentComposeProjection(
				t.Context(),
				f.request.Environment.Record.ID,
			)
			if err != nil || !found || len(current.Record.Entries) != 1 ||
				current.Record.Entries[0].Entry.ID != f.entryID {
				t.Fatalf("rejected Entry removal changed desired state: %v", err)
			}
			value, err := f.store.Get(t.Context(), f.valueKey)
			if err != nil || value.Entry == nil {
				t.Fatal("rejected Entry removal retired its value")
			}
		})
	}
}

// TASK-55: supplying an initial projection does not permit unsealed bytes, and
// ordinary host settlement still requires the persisted projection to exist.
func TestNativeEntryDefinitionRemovalRequiresExactSealedCandidate(t *testing.T) {
	f := newNativeEntryDefinitionFixture(t)
	inputs, err := entrywork.NewInputRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	view := workqueue.View{Task: workqueue.TaskSnapshot{Task: f.request.Task},
		Item: workqueue.ItemSnapshot{Item: f.request.Item}, ReadRevision: f.store.revision}
	removal := *f.request.Capture.Removal
	if _, err := inputs.PrepareDefinitionRemoval(t.Context(), view, removal, f.request.Claim.RenderGeneration, f.request.Projection); err != nil {
		t.Fatalf("sealed initial candidate was rejected: %v", err)
	}
	changed := environmentprojection.CloneEnvironmentComposeProjection(f.request.Projection)
	additional, err := entries.NewRecord(f.request.Environment.Record.ID, core.EnvEntry{ID: ids.New(ids.KindEnvEntry),
		Kind: core.EntryKindEnv, Key: "UNSEALED", Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "new"},
		Exposure: []string{"all"}}, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	changed.Entries = append(changed.Entries, additional)
	if _, err := inputs.PrepareDefinitionRemoval(t.Context(), view, removal, f.request.Claim.RenderGeneration, changed); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("unsealed projection bytes were accepted: %v", err)
	}
	_, hostItem, err := taskplanning.NewEntryConfigurationWork(f.request.Task.Scope,
		workexecution.Placement{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost)},
		f.request.Claim, workexecution.ActionRemove, f.entryID, "REMOVE")
	if err != nil {
		t.Fatal(err)
	}
	view.Item.Item = hostItem
	if _, err := inputs.PrepareRemoval(t.Context(), view, removal, f.request.Claim.RenderGeneration); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("host settlement accepted missing persisted runtime: %v", err)
	}
}

type nativeEntryDefinitionFixture struct {
	store     *memoryHierarchyStore
	hierarchy *etcd.HierarchyRepository
	request   etcd.EntryWorkPublication
	entryID   string
	valueKey  string
	valueID   string
}

func newNativeEntryDefinitionFixture(t *testing.T) *nativeEntryDefinitionFixture {
	t.Helper()
	ctx := t.Context()
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Second)
	f := &nativeEntryDefinitionFixture{store: newMemoryHierarchyStore()}
	f.store.revision = 1
	var err error
	f.hierarchy, err = etcd.NewHierarchyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "entry-check", Name: "Entry check"}
	nativeEntryDefinitionPut(t, f.store, hierarchy.TenantKey(tenant.ID), tenant, hierarchy.EncodeTenant)
	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "entry-check", Name: "Entry check", Kind: hierarchy.ProjectKindTenant}
	projectRecord := nativeEntryDefinitionPut(
		t,
		f.store,
		hierarchy.ProjectKey(project.ID),
		project,
		hierarchy.EncodeProject,
	)
	environment, err := hierarchy.NewProvisioningEnvironment("/var/lib/groundplane/vol", project,
		ids.New(ids.KindEnvironment), "qa", "10.40.0.0/24", ids.New(ids.KindTask), at.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	environment, err = hierarchy.CompleteEnvironmentProvisioning(environment, environment.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	environmentRecord := nativeEntryDefinitionPut(
		t,
		f.store,
		hierarchy.EnvironmentKey(environment.ID),
		environment,
		hierarchy.EncodeEnvironment,
	)
	epoch := nativeEntryDefinitionPut(
		t,
		f.store,
		hierarchy.EnvironmentMutationEpochKey(environment.ID),
		backupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environment.ID,
		},
		backupruntime.EncodeEnvironmentMutationEpochRecord,
	)
	entry, err := entries.NewRecord(
		environment.ID,
		core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv,
			Key: "REMOVE", Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "value"}, Exposure: []string{"all"}},
		ids.New(ids.KindConfig),
	)
	if err != nil {
		t.Fatal(err)
	}
	f.entryID = entry.Entry.ID
	f.valueID = entry.CurrentValueGenerationID
	f.valueKey = entryvalues.PlainKey(entry.Entry.ID, entry.CurrentValueGenerationID)
	digest := sha256.Sum256([]byte("value"))
	nativeEntryDefinitionPut(t, f.store, f.valueKey, entryvalues.PlainGeneration{
		EnvironmentID: environment.ID, EntryID: entry.Entry.ID, GenerationID: entry.CurrentValueGenerationID,
		Content: []byte(
			"value",
		), PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: at}, entryvalues.EncodePlain)
	baseID := ids.New(ids.KindTask)
	projection, err := desiredauthoring.EmptyProjection(environment, baseID)
	if err != nil {
		t.Fatal(err)
	}
	projection.Entries = []entries.Record{entry}
	owned, err := environmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	authoredEntries, err := entrycontroller.BlueprintAuthoring(projection.Entries)
	if err != nil {
		t.Fatal(err)
	}
	input := environmentprojection.EnvironmentDesiredInput{
		EnvironmentID:    environment.ID,
		RevisionID:       baseID,
		RenderGeneration: 1,
		Input: core.BlueprintDesiredInput{
			NormalizedCompose: projection.NormalizedCompose,
			NetworkPool:       environment.NetworkPool,
			Entries:           authoredEntries,
		},
	}
	seed := agentConfigurationTestMarker()
	seed.CreatedAt, seed.UpdatedAt = at.Add(-time.Second), at.Add(-time.Second)
	seed.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environment.ID, Method: http.MethodPost, Route: "/environments/{id}/blueprint", Key: "entry-definition-baseline"}
	baseline, err := desiredauthoring.PrepareValues(input, projection, owned, seed, "", 0, environmentRecord.Revision)
	if err != nil {
		t.Fatal(err)
	}
	baseline.Mutations = append(baseline.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: entries.BlueprintEntryEnvironmentPrefix + entry.Entry.ID, Value: []byte(environment.ID)})
	committed, err := f.store.Transact(ctx, baseline.Conditions, baseline.Mutations)
	if err != nil || !committed.Succeeded {
		t.Fatalf("Entry baseline publication: %v", err)
	}
	desired, err := desiredstore.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.New(ids.KindTask)
	marker := agentConfigurationTestMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, taskID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environment.ID, Method: http.MethodDelete, Route: "/entries/{id}", Key: "native-entry-definition-removal"}
	marker.ReplayTarget = &idempotency.IdempotencyReplayTarget{
		Kind: idempotency.IdempotencyReplayTargetEntry,
		ID:   entry.Entry.ID,
	}
	marker.Response = idempotency.IdempotencyResponse{
		Status:      202,
		ContentKind: "application/json",
		Body:        []byte(fmt.Sprintf(`{"task_id":%q}`, taskID)),
	}
	claim, err := desired.ClaimEnvironmentBlueprintStage(ctx, blueprints.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: environment.ID, CandidateRevisionID: taskID, CandidateTaskID: taskID, Locator: marker.Locator,
		Intent: marker.Intent, BaselineHeadRevision: committed.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: 2, ProjectionSchema: blueprints.EnvironmentDesiredInputSchema, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	candidate, _, err := composerender.ProjectEnvironmentEntryMutation(
		projection,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID: taskID, ArtifactID: ids.New(ids.KindConfig), PlanID: ids.New(ids.KindPlan), RenderGeneration: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	dependencyDigest, err := blueprints.EnvironmentBlueprintDependencyDigest(candidate)
	if err != nil {
		t.Fatal(err)
	}
	nextInput := environmentprojection.EnvironmentDesiredInput{
		EnvironmentID:    environment.ID,
		RevisionID:       taskID,
		RenderGeneration: 2,
		Input: core.BlueprintDesiredInput{
			NormalizedCompose: candidate.NormalizedCompose,
			NetworkPool:       environment.NetworkPool,
		},
	}
	if _, err := desired.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{Claim: claim,
		DesiredInput: nextInput, DependencyDigest: dependencyDigest, Mutation: &blueprints.EnvironmentDesiredMutationAudit{
			Entry: &blueprints.EnvironmentEntryMutationAudit{Action: blueprints.EnvironmentEntryMutationRemove, EntryID: entry.Entry.ID, BaseRevisionID: baseID}}}); err != nil {
		t.Fatal(err)
	}
	task, item, err := taskplanning.NewEntryConfigurationWork(workexecution.Scope{TenantID: tenant.ID,
		ProjectID: project.ID, EnvironmentID: environment.ID}, workexecution.Placement{Executor: workexecution.Controller},
		claim, workexecution.ActionRemove, entry.Entry.ID, "REMOVE")
	if err != nil {
		t.Fatal(err)
	}
	f.request = etcd.EntryWorkPublication{Project: projectRecord, Environment: environmentRecord, Claim: claim,
		Projection: candidate, EpochRevision: epoch.Revision, Task: task, Item: item, Marker: marker,
		Capture: entrywork.Capture{Removal: &entrywork.Removal{EntryID: entry.Entry.ID, BaseRevisionID: baseID,
			HeadRevision: committed.Revision, DescriptorID: claim.DescriptorID, RevisionID: taskID}}}
	return f
}

func stageNativeEntryDefinitionConfiguration(t *testing.T, f *nativeEntryDefinitionFixture, includeEntry bool) []byte {
	t.Helper()
	configuration := runtimeconfiguration.Snapshot{
		ID:            ids.New(ids.KindConfig),
		EnvironmentID: f.request.Environment.Record.ID,
		Generation:    1,
	}
	if includeEntry {
		content := []byte("REMOVE=value\n")
		digest := sha256.Sum256(content)
		configuration.Files = []taskmaterialization.Record{{EnvironmentID: configuration.EnvironmentID,
			StepID: ids.New(
				ids.KindStep,
			), MaterializationID: ids.New(ids.KindConfig), Destination: "secrets/.env." + configuration.EnvironmentID,
			OutputKind: taskmaterialization.OutputGeneratedEnvironment, Mode: 0o600,
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
			Source: taskmaterialization.Source{Kind: taskmaterialization.SourceGeneratedEnvironment,
				GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{FormatVersion: 1,
					Values: []taskmaterialization.GeneratedEnvironmentEntryReference{{Name: "REMOVE",
						Value: taskmaterialization.EntryValueReference{EntryID: f.entryID, ValueGenerationID: f.valueID,
							Storage: taskmaterialization.EntryValueStoragePlain}}}}}}}
	}
	repository, err := runtimeconfiguration.New(f.store)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.Stage(t.Context(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	head, err := runtimeconfiguration.EncodeReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

type nativeEntryDefinitionRaceStore struct {
	*memoryHierarchyStore
	taskKey string
	before  func()
}

func (store *nativeEntryDefinitionRaceStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if mutation.Key == store.taskKey && store.before != nil {
			before := store.before
			store.before = nil
			before()
			break
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func nativeEntryDefinitionPut[T any](t *testing.T, store *memoryHierarchyStore, key string, record T,
	encode func(T) ([]byte, error),
) keyvalue.Versioned[T] {
	t.Helper()
	raw, err := encode(record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	revision, err := store.Put(t.Context(), key, raw)
	if err != nil {
		t.Fatal(err)
	}
	return keyvalue.Versioned[T]{Record: record, Revision: revision, ReadRevision: revision}
}
