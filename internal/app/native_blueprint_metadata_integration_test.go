package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
)

// BP-10/12: metadata receipts and native work outcomes must commit together,
// with no Agent attempt. A new Blueprint head invalidates a prepared receipt
// before it can publish either outcome. Source revisions and Entry generation
// IDs are supplied: this does not qualify actual Script/body or file retirement.
func TestNativeBlueprintMetadataSettlementFencesSupersession(t *testing.T) {
	for _, kind := range []ids.Kind{ids.KindScript, ids.KindEnvEntry, ids.KindRoute} {
		t.Run(string(kind), func(t *testing.T) {
			store := newMemoryHierarchyStore()
			store.revision = 1
			queue, err := workqueue.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := blueprintunits.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			environmentID := ids.New(ids.KindEnvironment)
			target := blueprintunits.ResourceKey{Kind: kind, ID: ids.New(kind)}
			unit := blueprintunits.Unit{Target: target, Fingerprint: strings.Repeat("b", 64),
				Writes: []blueprintunits.ResourceKey{target}}
			generationID := ids.New(ids.KindConfig)
			for phase := range 2 {
				if phase == 1 {
					if kind == ids.KindRoute {
						unit.Fingerprint = strings.Repeat("c", 64)
					} else {
						unit.Removal, unit.Fingerprint = true, ""
					}
				}
				view := publishNativeBlueprintMetadataWork(t, store, queue, ledger, environmentID, unit, phase == 0)
				prepare := func() workqueue.Change {
					t.Helper()
					snapshot, err := ledger.Load(t.Context(), environmentID)
					if err != nil {
						t.Fatal(err)
					}
					view, err = queue.ReadWork(t.Context(), view.Task.Task.ID, view.Item.Item.ID, snapshot.ReadRevision)
					if err != nil {
						t.Fatal(err)
					}
					var change workqueue.Change
					if kind == ids.KindEnvEntry {
						change, err = blueprintunits.PrepareEntryWorkSettlement(
							snapshot,
							view,
							unit,
							generationID,
							time.Now().UTC(),
						)
					} else {
						change, err = blueprintunits.PrepareDefinitionWorkSettlement(snapshot, view, unit, snapshot.ReadRevision, time.Now().UTC())
					}
					if err != nil {
						t.Fatal(err)
					}
					return change
				}
				change := prepare()
				defer keyvalue.ClearMutationValues(change.Mutations)
				if phase == 1 {
					head, err := store.Get(t.Context(), blueprints.EnvironmentBlueprintHeadKey(environmentID))
					if err != nil || head.Entry == nil {
						t.Fatalf("accepted head unavailable: %v", err)
					}
					other, err := idempotency.EncodeTaskReference(ids.New(ids.KindTask))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.Put(t.Context(), head.Entry.Key, other); err != nil {
						t.Fatal(err)
					}
					clear(other)
					before := store.revision
					result, err := queue.Commit(t.Context(), change)
					if err != nil || result.Succeeded || store.revision != before {
						t.Fatalf("superseded metadata receipt committed: %v", err)
					}
					pending, err := queue.ReadWork(t.Context(), view.Task.Task.ID, view.Item.Item.ID, 0)
					if err != nil || pending.Item.Item.Phase != workexecution.Ready || pending.Item.Item.Result != nil {
						t.Fatalf("supersession partially settled native work: %v", err)
					}
					if _, err := store.Put(t.Context(), head.Entry.Key, head.Entry.Value); err != nil {
						t.Fatal(err)
					}
					clear(head.Entry.Value)
					if result, err := queue.Commit(t.Context(), change); err != nil || result.Succeeded {
						t.Fatalf("restoring identical head bytes revived a stale receipt: %v", err)
					}
					change = prepare()
					defer keyvalue.ClearMutationValues(change.Mutations)
				}
				result, err := queue.Commit(t.Context(), change)
				if err != nil || !result.Succeeded {
					t.Fatalf("native metadata settlement: %v", err)
				}
				snapshot, err := ledger.Load(t.Context(), environmentID)
				if err != nil || len(snapshot.Executions) != 0 || len(snapshot.Applied) != 1 {
					t.Fatalf("Controller metadata invented Agent execution: %v", err)
				}
				applied := snapshot.Applied[0]
				want := blueprintunits.Applied
				if unit.Removal {
					want = blueprintunits.Absent
				}
				completed, err := queue.ReadWork(
					t.Context(),
					view.Task.Task.ID,
					view.Item.Item.ID,
					snapshot.ReadRevision,
				)
				if err != nil || completed.Item.Item.Phase != workexecution.Settled ||
					completed.Item.Item.Outcome != workexecution.Succeeded || completed.Item.Item.AttemptNumber != 0 ||
					applied.Record.State != want || applied.Revision != completed.Item.Revision ||
					kind == ids.KindEnvEntry && applied.Record.ControllerValueGenerationID != generationID {
					t.Fatalf("native work and exact metadata receipt were not atomic: %v", err)
				}
				selection, err := blueprintunits.Select(snapshot)
				if err != nil || len(selection.Satisfied) != 1 || len(selection.Ready) != 0 {
					t.Fatalf("settled metadata remains executable: %v", err)
				}
			}
		})
	}
}

func publishNativeBlueprintMetadataWork(t *testing.T, store *memoryHierarchyStore, queue *workqueue.Repository,
	ledger *blueprintunits.Repository, environmentID string, unit blueprintunits.Unit, initial bool,
) workqueue.View {
	t.Helper()
	at := time.Now().UTC()
	task := workexecution.Task{
		ID:     ids.New(ids.KindTask),
		Actor:  workexecution.OperatorActor,
		Action: workexecution.ActionApply,
		Scope: workexecution.Scope{
			TenantID:      ids.New(ids.KindTenant),
			ProjectID:     ids.New(ids.KindProject),
			EnvironmentID: environmentID,
		},
		TargetID:      environmentID,
		TargetName:    "metadata",
		ExpectedItems: 1,
		CreatedAt:     at,
		UpdatedAt:     at,
		Deadline:      at.Add(time.Hour),
	}
	_, ciphertext := manualJourneyEncryptedValue(t, "captured metadata intent")
	defer clear(ciphertext)
	digest := sha256.Sum256(ciphertext)
	task.Intent = workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: hex.EncodeToString(digest[:])}
	value, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), blueprints.EnvironmentBlueprintHeadKey(environmentID), value); err != nil {
		t.Fatal(err)
	}
	clear(value)
	if initial {
		seedNativeBlueprintInitialAbsence(t, store, environmentID, task.ID, unit.Target)
	}
	snapshot, err := ledger.Load(t.Context(), environmentID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := blueprintunits.PrepareDesiredPlan(snapshot, blueprintunits.DesiredPlan{
		EnvironmentID: environmentID, ParentTaskID: task.ID, Complete: true, Units: []blueprintunits.Unit{unit},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Clear()
	input := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	operation := workexecution.EntrySettlement
	if unit.Target.Kind == ids.KindEnvEntry {
		input, err = blueprintunits.EntryWorkInput(task, unit)
	} else {
		operation = workexecution.ResourceDefinition
		input, err = blueprintunits.DefinitionWorkInput(task, unit)
	}
	if err != nil {
		t.Fatal(err)
	}
	graph, err := blueprintcoordinator.BuildWorkGraph(task, []blueprintcoordinator.PreparedUnit{{Unit: unit,
		Operation: operation, Placement: workexecution.Placement{Executor: workexecution.Controller}, Input: &input}})
	if err != nil {
		t.Fatal(err)
	}
	item := graph.Items[0]
	captured, err := blueprintunits.PrepareCapturedGraph(task, graph)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(captured.Mutations)
	body, err := json.Marshal(api.TaskAccepted{TaskID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	marker := idempotency.IdempotencyMarker{
		Kind:  idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   environmentID,
			Method:    "POST",
			Route:     "/environments/{id}/blueprint",
			Key:       task.ID,
		},
		Intent: idempotency.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: task.Intent.SHA256, Ciphertext: ciphertext},
		Response:  idempotency.IdempotencyResponse{Status: 202, ContentKind: "application/json", Body: body},
		TaskID:    task.ID,
		CreatedAt: at,
		UpdatedAt: at,
	}
	publication, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(mutations)
	result, err := queue.Commit(t.Context(), publication, captured,
		workqueue.Change{Conditions: plan.Conditions(), Mutations: plan.Mutations()},
		workqueue.Change{Conditions: conditions, Mutations: mutations})
	if err != nil || !result.Succeeded {
		t.Fatalf("publish native metadata work: %v", err)
	}
	view, err := queue.ReadWork(t.Context(), task.ID, item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return view
}
