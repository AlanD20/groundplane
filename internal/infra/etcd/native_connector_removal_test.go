package etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CON-05/TASK-54: native removal must settle its exact definition, encrypted
// credentials, indexes and fence atomically, and preserve acceptance on replay.
// Cancellation releases only the fence; no remote object deletion is authorized.
func TestNativeConnectorRemovalSettlesCapturedResourceAndReplays(t *testing.T) {
	for _, outcome := range []workexecution.Outcome{workexecution.Succeeded, workexecution.Cancelled} {
		t.Run(string(outcome), func(t *testing.T) {
			fixture := nativeConnectorRemovalFixture(t)
			fixture.publish(t)
			queue, owner := fixture.owners(t)
			view, err := queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, 0)
			if err != nil || view.Item.Item.Phase != workexecution.Ready || !view.Task.Task.PublicationComplete {
				t.Fatalf("removal did not publish executable native work: %v", err)
			}
			change, err := owner.PrepareDecision(t.Context(), view, outcome, fixture.task.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(change.Mutations)
			committed, err := queue.Commit(t.Context(), change)
			if err != nil || !committed.Succeeded {
				t.Fatalf("settle captured Connector: %v", err)
			}
			fixture.assertResource(t, outcome != workexecution.Succeeded, false)
			view, err = queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, 0)
			if err != nil || view.Item.Item.Phase != workexecution.Settled || view.Item.Item.Outcome != outcome ||
				view.Item.Revision != committed.Revision || view.Task.Revision != committed.Revision ||
				view.Task.Task.Progress.Unsettled() != 0 {
				t.Fatalf("resource deletion and native settlement were not joined: %#v, %v", view, err)
			}
			before := fixture.store.revision
			restarted, err := newConnectorRepository(fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := restarted.PublishConnectorRemoval(t.Context(), fixture.environment, fixture.project,
				fixture.current, fixture.input, fixture.task, fixture.marker)
			if err != nil || replay.kind != idempotencyTransactionExisting || fixture.store.revision != before {
				t.Fatalf("exact replay lost acceptance or wrote after settlement: %v", err)
			}
			if repeated, err := queue.Commit(t.Context(), change); err != nil || repeated.Succeeded {
				t.Fatalf("stale settlement changed the completed resource: %v", err)
			}
		})
	}
}

// CON-05/BAK-09: policy, Recovery Point, orphan and connection-check retention
// must protect credentials at admission and finalization, including a reference
// acquired after finalization's read. Corrupt membership must fail closed.
func TestNativeConnectorRemovalReferenceFences(t *testing.T) {
	for _, reference := range []string{"policy", "point", "orphan", "connection-check", "malformed-point"} {
		for _, timing := range []string{"before-acceptance", "before-settlement", "after-settlement-read"} {
			t.Run(reference+"/"+timing, func(t *testing.T) {
				fixture := nativeConnectorRemovalFixture(t)
				key, value := fixture.reference(t, reference)
				put := func() {
					t.Helper()
					if _, err := fixture.store.Transact(t.Context(), nil, []keyvalue.Mutation{{
						Type: keyvalue.MutationPut, Key: key, Value: value,
					}}); err != nil {
						t.Fatal(err)
					}
				}
				want := errs.KindResourceInUse
				if reference == "malformed-point" {
					want = errs.KindInternal
				}
				if timing == "before-acceptance" {
					put()
					before := fixture.store.revision
					_, err := fixture.repository.PublishConnectorRemoval(
						t.Context(),
						fixture.environment,
						fixture.project,
						fixture.current,
						fixture.input,
						fixture.task,
						fixture.marker,
					)
					if !isKind(err, want) || fixture.store.revision != before {
						t.Fatalf("referenced Connector removal wrote or returned wrong failure: %v", err)
					}
					fixture.assertResource(t, true, false)
					return
				}
				fixture.publish(t)
				queue, owner := fixture.owners(t)
				if timing == "before-settlement" {
					put()
				}
				view, err := queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, 0)
				if err != nil {
					t.Fatal(err)
				}
				before := fixture.store.revision
				change, err := owner.PrepareDecision(
					t.Context(),
					view,
					workexecution.Succeeded,
					fixture.task.CreatedAt.Add(time.Second),
				)
				defer keyvalue.ClearMutationValues(change.Mutations)
				if timing == "before-settlement" {
					if !isKind(err, want) || fixture.store.revision != before {
						t.Fatalf("referenced Connector finalization wrote or returned wrong failure: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					put()
					before = fixture.store.revision
					if result, err := queue.Commit(t.Context(), change); err != nil || result.Succeeded || fixture.store.revision != before {
						t.Fatalf("late reference lost its Connector or settled work: %v", err)
					}
				}
				fixture.assertResource(t, true, true)
				pending, err := queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, 0)
				if err != nil || pending.Item.Item.Phase != workexecution.Ready ||
					pending.Task.Task.Progress.Ready != 1 {
					t.Fatalf("refused finalization partially settled work: %v", err)
				}
			})
		}
	}
}

type nativeConnectorFixture struct {
	store       *memoryHierarchyStore
	repository  *ConnectorRepository
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord]
	project     keyvalue.Versioned[hierarchy.ProjectRecord]
	current     keyvalue.Versioned[connectors.Record]
	input       connectorremoval.Input
	task        workexecution.Task
	marker      idempotency.IdempotencyMarker
}

func nativeConnectorRemovalFixture(t *testing.T) nativeConnectorFixture {
	t.Helper()
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := environment.Record.CreatedAt.Add(time.Minute)
	record := testConnectorRecord(t, environment.Record.ID, at, 7100, "native-removal")
	credentials, err := connectors.NewEncryptedCredentials(record.Connector.ID, []byte("opaque-captured-credentials"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.CreateConnector(t.Context(), environment, project, record, credentials)
	if err != nil {
		t.Fatal(err)
	}
	fixture := nativeConnectorFixture{
		store:       store,
		repository:  repository,
		environment: environment,
		project:     project,
		current:     current,
	}
	fixture.capture(t, at)
	return fixture
}

func (fixture *nativeConnectorFixture) capture(t *testing.T, at time.Time) {
	t.Helper()
	input, err := fixture.repository.CaptureConnectorRemoval(
		t.Context(),
		fixture.environment,
		fixture.project,
		fixture.current,
		ids.New(ids.KindTask),
		ids.New(ids.KindWorkItem),
		ids.New(ids.KindPlan),
		at,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := input.Reference()
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionRemove,
		Scope: workexecution.Scope{
			TenantID:      fixture.project.Record.TenantID,
			ProjectID:     fixture.project.Record.ID,
			EnvironmentID: fixture.environment.Record.ID,
		},
		TargetID: fixture.current.Record.Connector.ID, TargetName: fixture.current.Record.Connector.Name, Intent: ref,
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute), ExpectedItems: 1}
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, task.ID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: fixture.environment.Record.ID, Method: "DELETE", Route: connectorDeletionRoute, Key: "native-connector-removal-0001"}
	marker.ReplayTarget = &idempotency.IdempotencyReplayTarget{
		Kind: idempotency.IdempotencyReplayTargetConnector,
		ID:   fixture.current.Record.Connector.ID,
	}
	marker.Response.Status = 202
	marker.Response.Body = []byte(fmt.Sprintf(`{"task_id":%q}`, task.ID))
	fixture.input, fixture.task, fixture.marker = input, task, marker
}

func (fixture nativeConnectorFixture) publish(t *testing.T) {
	t.Helper()
	result, err := fixture.repository.PublishConnectorRemoval(t.Context(), fixture.environment, fixture.project,
		fixture.current, fixture.input, fixture.task, fixture.marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish native Connector removal: %v", err)
	}
}

func (fixture nativeConnectorFixture) owners(t *testing.T) (*workqueue.Repository, *connectorremoval.Repository) {
	t.Helper()
	queue, err := workqueue.NewRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := connectorremoval.NewRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return queue, owner
}

func (fixture nativeConnectorFixture) assertResource(t *testing.T, present, fenced bool) {
	t.Helper()
	id := fixture.current.Record.Connector.ID
	for _, key := range []string{connectors.RecordKey(id), connectors.CredentialValueKey(id),
		connectors.ConnectorEnvironmentKey(fixture.environment.Record.ID, id),
		connectors.ConnectorNameKey(fixture.environment.Record.ID, fixture.current.Record.Connector.Name)} {
		read, err := fixture.store.Get(t.Context(), key)
		if err != nil || (read.Entry != nil) != present {
			t.Fatalf("Connector resource %s presence differs from %t: %v", key, present, err)
		}
	}
	read, err := fixture.store.Get(t.Context(), deletions.TombstoneKey(string(deletions.DeletionTargetConnector), id))
	if err != nil || (read.Entry != nil) != fenced {
		t.Fatalf("Connector fence presence differs from %t: %v", fenced, err)
	}
}

func (fixture nativeConnectorFixture) reference(t *testing.T, kind string) (string, []byte) {
	t.Helper()
	id := fixture.current.Record.Connector.ID
	pointID := ids.New(ids.KindRecoveryPoint)
	switch kind {
	case "policy":
		return backuppolicy.BackupPolicyConnectorReferenceKey(
				id,
				fixture.environment.Record.ID,
			), []byte(
				fixture.environment.Record.ID,
			)
	case "point", "malformed-point":
		key, err := backupruntime.BackupRecoveryPointConnectorIndexKey(id, pointID)
		if err != nil {
			t.Fatal(err)
		}
		if kind == "malformed-point" {
			return key, []byte("not-a-point")
		}
		return key, []byte(pointID)
	case "orphan":
		key, err := backupruntime.BackupOrphanConnectorIndexKey(id, pointID)
		if err != nil {
			t.Fatal(err)
		}
		return key, []byte(pointID)
	default:
		taskID := ids.New(ids.KindTask)
		return connectorchecks.ReferenceKey(id, taskID), []byte(taskID)
	}
}
