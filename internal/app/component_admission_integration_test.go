package app

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: captured Component input must not be admitted against a removed or
// advanced applied predecessor, or a newly acknowledged file configuration.
func TestNativeComponentAdmissionRejectsChangedPredecessors(t *testing.T) {
	for _, change := range []string{"runtime removed", "runtime advanced", "configuration acknowledged"} {
		t.Run(change, func(t *testing.T) {
			f := newNativeComponentAdmissionFixture(t)
			if _, err := f.inputs.AdmissionConditions(t.Context(), f.view); err != nil {
				t.Fatalf("unchanged captured sources rejected: %v", err)
			}
			switch change {
			case "runtime removed":
				if _, err := f.store.Delete(t.Context(), f.runtimeKey); err != nil {
					t.Fatal(err)
				}
			case "runtime advanced":
				if _, err := f.store.Put(t.Context(), f.runtimeKey, f.runtime); err != nil {
					t.Fatal(err)
				}
			case "configuration acknowledged":
				value, err := runtimeconfiguration.EncodeReference(f.configuration)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.Put(t.Context(), runtimeconfiguration.HeadKey(f.environmentID), value); err != nil {
					t.Fatal(err)
				}
			}
			f.refresh(t)
			if _, err := f.inputs.AdmissionConditions(t.Context(), f.view); !errors.Is(
				err,
				errs.New(errs.KindStateConflict, ""),
			) {
				t.Fatalf("changed %s authorized captured input: %v", change, err)
			}
		})
	}
}

// Rationale: a valid read is not a grant. Its applied-state guards must still
// reject a concurrent write when the actual native transaction is committed.
func TestNativeComponentAdmissionGuardsRejectConcurrentRuntimeAdvance(t *testing.T) {
	f := newNativeComponentAdmissionFixture(t)
	guards, err := f.inputs.AdmissionConditions(t.Context(), f.view)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(t.Context(), f.runtimeKey, f.runtime); err != nil {
		t.Fatal(err)
	}
	result, err := f.queue.Commit(t.Context(), workqueue.Change{Conditions: guards,
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: "/test/component-effect-grant", Value: []byte("granted")},
		}})
	if err != nil || result.Succeeded || f.store.valueAt("/test/component-effect-grant", f.store.revision) != nil {
		t.Fatalf("stale runtime read granted effects: committed=%t, %v", result.Succeeded, err)
	}
}

// Rationale: a newer authored Apply revokes new Component effects without
// changing the captured work; its desired-head compare must close the read race.
func TestNativeComponentDesiredHeadFencesNewEffects(t *testing.T) {
	f := newNativeComponentAdmissionFixture(t)
	current, guards, err := f.inputs.ReadDesiredMatch(t.Context(), f.view)
	if err != nil || !current {
		t.Fatalf("original desired head: current=%t, %v", current, err)
	}
	next, err := idempotency.EncodeTaskReference(ids.New(ids.KindTask))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(t.Context(), blueprints.EnvironmentBlueprintHeadKey(f.environmentID), next); err != nil {
		t.Fatal(err)
	}
	result, err := f.queue.Commit(t.Context(), workqueue.Change{Conditions: guards,
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: "/test/component-stage-grant", Value: []byte("granted")},
		}})
	if err != nil || result.Succeeded || f.store.valueAt("/test/component-stage-grant", f.store.revision) != nil {
		t.Fatalf("superseded desired head granted new stage: committed=%t, %v", result.Succeeded, err)
	}
	f.refresh(t)
	current, _, err = f.inputs.ReadDesiredMatch(t.Context(), f.view)
	if err != nil || current {
		t.Fatalf("new desired head was ignored: current=%t, %v", current, err)
	}
}

type nativeComponentAdmissionFixture struct {
	store         *memoryHierarchyStore
	queue         *workqueue.Repository
	inputs        *componentwork.InputRepository
	view          workqueue.View
	environmentID string
	runtimeKey    string
	runtime       []byte
	configuration runtimeconfiguration.Reference
}

func (f *nativeComponentAdmissionFixture) refresh(t *testing.T) {
	t.Helper()
	view, err := f.queue.ReadWork(t.Context(), f.view.Task.Task.ID, f.view.Item.Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.view = view
}

func newNativeComponentAdmissionFixture(t *testing.T) *nativeComponentAdmissionFixture {
	t.Helper()
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	id := func(kind ids.Kind, seed int64) string { return ids.NewAt(kind, at, seed) }
	store := newMemoryHierarchyStore()
	store.revision = 1
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := componentwork.NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := workexecution.Scope{
		TenantID:      id(ids.KindTenant, 1),
		ProjectID:     id(ids.KindProject, 2),
		EnvironmentID: id(ids.KindEnvironment, 3),
	}
	placement := workexecution.Placement{Executor: workexecution.Agent, HostID: id(ids.KindHost, 4)}
	component, err := components.NewRecord(core.Component{ID: id(ids.KindComponent, 5),
		Owner: core.ComponentOwnerEnvironment, OwnerID: scope.EnvironmentID, Kind: core.ComponentKindIngressCaddy})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := components.EncodeRecord(component)
	if err != nil {
		t.Fatal(err)
	}
	componentRevision, err := store.Put(t.Context(), components.RecordPrefix+component.Desired.ID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	prior := componentplanning.Runtime{EnvironmentID: scope.EnvironmentID, Component: component,
		RevisionID: id(ids.KindTask, 6), RenderGeneration: 1,
		Execution: blueprintunits.AppliedExecution{TaskID: id(ids.KindTask, 6), WorkItemID: id(ids.KindWorkItem, 7),
			AttemptID: id(ids.KindAttempt, 8), Executor: workexecution.Identity{Executor: workexecution.Agent,
				HostID: placement.HostID, AgentID: id(ids.KindAgent, 9), Generation: 1},
			Input: workexecution.Input{
				ID:     id(ids.KindPlan, 10),
				SHA256: digest,
			}, Result: workexecution.Input{ID: id(ids.KindPlan, 11), SHA256: digest}}}
	runtime, err := componentplanning.EncodeRuntime(prior)
	if err != nil {
		t.Fatal(err)
	}
	runtimeKey := componentplanning.RuntimeKey(component.Desired.ID)
	runtimeRevision, err := store.Put(t.Context(), runtimeKey, runtime)
	if err != nil {
		t.Fatal(err)
	}
	configurations, err := runtimeconfiguration.New(store)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := configurations.Stage(
		t.Context(),
		runtimeconfiguration.Snapshot{ID: id(ids.KindConfig, 12), EnvironmentID: scope.EnvironmentID, Generation: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: id(ids.KindTask, 13), Actor: workexecution.OperatorActor, Scope: scope,
		Action: workexecution.ActionApply, TargetID: scope.EnvironmentID, Intent: workexecution.Input{ID: id(ids.KindPlan, 14), SHA256: digest},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute), ExpectedItems: 1}
	item := workexecution.Item{ID: id(ids.KindWorkItem, 15), TaskID: task.ID, Operation: workexecution.Component,
		TargetID: component.Desired.ID, Placement: placement, Phase: workexecution.Waiting, Wait: &workexecution.Wait{Reason: workexecution.WaitInput},
		Claims: []workexecution.Claim{
			{ScopeID: scope.EnvironmentID},
			{HostID: placement.HostID, ScopeID: scope.EnvironmentID},
		},
		Cleanup: workexecution.CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	ciphertext := []byte("protected-component-intent")
	protectedDigest := sha256.Sum256(ciphertext)
	marker := idempotency.IdempotencyMarker{
		Kind:  idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   scope.EnvironmentID,
			Method:    http.MethodPost,
			Route:     "/environments/{id}/blueprint",
			Key:       "component-admission-0001",
		},
		Intent: idempotency.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(protectedDigest[:]), Ciphertext: ciphertext},
		Response: idempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        []byte("{\"task_id\":\"" + task.ID + "\"}"),
		},
		TaskID:    task.ID,
		CreatedAt: at,
		UpdatedAt: at,
	}
	publication, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		t.Fatal(err)
	}
	head, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	publication.Mutations = append(
		publication.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(scope.EnvironmentID),
			Value: head,
		},
	)
	committed, err := queue.Commit(t.Context(), publication)
	if err != nil || !committed.Succeeded {
		t.Fatalf("fixture admission: %v, %v", committed.Succeeded, err)
	}
	view, err := queue.ReadWork(t.Context(), task.ID, item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	predecessor, err := componentwork.CaptureRuntimePredecessor(prior)
	if err != nil {
		t.Fatal(err)
	}
	input := &agentpb.ComponentWorkInput{OperationId: id(ids.KindOperation, 16), EnvironmentId: scope.EnvironmentID,
		ComponentId: component.Desired.ID, RenderGeneration: 2, Remove: true, Predecessor: &agentpb.ComponentWorkPredecessor{Absent: true}}
	capture := componentwork.Capture{Current: component, Candidate: component, CurrentRevision: componentRevision,
		RuntimeRevision: runtimeRevision, Predecessor: &predecessor, Configuration: runtimeconfiguration.WorkCapture{
			TaskID: task.ID, WorkItemID: item.ID, Placement: placement, ReadRevision: view.ReadRevision, Generation: 2, Current: reference}}
	sealed, change, err := inputs.Stage(t.Context(), view, id(ids.KindPlan, 17), input, capture)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := workqueue.PrepareInput(view.Task, view.Item, sealed, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err = queue.Commit(t.Context(), change, ready)
	if err != nil || !committed.Succeeded {
		t.Fatalf("fixture input publication: %v, %v", committed.Succeeded, err)
	}
	f := &nativeComponentAdmissionFixture{store: store, queue: queue, inputs: inputs, view: view,
		environmentID: scope.EnvironmentID, runtimeKey: runtimeKey, runtime: runtime, configuration: reference}
	f.refresh(t)
	return f
}
