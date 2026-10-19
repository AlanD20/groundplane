package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-03/07/TASK-54: resource readiness, runtime ownership and native progress
// settle together. Failed creation keeps the allocation and Retry reopens the
// same Task/work/input, not a replacement Task. Host observations are supplied.
func TestRunnerNativeSettlementAndSameWorkRetry(t *testing.T) {
	for _, effects := range []runnerwork.EffectState{runnerwork.RuntimeReady, runnerwork.NoEffects, runnerwork.EffectsUnknown} {
		t.Run(string(effects), func(t *testing.T) {
			store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
			environmentID := runnerTestEnvironment(t, store, tenantID, projectID)
			desired := runnerTestDesired(900, runners.RunnerOwnerEnvironment, environmentID, tenantID)
			desired.ParentProjectID = projectID
			fixture, err := prepareRunnerCreationFixture(
				t,
				repository,
				runnerTestAllocationConfig(),
				desired,
				901,
				"runner-native-settlement",
			)
			if err != nil {
				t.Fatal(err)
			}
			fixture.publish(t, repository)
			queue, owner, view := startRunnerStorageWork(t, store, fixture)
			input, result := recordRunnerStorageResult(t, owner, view, effects)
			resource, err := repository.PrepareRunnerCreationResult(t.Context(), input, result)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(resource.Mutations)
			completion, err := result.Record.Completion(input.Record)
			if err != nil {
				t.Fatal(err)
			}
			ending, err := queue.ReadCompletionAt(t.Context(), view)
			if err != nil {
				t.Fatal(err)
			}
			ending.Result, ending.At = completion, result.Record.RecordedAt
			settled, err := workqueue.PrepareCompletion(ending)
			if err != nil {
				t.Fatal(err)
			}
			commitRunnerStorageWork(t, queue, resource, settled)
			current, err := repository.GetRunner(t.Context(), desired.ID)
			if err != nil {
				t.Fatal(err)
			}
			finished, err := queue.ReadResume(
				t.Context(),
				input.Record.TaskID,
				input.Record.WorkItemID,
				view.Attempt.Attempt.ID,
				view.Attempt.Attempt.Executor,
			)
			if err != nil {
				t.Fatal(err)
			}
			if current.Record.LifecycleRevision != finished.Work.Item.Revision ||
				finished.Work.Task.Revision != finished.Work.Item.Revision || finished.Attempt.Revision != finished.Work.Item.Revision {
				t.Fatal("Runner lifecycle, attempt and work progress did not settle atomically")
			}
			if _, err := owner.LoadResult(t.Context(), finished.Attempt.Attempt, 0); err != nil {
				t.Fatal(err)
			}
			assertRunnerAllocationRetained(t, repository, current.Record, fixture.input.Plan.Allocation)
			slots, err := store.Range(
				t.Context(),
				keyvalue.RangeRequest{Prefix: workqueue.SlotKey(view.Attempt.Attempt.Executor, 0), Limit: 1},
			)
			if err != nil || len(slots.Values) != 0 {
				t.Fatalf("finished Runner retained executor capacity: %v", err)
			}
			ownership, found, err := repository.GetRunnerRuntimeOwnership(t.Context(), desired.ID)
			if err != nil {
				t.Fatal(err)
			}
			if effects == runnerwork.RuntimeReady {
				if current.Record.ProvisioningState != runners.RunnerProvisioningReady || !found ||
					ownership.Record.RuntimeEpoch != input.Record.Plan.RuntimeEpoch || ownership.Revision != current.Record.LifecycleRevision ||
					current.Record.ContainerID != result.Record.Evidence.ContainerID || current.Record.Software != result.Record.Evidence.Software {
					t.Fatalf("ready Runner lost its exact runtime: %#v, %#v", current.Record, ownership)
				}
				return
			}
			if current.Record.ProvisioningState != runners.RunnerProvisioningFailed || found {
				t.Fatal("failed creation published a ready runtime")
			}
			retryAt := result.Record.RecordedAt.Add(time.Second)
			retry, err := runnerwork.PrepareRetry(
				input,
				finished.Work.Task,
				finished.Work.Item,
				finished.Attempt,
				retryAt,
			)
			if effects == runnerwork.EffectsUnknown {
				if !isKind(err, errs.KindTaskNotRetryable) {
					t.Fatalf("unknown effects became retryable: %v", err)
				}
				claim := mustOptionalKey(t, store, workqueue.ClaimKey(workexecution.Claim{ScopeID: desired.ID}))
				if claim == nil {
					t.Fatal("unknown effects released their resource claim")
				}
				owner, err := workqueue.DecodeClaimOwner(claim.Value)
				if err != nil || owner.TaskID != fixture.task.ID || owner.WorkItemID != fixture.input.WorkItemID {
					t.Fatalf("unknown effects lost their captured owner: %#v, %v", owner, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(retry.Mutations)
			admission, err := repository.PrepareRunnerRetry(t.Context(), current, input.Record)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(admission.Mutations)
			commitRunnerStorageWork(t, queue, retry, admission)
			reopened, err := queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, 0)
			if err != nil || reopened.Item.Item.Phase != workexecution.Ready ||
				reopened.Item.Item.AttemptNumber != 1 || reopened.Task.Task.Scope != fixture.task.Scope ||
				reopened.Item.Item.Input == nil || *reopened.Item.Item.Input != fixture.task.Intent {
				t.Fatalf("Retry replaced captured identity or scope: %#v, %v", reopened, err)
			}
			prior, err := queue.ReadResume(
				t.Context(),
				fixture.task.ID,
				fixture.input.WorkItemID,
				view.Attempt.Attempt.ID,
				view.Attempt.Attempt.Executor,
			)
			if err != nil || prior.Attempt.Attempt.Outcome != workexecution.Failed ||
				prior.Attempt.Revision != finished.Attempt.Revision {
				t.Fatalf("Retry rewrote prior attempt: %#v, %v", prior.Attempt, err)
			}
			assertRunnerAllocationRetained(t, repository, current.Record, fixture.input.Plan.Allocation)
		})
	}
}

// RUN-03/07: neither a success nor failure may settle a Runner whose scarce
// allocation was lost; an ownership change after preparation must lose the CAS.
func TestRunnerNativeSettlementFencesAllocationOwnership(t *testing.T) {
	keys := []func(runners.RunnerRecord) string{
		func(r runners.RunnerRecord) string {
			return runners.RunnerOwnerKey(r.Desired.OwnerKind, r.Desired.OwnerID, r.Desired.ID)
		},
		func(r runners.RunnerRecord) string {
			return runners.RunnerTenantSlugKey(r.Desired.TenantID, r.Desired.Slug)
		},
		func(r runners.RunnerRecord) string { return runners.RunnerTenantQuotaKey(r.Desired.TenantID) },
		func(r runners.RunnerRecord) string { return runners.RunnerHostSlotKey(r.Allocation.Slot) },
		func(r runners.RunnerRecord) string { return runners.SystemPoolRegistryKey },
	}
	for _, effects := range []runnerwork.EffectState{runnerwork.RuntimeReady, runnerwork.NoEffects} {
		for _, key := range keys {
			t.Run(string(effects)+"/"+key(runners.RunnerRecord{}), func(t *testing.T) {
				store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
				desired := runnerTestDesired(910, runners.RunnerOwnerTenant, tenantID, tenantID)
				fixture, err := prepareRunnerCreationFixture(
					t,
					repository,
					runnerTestAllocationConfig(),
					desired,
					911,
					"runner-native-allocation-fence",
				)
				if err != nil {
					t.Fatal(err)
				}
				fixture.publish(t, repository)
				queue, owner, view := startRunnerStorageWork(t, store, fixture)
				input, result := recordRunnerStorageResult(t, owner, view, effects)
				change, err := repository.PrepareRunnerCreationResult(t.Context(), input, result)
				if err != nil {
					t.Fatal(err)
				}
				defer keyvalue.ClearMutationValues(change.Mutations)
				connectorDeletionDeleteKey(t, store, key(fixture.source.Runner()))
				transaction, err := queue.Commit(t.Context(), change)
				if err != nil || transaction.Succeeded {
					t.Fatalf("lost allocation passed settlement CAS: %t, %v", transaction.Succeeded, err)
				}
				if _, err := repository.PrepareRunnerCreationResult(t.Context(), input, result); !isKind(
					err,
					errs.KindInternal,
				) {
					t.Fatalf("missing allocation passed preparation: %v", err)
				}
			})
		}
	}
}

func startRunnerStorageWork(
	t *testing.T,
	store *memoryHierarchyStore,
	fixture runnerCreationFixture,
) (*workqueue.Repository, *runnerwork.Repository, workqueue.ResumeView) {
	t.Helper()
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runnerwork.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	registered, executor, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{
			Executor: workexecution.Controller,
		}, Capacity: 1,
		Operations: []workexecution.Operation{workexecution.Runner},
	})
	if err != nil {
		t.Fatal(err)
	}
	commitRunnerStorageWork(t, queue, registered)
	placement := workexecution.Placement{Executor: workexecution.Controller}
	current, err := queue.ReadExecutor(t.Context(), placement)
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(current, executor.Identity, true, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	commitRunnerStorageWork(t, queue, available)
	page, err := queue.ScanReady(t.Context(), placement, "")
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("Runner ready queue = %#v, %v", page, err)
	}
	at := fixture.task.CreatedAt.Add(time.Second)
	candidate, err := queue.ReadCandidate(t.Context(), page.Entries[0], executor.Identity, at, nil)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("Runner assignment = %#v, %v", candidate, err)
	}
	request := candidate.Assignment
	request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at, fixture.task.Deadline
	change, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	commitRunnerStorageWork(t, queue, change)
	view, err := queue.ReadResume(t.Context(), fixture.task.ID, fixture.input.WorkItemID, attempt.ID, executor.Identity)
	if err != nil {
		t.Fatal(err)
	}
	source, err := owner.PrepareStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	current, err = queue.ReadExecutor(t.Context(), placement)
	if err != nil {
		t.Fatal(err)
	}
	start, err := workqueue.PrepareStart(view.Work.Task, view.Work.Item, view.Attempt, current, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	commitRunnerStorageWork(t, queue, source, start)
	view, err = queue.ReadResume(t.Context(), fixture.task.ID, fixture.input.WorkItemID, attempt.ID, executor.Identity)
	if err != nil {
		t.Fatal(err)
	}
	return queue, owner, view
}

func recordRunnerStorageResult(
	t *testing.T,
	owner *runnerwork.Repository,
	view workqueue.ResumeView,
	effects runnerwork.EffectState,
) (keyvalue.Versioned[runnerwork.Input], keyvalue.Versioned[runnerwork.Result]) {
	t.Helper()
	attempt := view.Attempt.Attempt
	input, err := owner.Load(t.Context(), attempt.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := runnerwork.Result{Attempt: attempt, Outcome: workexecution.Failed, Effects: effects,
		Failure: "runtime_failed", RecordedAt: attempt.StartedAt.Add(time.Second)}
	if effects == runnerwork.RuntimeReady {
		result.Outcome, result.Failure = workexecution.Succeeded, ""
		result.Evidence = &runnerallocation.RunnerRuntimeEvidence{
			Software: buildversion.ContainerIdentity{
				Image:        input.Record.Desired.ImageRef,
				ConfigDigest: "sha256:" + strings.Repeat("b", 64),
			},
			ContainerID: strings.Repeat(
				"c",
				64,
			), DaemonNonce: strings.Repeat("d", 64), SocketDevice: 123, SocketInode: 456,
		}
	}
	if err := owner.RecordResult(t.Context(), result); err != nil {
		t.Fatal(err)
	}
	first, err := owner.LoadResult(t.Context(), attempt, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.RecordResult(t.Context(), result); err != nil {
		t.Fatalf("result replay = %v", err)
	}
	stored, err := owner.LoadResult(t.Context(), attempt, 0)
	if err != nil || stored.Revision != first.Revision || stored.ReadRevision != first.ReadRevision {
		t.Fatalf("Runner result = %#v, %v", stored, err)
	}
	return input, stored
}

func commitRunnerStorageWork(t *testing.T, queue *workqueue.Repository, changes ...workqueue.Change) {
	t.Helper()
	result, err := queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("Runner native transaction = %t, %v", result.Succeeded, err)
	}
}
