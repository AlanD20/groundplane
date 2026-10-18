package app

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: lost completion must qualify an already-ready captured candidate,
// not rotate credentials again or release another operator's admission hold.
func TestNativeAgentWorkReadyReplayPreservesIdentityAndIndependentPause(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Minute)
	f.installCandidate(t, true)
	release, err := f.registry.PauseAssignments(t.Context(), f.capture.AgentID, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var first agentoperations.Receipt
	for replay := 0; replay < 2; replay++ {
		if err := f.owner.Reconcile(f.ctx, f.delivery); err != nil {
			t.Fatal(err)
		}
		stored, found, err := f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
		if err != nil || !found {
			t.Fatalf("native receipt: %v, %v", found, err)
		}
		result := stored.Record.Result
		if result.Outcome != workexecution.Succeeded || result.Observed != agentoperations.ReadyDesired ||
			result.Generation != 2 || f.runtime.generated != 0 {
			t.Fatalf("ready replay changed candidate: %#v; rotations %d", result, f.runtime.generated)
		}
		if replay == 0 {
			first = stored.Record
		} else if !stored.Record.At.Equal(first.At) {
			t.Fatal("receipt replay replaced original outcome time")
		}
	}
	if err := f.owner.Settle(f.ctx, f.queue, f.delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if f.container.sessions[2].AssignmentsAllowed() {
		t.Fatal("replay released an independent admission pause")
	}
}

// Rationale: an interrupted, unqualified generation cannot be promoted by
// replay. Restore the exact predecessor once, using a new fenced generation.
func TestNativeAgentWorkRestoresPartialCandidateOnce(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Minute)
	f.installCandidate(t, false)
	for replay := 0; replay < 2; replay++ {
		if err := f.owner.Reconcile(f.ctx, f.delivery); err != nil {
			t.Fatal(err)
		}
		stored, found, err := f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
		if err != nil || !found {
			t.Fatalf("restoration receipt: %v, %v", found, err)
		}
		result := stored.Record.Result
		current, err := f.agents.GetSingleton(f.ctx)
		if err != nil || result.Outcome != workexecution.Failed || result.Observed != agentoperations.ReadyPrevious ||
			result.Generation != 3 || current.Record.Image != f.capture.PreviousImage || f.runtime.generated != 1 {
			t.Fatalf(
				"partial recovery changed authority: %#v; current %#v; rotations %d; %v",
				result,
				current.Record,
				f.runtime.generated,
				err,
			)
		}
	}
	if err := f.owner.Settle(f.ctx, f.queue, f.delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// Rationale: unavailable resource state is not proof of settled replacement effects.
// A reconstructed operation owner must retain and later clear its durable fence.
func TestNativeAgentWorkUnresolvedRecoveryRetainsForwardFence(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Minute)
	f.installCandidate(t, false)
	f.store.failAfterFence = true
	if err := f.owner.Reconcile(f.ctx, f.delivery); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("unresolved recovery = %v", err)
	}
	stored, found, err := f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
	if err != nil || !found || stored.Record.Result.Observed != agentoperations.Unresolved ||
		stored.Record.Result.Failure == nil {
		t.Fatalf("unresolved receipt: %#v, %v", stored, err)
	}
	originalFailure := stored.Record.Result.Failure.Code
	binding, err := f.agents.MaintenanceBinding(f.ctx, f.capture.AgentID, 2)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.store.Get(f.ctx, workqueue.HostForwardFenceKey(binding.HostID))
	if err != nil || guard.Entry == nil {
		t.Fatalf("lost durable recovery fence: %v", err)
	}
	f.store.failAfterFence, f.store.failPrimary = false, false
	f.resetOwner(t)
	if err := f.owner.Reconcile(f.ctx, f.delivery); err != nil {
		t.Fatal(err)
	}
	stored, found, err = f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
	if err != nil || !found || stored.Record.Result.Observed != agentoperations.ReadyPrevious ||
		f.runtime.generated != 1 || stored.Record.Result.Failure == nil || stored.Record.Result.Failure.Code != originalFailure {
		t.Fatalf("resumed recovery: %#v; rotations %d; %v", stored, f.runtime.generated, err)
	}
	guard, err = f.store.Get(f.ctx, workqueue.HostForwardFenceKey(binding.HostID))
	if err != nil || guard.Entry == nil {
		t.Fatalf("readiness reopened admission before native settlement: %v", err)
	}
	if err := f.owner.Settle(f.ctx, f.queue, f.delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	guard, err = f.store.Get(f.ctx, workqueue.HostForwardFenceKey(binding.HostID))
	if err != nil || guard.Entry != nil {
		t.Fatalf("settled recovery retained its own fence: %v", err)
	}
}

// Rationale: a stop committed as the candidate reports Ready cannot become
// success. The same native owner must restore the predecessor before settlement.
func TestNativeAgentWorkAbortDuringReadinessRestoresPredecessor(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Minute)
	f.container.onReady = func() {
		f.container.onReady = nil
		view, err := f.queue.ReadWork(f.ctx, f.capture.TaskID, f.capture.WorkItemID, 0)
		if err != nil {
			t.Fatal(err)
		}
		stop, err := workqueue.PrepareStop(view.Task, workexecution.Cancelled, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		commitAgentLifecycleChange(t, f.queue, stop)
		view, err = f.queue.ReadWork(f.ctx, f.capture.TaskID, f.capture.WorkItemID, 0)
		if err != nil {
			t.Fatal(err)
		}
		revoke, err := f.owner.Stop(f.ctx, view.Task, view.Item, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		commitAgentLifecycleChange(t, f.queue, revoke)
	}
	if err := f.owner.Execute(f.ctx, f.delivery); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stored, found, err := f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
	if err != nil || !found || stored.Record.Result.Outcome != workexecution.Cancelled ||
		stored.Record.Result.Observed != agentoperations.ReadyPrevious || stored.Record.Result.Generation != 3 ||
		stored.Record.Result.Failure == nil || f.runtime.generated != 2 {
		t.Fatalf("stop/Ready race = %#v, rotations %d, %v", stored, f.runtime.generated, err)
	}
	if err := f.owner.Settle(f.ctx, f.queue, f.delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// Rationale: an expired assignment that never changed the Agent may qualify
// the predecessor, but must not rotate credentials or install its candidate.
func TestNativeAgentWorkExpiredUntouchedInputPreservesPredecessor(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Hour)
	for replay := 0; replay < 2; replay++ {
		f.resetOwner(t)
		if err := f.owner.Reconcile(f.ctx, f.delivery); err != nil {
			t.Fatal(err)
		}
		stored, found, err := f.inputs.ReadResult(f.ctx, f.delivery.Attempt.Attempt, 0)
		if err != nil || !found || stored.Record.Result.Outcome != workexecution.Failed ||
			stored.Record.Result.Observed != agentoperations.ReadyPrevious || stored.Record.Result.Generation != 1 ||
			f.runtime.generated != 0 {
			t.Fatalf("expired untouched recovery: %#v; rotations %d; %v", stored, f.runtime.generated, err)
		}
	}
	if err := f.owner.Settle(f.ctx, f.queue, f.delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// Rationale: a changed stored target cannot turn this captured action into
// authority to replace another Agent, even under an otherwise valid attempt.
func TestNativeAgentWorkRejectsChangedTargetAuthority(t *testing.T) {
	f := newAgentLifecycleFixture(t, time.Minute)
	view, err := f.queue.ReadWork(f.ctx, f.capture.TaskID, f.capture.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	item := view.Item.Item
	item.TargetID = ids.New(ids.KindAgent)
	raw, err := workqueue.EncodeItem(item)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Transact(f.ctx, nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.ItemKey(item.ID), Value: raw}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.AuthorizeStart(f.ctx, f.delivery); err == nil {
		t.Fatal("changed Agent target authorized")
	}
}
