package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const nativeAdmissionTaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"

type closedMutationAdmission struct {
	calls   int
	failure error
	open    bool
}

func (admission *closedMutationAdmission) CheckMutation(_ context.Context, nativeAbortTaskID string) error {
	admission.calls++
	if admission.failure != nil {
		return admission.failure
	}
	if admission.open || nativeAbortTaskID == nativeAdmissionTaskID {
		return nil
	}
	return errs.New(errs.KindResourceInUse, "native recovery is active")
}

type nativeAdmissionBackupSchedules struct{ calls int }

func (schedules *nativeAdmissionBackupSchedules) RunBackupSchedules(context.Context, time.Time) error {
	schedules.calls++
	return nil
}

// Rationale: a trial must not publish due backups or rewrite/prune ordinary Task
// and idempotency state while its predecessor still owns possible recovery.
// QA: UP-10; scheduler calls against fake stores, not durable trial recovery.
func TestNativeMutationAdmissionPausesScheduler(t *testing.T) {
	tasks, markers, agents, schedules := &fakeTaskExpiration{}, &fakeIdempotencyPruning{}, &fakeStaleAgentExpiration{}, &nativeAdmissionBackupSchedules{}
	admission := &closedMutationAdmission{}
	scheduler := &Scheduler{
		admission: admission, tasks: tasks, idempotency: markers, agents: agents,
		backupSchedules: schedules, now: time.Now,
	}
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tasks.calls != 0 || tasks.pruneCalls != 0 || markers.calls != 0 || agents.calls != 0 || schedules.calls != 0 ||
		!scheduler.nextPrune.IsZero() {
		t.Fatal("trial scheduler reached ordinary durable mutation")
	}
	admission.failure = errs.New(errs.KindInternal, "journal unavailable")
	if scheduler.tick(context.Background()) == nil || tasks.calls != 0 {
		t.Fatal("unavailable admission did not fail closed")
	}
	scheduler.admission = nil
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tasks.calls != 1 || tasks.pruneCalls != 1 || markers.calls != 1 || agents.calls != 1 || schedules.calls != 1 {
		t.Fatal("scheduler did not resume")
	}
}
