package backupwork

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: expiry is an exclusive credential boundary, independent of timeout
// maintenance. No captured input or credential read may begin at/after it.
func TestSecretSourceDeadlineRejectsBeforeCapturedInputRead(t *testing.T) {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	deadline := at.Add(6 * time.Hour)
	reference := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, 1), SHA256: strings.Repeat("a", 64)}
	scope := workexecution.Scope{
		TenantID:      ids.NewAt(ids.KindTenant, at, 2),
		ProjectID:     ids.NewAt(ids.KindProject, at, 3),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 4),
	}
	task := workexecution.Task{ID: ids.NewAt(ids.KindTask, at, 5), Actor: workexecution.OperatorActor,
		Scope: scope, Action: workexecution.ActionBackup, TargetID: scope.EnvironmentID,
		Intent: reference, ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Running: 1},
		CreatedAt: at, UpdatedAt: at, Deadline: deadline}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: ids.NewAt(ids.KindHost, at, 6),
		AgentID: ids.NewAt(ids.KindAgent, at, 7), Generation: 1}
	item := workexecution.Item{ID: ids.NewAt(ids.KindWorkItem, at, 8), TaskID: task.ID,
		Operation: workexecution.Backup, TargetID: task.TargetID, Input: &reference,
		Placement: workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
		Claims:    []workexecution.Claim{{HostID: identity.HostID, ScopeID: scope.EnvironmentID}},
		Phase:     workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: deadline}
	item, attempt, err := workexecution.Assign(
		item,
		identity,
		ids.NewAt(ids.KindAttempt, at, 9),
		workexecution.Forward,
		at,
		deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	item, attempt, err = workexecution.Start(item, attempt, at)
	if err != nil {
		t.Fatal(err)
	}
	view := workqueue.ResumeView{Work: workqueue.View{
		Task: workqueue.TaskSnapshot{
			Task:     task,
			Revision: 40,
		}, Item: workqueue.ItemSnapshot{Item: item, Revision: 40}, ReadRevision: 41},
		Attempt: workqueue.AttemptSnapshot{Attempt: attempt, Revision: 40}, ActiveAttempt: true, CurrentExecutor: true}
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		t.Run(offset.String(), func(t *testing.T) {
			store := &credentialInputReadProbe{}
			repository, err := NewInputRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			_, err = repository.LoadSecretSource(t.Context(), view, 0, deadline.Add(offset))
			if offset < 0 {
				if !errors.Is(err, store.failure()) || store.calls != 1 {
					t.Fatalf("active window did not reach captured input: calls=%d, error=%v", store.calls, err)
				}
			} else if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || store.calls != 0 {
				t.Fatalf("expired window read input: calls=%d, error=%v", store.calls, err)
			}
		})
	}
}

type credentialInputReadProbe struct{ calls int }

func (*credentialInputReadProbe) failure() error {
	return errs.New(errs.KindStorageUnavailable, "captured input read reached")
}

func (store *credentialInputReadProbe) GetMany(
	context.Context,
	keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	store.calls++
	return nil, store.failure()
}
func (*credentialInputReadProbe) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errs.New(errs.KindInternal, "credential deadline unexpectedly read a range")
}
