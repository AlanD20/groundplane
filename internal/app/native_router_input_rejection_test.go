package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routerInputRefusal struct{ failure error }

func (owner routerInputRefusal) Prepare(context.Context, string, string) error { return owner.failure }

// Never-executable immutable configuration must fail promptly; a temporary
// source conflict still waits and neither outcome may create an execution attempt.
func TestNativeRouterConfigurationRefusalSettlesWithoutExecution(t *testing.T) {
	for _, permanent := range []bool{true, false} {
		name := "permanent_configuration"
		if !permanent {
			name = "temporary_conflict"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeBlueprintPublication(t, true)
			for range 4 {
				if _, err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			units, err := blueprintunits.NewRepository(f.store)
			if err != nil {
				t.Fatal(err)
			}
			graph, err := units.LoadCapturedGraph(t.Context(), f.task, f.store.revision)
			if err != nil {
				t.Fatal(err)
			}
			item := graph.Record.Items[0]
			kind := errs.KindRouterConfigurationInvalid
			if !permanent {
				kind = errs.KindStateConflict
			}
			failure := errs.Wrap(kind, errors.New("private planner cause"))
			inputs, err := workdispatch.NewInputs(f.queue, []workdispatch.InputRegistration{{
				Action: workexecution.ActionApply, Operation: item.Operation, Owner: routerInputRefusal{failure},
			}})
			if err != nil {
				t.Fatal(err)
			}
			err = inputs.Advance(t.Context(), workqueue.QueueEntry{TaskID: f.task.ID, WorkItemID: item.ID})
			if permanent && err != nil {
				t.Fatal(err)
			}
			if !permanent && !errors.Is(err, failure) {
				t.Fatalf("temporary conflict lost: %v", err)
			}
			view, err := f.queue.ReadWork(t.Context(), f.task.ID, item.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := view.Item.Item
			if got.Input != nil || got.AttemptNumber != 0 || got.ActiveAttemptID != "" {
				t.Fatal("refusal authorized execution")
			}
			if permanent {
				if got.Phase != workexecution.Settled || got.Outcome != workexecution.Failed || got.Result == nil {
					t.Fatalf("invalid router kept waiting: %+v", got)
				}
			} else if got.Phase != workexecution.Waiting || got.Wait == nil || got.Wait.Reason != workexecution.WaitInput {
				t.Fatalf("temporary conflict was closed: %+v", got)
			}
		})
	}
}
