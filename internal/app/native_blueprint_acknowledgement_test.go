package app

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Supplied applied receipts are selection inputs, not simulated host proof.
// Exercise the real preparation wrapper and atomic queue settlement: an exact
// acknowledgement skips execution, while changed/uncertain/racing state cannot.
func TestNativeBlueprintAcknowledgesAppliedInputWithoutDispatch(t *testing.T) {
	for _, scenario := range []string{"applied", "changed", "uncertain", "racing_epoch"} {
		t.Run(scenario, func(t *testing.T) {
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
			item, unit := graph.Record.Items[0], graph.Record.Units[0]
			plan := blueprintunits.DesiredPlan{EnvironmentID: f.task.Scope.EnvironmentID,
				ParentTaskID: f.task.ID, Complete: true, Units: slices.Clone(graph.Record.Units)}
			slices.SortFunc(
				plan.Units,
				func(a, b blueprintunits.Unit) int { return strings.Compare(a.Target.ID, b.Target.ID) },
			)
			applied := blueprintunits.AppliedRecord{EnvironmentID: plan.EnvironmentID, Target: unit.Target,
				State: blueprintunits.Applied, Fingerprint: unit.Fingerprint, ParentTaskID: ids.New(ids.KindTask),
				ControllerOperationID: ids.New(
					ids.KindOperation,
				), ControllerRuntimeRevision: 1, ControllerAuthoredRevision: 1}
			if scenario == "changed" {
				applied.Fingerprint = strings.Repeat("c", 64)
			}
			if scenario == "uncertain" {
				applied.State = blueprintunits.Uncertain
				applied.AffectedWrites = []blueprintunits.ResourceKey{unit.Target}
				applied.ControllerOperationID = ""
				applied.ControllerRuntimeRevision, applied.ControllerAuthoredRevision = 0, 0
				applied.Execution = &blueprintunits.AppliedExecution{TaskID: applied.ParentTaskID,
					WorkItemID: ids.New(ids.KindWorkItem), AttemptID: ids.New(ids.KindAttempt),
					Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: item.Placement.HostID,
						AgentID: ids.New(ids.KindAgent), Generation: 1}, Input: f.task.Intent, Result: f.task.Intent}
			}
			head, err := idempotency.EncodeTaskReference(f.task.ID)
			if err != nil {
				t.Fatal(err)
			}
			desired, err := blueprintunits.EncodeDesiredPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := blueprintunits.EncodeApplied(applied)
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := blueprintunits.EncodeEpoch(
				blueprintunits.EpochRecord{EnvironmentID: plan.EnvironmentID, Sequence: 1},
			)
			if err != nil {
				t.Fatal(err)
			}
			change := map[string][]byte{blueprints.EnvironmentBlueprintHeadKey(plan.EnvironmentID): head,
				blueprintunits.DesiredPlanKey(plan.EnvironmentID):          desired,
				blueprintunits.AppliedKey(plan.EnvironmentID, unit.Target): receipt,
				blueprintunits.EpochKey(plan.EnvironmentID):                epoch}
			for _, other := range plan.Units {
				if other.Target == unit.Target {
					continue
				}
				value, err := blueprintunits.EncodeApplied(blueprintunits.AppliedRecord{
					EnvironmentID: plan.EnvironmentID, Target: other.Target, State: blueprintunits.Applied,
					Fingerprint: other.Fingerprint, ParentTaskID: ids.New(ids.KindTask),
					ControllerOperationID: ids.New(
						ids.KindOperation,
					), ControllerRuntimeRevision: 1, ControllerAuthoredRevision: 1})
				if err != nil {
					t.Fatal(err)
				}
				change[blueprintunits.AppliedKey(plan.EnvironmentID, other.Target)] = value
			}
			var mutations []keyvalue.Mutation
			for key, value := range change {
				mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
			}
			if result, err := f.store.Transact(t.Context(), nil, mutations); err != nil || !result.Succeeded {
				t.Fatalf("fixture ledger: %v", err)
			}
			planner := &nativeCountingInputPlanner{}
			var registrations []workdispatch.InputRegistration
			for _, operation := range []workexecution.Operation{workexecution.Release, workexecution.ServiceRemoval,
				workexecution.Component, workexecution.Volume, workexecution.Network, workexecution.Attach,
				workexecution.Detach, workexecution.EntryConfiguration} {
				registrations = append(
					registrations,
					workdispatch.InputRegistration{
						Action:    workexecution.ActionApply,
						Operation: operation,
						Owner:     planner,
					},
				)
			}
			registered, err := blueprintcoordinator.NewInputPreparations(f.queue, units, registrations)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "racing_epoch" {
				f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
					for _, mutation := range mutations {
						if mutation.Key == blueprintunits.EpochKey(plan.EnvironmentID) {
							continue
						}
						if mutation.Key != "" && strings.Contains(mutation.Key, item.ID) {
							f.store.beforeCommit = nil
							if _, err := f.store.Put(t.Context(), blueprintunits.EpochKey(plan.EnvironmentID), epoch); err != nil {
								t.Fatal(err)
							}
							break
						}
					}
				}
			}
			err = registered[0].Owner.Prepare(t.Context(), f.task.ID, item.ID)
			view, readErr := f.queue.ReadWork(t.Context(), f.task.ID, item.ID, 0)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "applied" {
				if err != nil || planner.calls != 0 || view.Item.Item.Outcome != workexecution.Succeeded ||
					view.Item.Item.AttemptNumber != 0 || view.Item.Item.Input != nil {
					t.Fatalf("applied input dispatched: %v", err)
				}
				proof, err := f.queue.ReadInputSettlement(t.Context(), view)
				if err != nil || proof == nil || proof.Authority == nil {
					t.Fatalf("missing acknowledgement evidence: %v", err)
				}
			} else if scenario == "racing_epoch" {
				if err == nil || planner.calls != 0 || view.Item.Item.Phase != workexecution.Waiting {
					t.Fatal("stale acknowledgement committed")
				}
			} else if err != nil || planner.calls != 1 || view.Item.Item.Phase != workexecution.Waiting {
				t.Fatalf("unapplied input bypassed its planner: %v", err)
			}
		})
	}
}
