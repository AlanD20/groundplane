package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// BP-10/12: Script metadata must settle without an Agent execution, including
// omission, but a receipt prepared before a newer head cannot acknowledge it.
func TestBlueprintScriptReceiptSettlesMetadataAndFencesSupersession(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	environmentID, parentID := ids.New(ids.KindEnvironment), ids.New(ids.KindTask)
	target := blueprintunits.ResourceKey{Kind: ids.KindScript, ID: ids.New(ids.KindScript)}
	publishHead := func(parent string) {
		t.Helper()
		value, err := idempotency.EncodeTaskReference(parent)
		if err != nil {
			t.Fatal(err)
		}
		if result, err := store.Transact(ctx, nil, []keyvalue.Mutation{{
			Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), Value: value,
		}}); err != nil || !result.Succeeded {
			t.Fatalf("publish head = %#v, %v", result, err)
		}
	}
	load := func() blueprintunits.Snapshot {
		t.Helper()
		snapshot, err := ledger.Load(ctx, environmentID)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	publishPlan := func(parent string, unit blueprintunits.Unit) {
		t.Helper()
		plan, err := blueprintunits.PrepareDesiredPlan(load(), blueprintunits.DesiredPlan{
			EnvironmentID: environmentID, ParentTaskID: parent, Complete: true, Units: []blueprintunits.Unit{unit},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer plan.Clear()
		if result, err := store.Transact(ctx, plan.Conditions(), plan.Mutations()); err != nil || !result.Succeeded {
			t.Fatalf("publish unit plan = %#v, %v", result, err)
		}
	}
	publishHead(parentID)
	seedBlueprintUnitInitialAbsence(t, store, environmentID, parentID, target)
	unit := blueprintunits.Unit{Target: target, Fingerprint: strings.Repeat("a", 64), Writes: []blueprintunits.ResourceKey{target}}
	publishPlan(parentID, unit)
	receipt, err := blueprintunits.PrepareControllerScriptSettlement(load(), unit, ids.New(ids.KindOperation), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Clear()
	if result, err := store.Transact(ctx, receipt.Conditions(), receipt.Mutations()); err != nil || !result.Succeeded {
		t.Fatalf("settle metadata = %#v, %v", result, err)
	}
	settled := load()
	selection, err := blueprintunits.Select(settled)
	if err != nil || len(selection.Satisfied) != 1 || len(selection.Ready) != 0 ||
		len(settled.Executions) != 0 || len(settled.Applied) != 1 ||
		settled.Applied[0].Record.State != blueprintunits.Applied {
		t.Fatalf("Script settlement = %#v, %#v, %v", settled, selection, err)
	}
	parentID = ids.New(ids.KindTask)
	publishHead(parentID)
	unit.Removal, unit.Fingerprint = true, ""
	publishPlan(parentID, unit)
	removal, err := blueprintunits.PrepareControllerScriptSettlement(load(), unit, ids.New(ids.KindOperation), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer removal.Clear()
	if result, err := store.Transact(ctx, removal.Conditions(), removal.Mutations()); err != nil || !result.Succeeded {
		t.Fatalf("settle omission = %#v, %v", result, err)
	}
	if state := load(); state.Applied[0].Record.State != blueprintunits.Absent || len(state.Executions) != 0 {
		t.Fatalf("omission did not settle as Controller absence: %#v", state)
	}
	publishHead(ids.New(ids.KindTask))
	if result, err := store.Transact(ctx, removal.Conditions(), removal.Mutations()); err != nil || result.Succeeded {
		t.Fatalf("superseded receipt committed = %#v, %v", result, err)
	}
}
