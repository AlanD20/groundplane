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

// BP-10/12: Controller metadata must settle without an Agent execution,
// including omission, but a receipt prepared before a newer head cannot
// acknowledge it. Entry removal must retire the same exact value generation.
func TestBlueprintScriptReceiptSettlesMetadataAndFencesSupersession(t *testing.T) {
	t.Run("Script", func(t *testing.T) {
		testBlueprintControllerMetadataReceipt(t, ids.KindScript, func(snapshot blueprintunits.Snapshot, unit blueprintunits.Unit, revision int64) (blueprintunits.MutationPlan, error) {
			return blueprintunits.PrepareControllerScriptSettlement(snapshot, unit, ids.New(ids.KindOperation), revision)
		})
	})
	t.Run("Entry", func(t *testing.T) {
		generationID := ids.New(ids.KindConfig)
		testBlueprintControllerMetadataReceipt(t, ids.KindEnvEntry, func(snapshot blueprintunits.Snapshot, unit blueprintunits.Unit, _ int64) (blueprintunits.MutationPlan, error) {
			if unit.Removal {
				return blueprintunits.PrepareControllerEntryRemoval(snapshot, unit, ids.New(ids.KindOperation), generationID)
			}
			return blueprintunits.PrepareControllerEntrySettlement(snapshot, unit, ids.New(ids.KindOperation), generationID)
		})
	})
	t.Run("Route without runtime projection", func(t *testing.T) {
		testBlueprintControllerMetadataReceipt(t, ids.KindRoute, func(snapshot blueprintunits.Snapshot, unit blueprintunits.Unit, revision int64) (blueprintunits.MutationPlan, error) {
			return blueprintunits.PrepareControllerRouteSettlement(snapshot, unit, ids.New(ids.KindOperation), revision)
		})
	})
}

func testBlueprintControllerMetadataReceipt(
	t *testing.T, kind ids.Kind,
	settle func(blueprintunits.Snapshot, blueprintunits.Unit, int64) (blueprintunits.MutationPlan, error),
) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryTaskStore()
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	environmentID, parentID := ids.New(ids.KindEnvironment), ids.New(ids.KindTask)
	target := blueprintunits.ResourceKey{Kind: kind, ID: ids.New(kind)}
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
	receipt, err := settle(load(), unit, 1)
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
	if kind == ids.KindRoute {
		unit.Fingerprint = strings.Repeat("b", 64)
	} else {
		unit.Removal, unit.Fingerprint = true, ""
	}
	publishPlan(parentID, unit)
	removal, err := settle(load(), unit, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer removal.Clear()
	if result, err := store.Transact(ctx, removal.Conditions(), removal.Mutations()); err != nil || !result.Succeeded {
		t.Fatalf("settle omission = %#v, %v", result, err)
	}
	wantState := blueprintunits.Absent
	if kind == ids.KindRoute {
		wantState = blueprintunits.Applied
	}
	if state := load(); state.Applied[0].Record.State != wantState || len(state.Executions) != 0 {
		t.Fatalf("metadata did not settle as %s: %#v", wantState, state)
	}
	publishHead(ids.New(ids.KindTask))
	if result, err := store.Transact(ctx, removal.Conditions(), removal.Mutations()); err != nil || result.Succeeded {
		t.Fatalf("superseded receipt committed = %#v, %v", result, err)
	}
}
