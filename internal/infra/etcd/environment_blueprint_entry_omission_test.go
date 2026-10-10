package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// BP-10/12: omission must read the published seal/chunks, retire the exact Entry
// atomically, and refuse metadata-only retirement while applied files need it.
func TestBlueprintEntryOmissionUsesSealedInputAndRequiresRuntimeAbsence(t *testing.T) {
	for _, appliedFiles := range []bool{false, true} {
		name := "never materialized"
		if appliedFiles {
			name = "applied files require Agent cleanup"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			hierarchy, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
			parent := environmentBlueprintTestTask(t, project.Record, environment.Record, 30)
			parent.Executor, parent.Steps = taskjournal.TaskExecutorBlueprint, nil
			parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
			parent, err = TransitionTaskStatus(parent, taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, parent.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			projection := environmentBlueprintTestProjection(environment.Record.ID, parent, 1)
			stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0,
				environmentBlueprintTestRevision(environment.Record.ID, parent, "services: {}\n"),
				projection, environmentBlueprintTestMarker(parent, environment.Record.ID))
			seedEffectiveBlueprintFixture(t, store, projection)

			entryID, generationID := ids.New(ids.KindEnvEntry), ids.New(ids.KindConfig)
			record, err := entries.NewBlueprintRecord(environment.Record.ID, "APP_ENV", core.EnvEntry{
				ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
				Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "staging"}, Exposure: []string{"all"},
			}, generationID)
			if err != nil {
				t.Fatal(err)
			}
			entryValue, err := entries.EncodeRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(entryValue)
			generationValue, err := entryvalues.EncodePlain(testPlainGeneration(environment.Record.ID, entryID, generationID, "staging", parent.CreatedAt))
			if err != nil {
				t.Fatal(err)
			}
			defer clear(generationValue)
			parentValue, err := EncodeTaskRecord(parent)
			if err != nil {
				t.Fatal(err)
			}
			parentRef, err := idempotency.EncodeTaskReference(parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			keys := []string{entries.RecordKey(entryID), entries.EntryOwnerKey(environment.Record.ID, entryID),
				entries.BlueprintEntryEnvironmentPrefix + entryID, entryvalues.PlainKey(entryID, generationID)}
			mutations := []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
				{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
				{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), Value: parentRef},
				{Type: keyvalue.MutationPut, Key: keys[0], Value: entryValue},
				{Type: keyvalue.MutationPut, Key: keys[1], Value: []byte(entryID)},
				{Type: keyvalue.MutationPut, Key: keys[2], Value: []byte(environment.Record.ID)},
				{Type: keyvalue.MutationPut, Key: keys[3], Value: generationValue},
			}
			if appliedFiles {
				projection.Entries = []entries.Record{record}
				value, err := environmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
					Key: environmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID), Value: value})
			}
			if result, err := store.Transact(ctx, nil, mutations); err != nil || !result.Succeeded {
				t.Fatalf("seed Entry and parent = %#v, %v", result, err)
			}
			ledger, err := blueprintunits.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			target := blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: entryID}
			appliedValue, err := blueprintunits.EncodeApplied(blueprintunits.AppliedRecord{
				EnvironmentID: environment.Record.ID, Target: target, State: blueprintunits.Applied,
				Fingerprint: strings.Repeat("a", 64), ParentTaskID: parent.ID,
				ControllerOperationID: ids.New(ids.KindOperation), ControllerValueGenerationID: generationID,
			})
			if err != nil {
				t.Fatal(err)
			}
			epochValue, err := blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{EnvironmentID: environment.Record.ID, Sequence: 1})
			if err != nil {
				t.Fatal(err)
			}
			if result, err := store.Transact(ctx, nil, []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: blueprintunits.AppliedKey(environment.Record.ID, target), Value: appliedValue},
				{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(environment.Record.ID), Value: epochValue},
			}); err != nil || !result.Succeeded {
				t.Fatalf("seed prior acknowledgement = %#v, %v", result, err)
			}
			snapshot, err := ledger.Load(ctx, environment.Record.ID)
			if err != nil {
				t.Fatal(err)
			}
			unit := blueprintunits.Unit{Target: target, Removal: true, Writes: []blueprintunits.ResourceKey{target}}
			plan, err := blueprintunits.PrepareDesiredPlan(snapshot, blueprintunits.DesiredPlan{
				EnvironmentID: environment.Record.ID, ParentTaskID: parent.ID, Complete: true, Units: []blueprintunits.Unit{unit},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer plan.Clear()
			if result, err := store.Transact(ctx, plan.Conditions(), plan.Mutations()); err != nil || !result.Succeeded {
				t.Fatalf("publish omission = %#v, %v", result, err)
			}
			repository, err := newEnvironmentBlueprintRepository(store, environmentBlueprintTestTransactionStore{hierarchyStore: store})
			if err != nil {
				t.Fatal(err)
			}
			settled, err := repository.TrySettleBlueprintEntryOmission(ctx, parent, unit)
			if err != nil || settled == appliedFiles {
				t.Fatalf("settle omission = %v, %v; applied files = %v", settled, err, appliedFiles)
			}
			for _, key := range keys {
				read, err := store.Get(ctx, key)
				if err != nil || (read.Entry != nil) != appliedFiles {
					t.Fatalf("Entry state for %s = %#v, %v", key, read, err)
				}
			}
			snapshot, err = ledger.Load(ctx, environment.Record.ID)
			wantState := blueprintunits.Absent
			if appliedFiles {
				wantState = blueprintunits.Applied
			}
			if err != nil || len(snapshot.Applied) != 1 || snapshot.Applied[0].Record.State != wantState || len(snapshot.Executions) != 0 {
				t.Fatalf("acknowledged omission = %#v, %v", snapshot, err)
			}
		})
	}
}
