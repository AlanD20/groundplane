package etcd

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-195: many encrypted Attach captures must not enlarge atomic acceptance;
// staged bytes are exact, source races still reject acceptance, and an expired
// descriptor must stop late writers without publishing any Task or work.
func TestBlueprintAttachCaptureStagingBoundsAcceptanceAndFencesSources(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(fmt.Sprint("abandon=", abandon), func(t *testing.T) {
			base := newMemoryHierarchyStore()
			descriptor := "/descriptor"
			if _, err := base.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: descriptor, Value: []byte("sealed")}}); err != nil {
				t.Fatal(err)
			}
			store := &attachStagingInterruptedStore{memoryHierarchyStore: base, abandon: abandon}
			repository := &EnvironmentBlueprintRepository{HierarchyRepository: &HierarchyRepository{store: store}}
			parent := workexecution.Task{ID: ids.New(ids.KindTask), Scope: workexecution.Scope{
				EnvironmentID: ids.New(ids.KindEnvironment),
			}}
			prepared := preparedBlueprintAttachInputGenerations{}
			const count = 32
			for index := range count {
				generation := blueprintattachinputs.Generation{
					AttachID:      ids.New(ids.KindAttach),
					AttachName:    fmt.Sprint("app-", index),
					EnvironmentID: parent.Scope.EnvironmentID,
					WorkItemID:    ids.New(ids.KindWorkItem),
				}
				prepared.generations = append(prepared.generations, generation)
				key := blueprintattachinputs.Key(parent.ID, generation.AttachID)
				prepared.mutations = append(prepared.mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
					Key: key, Value: []byte(fmt.Sprint("encrypted-capture-", index))})
				for _, key := range []string{key, workqueue.ItemKey(generation.WorkItemID),
					attachments.AttachKey(generation.AttachID), attachments.AttachNameKey(generation.EnvironmentID, generation.AttachName)} {
					prepared.conditions = append(prepared.conditions, keyvalue.Condition{Key: key})
				}
			}
			prepared.conditions = append(prepared.conditions, keyvalue.Condition{Key: "/backing-source"})
			err := repository.stageBlueprintAttachInputs(t.Context(), parent,
				keyvalue.Condition{Key: descriptor, ModRevision: 1}, nil, &prepared)
			if abandon {
				if err == nil {
					t.Fatal("expired descriptor permitted late staging")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for index, generation := range prepared.generations {
					read, err := base.Get(t.Context(), blueprintattachinputs.Key(parent.ID, generation.AttachID))
					if err != nil || read.Entry == nil || !bytes.Equal(read.Entry.Value, []byte(fmt.Sprint("encrypted-capture-", index))) {
						t.Fatalf("capture %d differs after staging: %v", index, err)
					}
				}
				budget, err := base.MeasureTransaction(t.Context(), prepared.conditions, prepared.mutations)
				if err != nil || !budget.Fits() || budget.Operations > 3 {
					t.Fatalf("immutable captures still enlarge acceptance: %+v %v", budget, err)
				}
				if _, err := base.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: "/backing-source", Value: []byte("changed")}}); err != nil {
					t.Fatal(err)
				}
				result, err := base.Transact(t.Context(), prepared.conditions, []keyvalue.Mutation{{
					Type: keyvalue.MutationPut, Key: workqueue.TaskKey(parent.ID), Value: []byte("accepted"),
				}})
				if err != nil || result.Succeeded {
					t.Fatalf("staging bypassed source concurrency guard: %+v %v", result, err)
				}
			}
			read, err := base.Get(t.Context(), workqueue.TaskKey(parent.ID))
			if err != nil || read.Entry != nil {
				t.Fatalf("staging published an executable parent: %v", err)
			}
		})
	}
}

type attachStagingInterruptedStore struct {
	*memoryHierarchyStore
	abandon bool
	writes  int
}

func (store *attachStagingInterruptedStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	store.writes++
	if store.abandon && store.writes == 2 {
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: "/descriptor", Value: []byte("abandoned")}}); err != nil {
			return keyvalue.TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
