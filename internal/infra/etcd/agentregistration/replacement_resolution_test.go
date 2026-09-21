package agentregistration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an ordinary read of unchanged state cannot disprove a delayed
// commit. The resolution barrier must prevent that exact old CAS committing
// after dispatch is reopened, while retaining an already committed replacement.
func TestLocalAgentReplacementResolutionFencesLateCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "late", true: "committed"}[committed], func(t *testing.T) {
			ctx := context.Background()
			memory := newMemoryTaskStore()
			store := &uncertainLocalAgentStore{memoryTaskStore: memory, committed: committed}
			repository, err := newRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			record := localAgentTestRecord(localAgentTestToken(7), taskJournalTime())
			created, err := repository.CreateSingleton(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			ready, err := repository.MarkReady(
				ctx,
				record.ID,
				record.Generation,
				created.Revision,
				record.CreatedAt.Add(time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			image := "registry.example/agent@sha256:" + strings.Repeat("a", 64)
			replacement := localAgentTestRecord(localAgentTestToken(8), taskJournalTime())
			_, err = repository.ReplaceGeneration(
				ctx,
				ready,
				image,
				replacement.EncryptedToken,
				replacement.TokenDigest,
				record.CreatedAt.Add(2*time.Second),
			)
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("lost transaction response: %v", err)
			}
			resolved, err := repository.FenceReplacementAttempt(
				ctx,
				ready.Record.ID,
				ready.Record.Generation,
				ready.Revision,
			)
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				if resolved.Record.Generation != ready.Record.Generation+1 ||
					resolved.Record.Image != image {
					t.Fatal("committed replacement was overwritten")
				}
			} else {
				if resolved.Record.Generation != ready.Record.Generation || resolved.Revision <= ready.Revision {
					t.Fatal("uncommitted attempt was not revision-fenced")
				}
				late, err := memory.Transact(ctx, store.conditions, store.mutations)
				if err != nil || late.Succeeded {
					t.Fatalf("late replacement = %#v, %v", late, err)
				}
			}
		})
	}
}

type uncertainLocalAgentStore struct {
	*memoryTaskStore
	committed  bool
	conditions []testkeyvalue.Condition
	mutations  []testkeyvalue.Mutation
}

func (store *uncertainLocalAgentStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if len(mutations) != 5 || mutations[3].Type != testkeyvalue.MutationDelete {
		return store.memoryTaskStore.Transact(ctx, conditions, mutations)
	}
	store.conditions = append([]testkeyvalue.Condition(nil), conditions...)
	store.mutations = make([]testkeyvalue.Mutation, len(mutations))
	for index, mutation := range mutations {
		store.mutations[index] = mutation
		store.mutations[index].Value = append([]byte(nil), mutation.Value...)
	}
	if store.committed {
		if _, err := store.memoryTaskStore.Transact(ctx, conditions, mutations); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	return testkeyvalue.TransactionResult{}, errs.New(
		errs.KindStorageUnavailable,
		"transaction response was lost",
	)
}
