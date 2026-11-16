package etcd

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskcredentialrefs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Stable identity reservations must win atomically against deletion, including
// a deletion whose initial absence read happened before request staging.
func TestAcceptedTaskCredentialsFenceDeletionAndReleaseAfterSettlement(t *testing.T) {
	for _, deletionFirst := range []bool{false, true} {
		t.Run(fmt.Sprint("deletion-first=", deletionFirst), func(t *testing.T) {
			store, project := secretMetadataStore(t)
			repository, err := newSecretRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			id := ids.New(ids.KindSecret)
			at := time.Now().UTC()
			record, err := secrets.NewProjectRecord(
				id,
				project.Record.ID,
				"SELECTED_TOKEN",
				core.SecretKindEnvVar,
				"",
				at,
			)
			if err != nil {
				t.Fatal(err)
			}
			current, err := repository.CreateSecret(
				t.Context(),
				secrets.ProjectOwner(project),
				record,
				testSecretEncryptedValue(id, "private-ciphertext"),
			)
			if err != nil {
				t.Fatal(err)
			}
			guards, err := secretmutations.PrepareSecretRemovalAbsence(t.Context(), store, id)
			if err != nil {
				t.Fatal(err)
			}
			refs, err := taskcredentialrefs.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			intent := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
			task := workexecution.Task{
				ID:             ids.New(ids.KindTask),
				Actor:          workexecution.OperatorActor,
				Action:         workexecution.ActionApply,
				TargetID:       ids.New(ids.KindEnvironment),
				AcceptedIntent: &intent,
				Scope:          workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID},
				CreatedAt:      at,
				UpdatedAt:      at,
				Deadline:       at.Add(time.Hour),
			}
			if deletionFirst {
				if _, err := repository.DeleteSecret(t.Context(), secrets.ProjectOwner(project), current); err != nil {
					t.Fatal(err)
				}
				// A replacement under the same human key is a different identity.
				// The already authored request must not silently adopt its value.
				replacementID := ids.New(ids.KindSecret)
				replacement, err := secrets.NewProjectRecord(
					replacementID,
					project.Record.ID,
					"SELECTED_TOKEN",
					core.SecretKindEnvVar,
					"",
					at,
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := repository.CreateSecret(t.Context(), secrets.ProjectOwner(project), replacement, testSecretEncryptedValue(replacementID, "replacement-ciphertext")); err != nil {
					t.Fatal(err)
				}
				if _, err := refs.Stage(t.Context(), task, []string{id}); !errors.Is(
					err,
					errs.New(errs.KindSecretNotFound, ""),
				) {
					t.Fatalf("accepted a deleted credential: %v", err)
				}
				left, err := store.Range(
					t.Context(),
					keyvalue.RangeRequest{Prefix: taskcredentialrefs.SecretPrefix(id), Limit: 1},
				)
				if err != nil || len(left.Values) != 0 {
					t.Fatalf("rejected request leaked credential custody: %v", err)
				}
				return
			}
			publication, err := refs.Stage(t.Context(), task, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(publication.Mutations)
			stale, err := store.Transact(
				t.Context(),
				guards,
				[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: secrets.RecordKey(id)}},
			)
			if err != nil || stale.Succeeded {
				t.Fatalf("stale deletion beat staged credential reservation: %v", err)
			}
			raw, err := workqueue.EncodeTask(task)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(raw)
			publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: workqueue.TaskKey(task.ID)})
			publication.Mutations = append(
				publication.Mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.ID), Value: raw},
			)
			accepted, err := store.Transact(t.Context(), publication.Conditions, publication.Mutations)
			if err != nil || !accepted.Succeeded {
				t.Fatalf("publish original Task and credentials: %v", err)
			}
			// A lost response/abandon callback must not tear down accepted custody.
			if err := refs.Finish(t.Context(), task.ID, false); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.DeleteSecret(t.Context(), secrets.ProjectOwner(project), current); !errors.Is(
				err,
				errs.New(errs.KindResourceInUse, ""),
			) {
				t.Fatalf("unfinished Task credential was deleted: %v", err)
			}
			finished, err := workexecution.RejectPlanning(
				task,
				workexecution.Failed,
				workexecution.Input{
					ID:     ids.New(ids.KindPlan),
					SHA256: strings.Repeat("b", 64),
				},
				at.Add(time.Second),
				at.Add(time.Hour),
			)
			if err != nil {
				t.Fatal(err)
			}
			settled, err := workqueue.EncodeTask(finished)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(settled)
			result, err := store.Transact(
				t.Context(),
				[]keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: accepted.Revision}},
				[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.ID), Value: settled}},
			)
			if err != nil || !result.Succeeded {
				t.Fatalf("settle failed no-work Task: %v", err)
			}
			if _, err := repository.DeleteSecret(t.Context(), secrets.ProjectOwner(project), current); err != nil {
				t.Fatalf("fully settled Task kept credential deletion blocked: %v", err)
			}
		})
	}
}

// The maximum authored reference set crosses many eight-reference batches.
// Crash-left submissions must remain protected before expiry and retire fully
// afterward, without deleting any actual credential metadata.
func TestAcceptedTaskCredentialMaximumOrphanCleanup(t *testing.T) {
	store, project := secretMetadataStore(t)
	at := time.Now().UTC()
	var selected []string
	for index := range 514 {
		id := ids.New(ids.KindSecret)
		record, err := secrets.NewProjectRecord(
			id,
			project.Record.ID,
			fmt.Sprintf("TOKEN_%d", index),
			core.SecretKindEnvVar,
			"",
			at,
		)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := secrets.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		result, err := store.Transact(
			t.Context(),
			[]keyvalue.Condition{{Key: secrets.RecordKey(id)}},
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: secrets.RecordKey(id), Value: raw}},
		)
		clear(raw)
		if err != nil || !result.Succeeded {
			t.Fatalf("create reference metadata: %v", err)
		}
		selected = append(selected, id)
	}
	refs, err := taskcredentialrefs.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	intent := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	task := workexecution.Task{
		ID:             ids.New(ids.KindTask),
		Actor:          workexecution.OperatorActor,
		Action:         workexecution.ActionApply,
		TargetID:       ids.New(ids.KindEnvironment),
		AcceptedIntent: &intent,
		Scope:          workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID},
		CreatedAt:      at,
		UpdatedAt:      at,
		Deadline:       at.Add(time.Hour),
	}
	publication, err := refs.Stage(t.Context(), task, selected)
	if err != nil {
		t.Fatal(err)
	}
	keyvalue.ClearMutationValues(publication.Mutations)
	if _, err := refs.Collect(t.Context(), "", at); err != nil {
		t.Fatal(err)
	}
	if err := refs.PurgeFinishedForSecret(t.Context(), selected[0]); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("unexpired submission lost custody: %v", err)
	}
	for range 66 {
		if _, err := refs.Collect(t.Context(), "", at.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range selected {
		if err := refs.PurgeFinishedForSecret(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		read, err := store.Get(t.Context(), secrets.RecordKey(id))
		if err != nil || read.Entry == nil {
			t.Fatalf("orphan cleanup removed credential: %v", err)
		}
	}
	owner, err := store.Get(t.Context(), taskcredentialrefs.Key(task.ID))
	if err != nil || owner.Entry != nil {
		t.Fatalf("orphan cleanup did not retire owner: %v", err)
	}
}
