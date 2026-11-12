package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Image cleanup must not permanently disable software updates. An idle fence
// permits publication; an active/corrupt fence or concurrent removal cannot.
func TestAgentPublicationRespectsImageCleanupState(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		race  bool
		want  errs.Kind
	}{
		{name: "absent"},
		{name: "idle", value: `{"operation":""}`},
		{name: "active", value: `{"operation":"removing"}`, want: errs.KindResourceInUse},
		{name: "corrupt", value: `{`, want: errs.KindInternal},
		{name: "changed after read", value: `{"operation":""}`, race: true, want: errs.KindStateConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &agentPublicationFenceStore{memoryHierarchyStore: newMemoryHierarchyStore()}
			store.revision = 1
			if test.value != "" {
				if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: []byte(test.value)}}); err != nil {
					t.Fatal(err)
				}
			}
			store.race = test.race
			repository, err := etcd.NewTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			capture := agentoperations.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
				PlanID: ids.New(ids.KindPlan), AgentID: ids.New(ids.KindAgent), Action: workexecution.ActionUpdate,
				Image: "registry.example/agent@sha256:" + strings.Repeat("b", 64),
				PreviousImage: "registry.example/agent@sha256:" + strings.Repeat(
					"a",
					64,
				), StartingGeneration: 1, CreatedAt: at}
			marker := agentConfigurationTestMarker()
			marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, capture.TaskID
			marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
			marker.Locator = idempotency.IdempotencyLocator{
				ScopeKind: idempotency.IdempotencyScopePlatform,
				ScopeID:   "-",
				Method:    "POST",
				Route:     "/agents/{id}/update",
				Key:       "agent-image-fence-behavior",
			}
			marker.Response = idempotency.IdempotencyResponse{
				Status:      202,
				ContentKind: "application/json",
				Body:        []byte(fmt.Sprintf(`{"task_id":%q}`, capture.TaskID)),
			}
			result, err := repository.CreateAgentWork(t.Context(), capture, marker)
			if err == nil {
				_, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil {
					t.Fatal(classifyErr)
				}
				err = conflict
			}
			if test.want != 0 {
				if !errors.Is(err, errs.New(test.want, "")) {
					t.Fatalf("publication: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			read, err := store.Get(t.Context(), agentoperations.InputKey(capture.WorkItemID))
			if err != nil {
				t.Fatal(err)
			}
			if (read.Entry != nil) != (test.want == 0) {
				t.Fatal("publication effects differ from admission outcome")
			}
		})
	}
}

type agentPublicationFenceStore struct {
	*memoryHierarchyStore
	race bool
}

func (store *agentPublicationFenceStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	read, err := store.memoryHierarchyStore.GetMany(ctx, request)
	if err == nil && store.race && len(request.Keys) == 1 && request.Keys[0] == imagefence.Key {
		store.race = false
		_, err = store.memoryHierarchyStore.Transact(
			ctx,
			nil,
			[]keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: []byte(`{"operation":"removing"}`)},
			},
		)
	}
	return read, err
}
