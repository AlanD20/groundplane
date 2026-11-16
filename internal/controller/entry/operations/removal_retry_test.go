package operations

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A transient replay-evidence read does not stage derived work or spin in an
// acceptance loop. An explicit repeated request resolves the same accepted Task.
func TestRemoveEntryReturnsEvidenceFailureAndPreservesReplay(t *testing.T) {
	for _, kind := range []errs.Kind{errs.KindStateConflict, errs.KindStorageUnavailable} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			repository := &entryRemovalRetryRepository{
				entryID:       ids.New(ids.KindEnvEntry),
				environmentID: ids.New(ids.KindEnvironment),
			}
			removal := &entryRemovalRetryEvidence{failures: 1, kind: kind, taskID: ids.New(ids.KindTask)}
			service := &entryDesiredMutationService{repository: repository, removal: removal}
			request := entrycontroller.RemoveRequest{
				EntryID:        repository.entryID,
				IdempotencyKey: "native-entry-removal-retry",
			}
			outcome, err := service.RemoveEntry(t.Context(), request)
			if !errors.Is(err, errs.New(kind, "")) || outcome.TaskID != "" || removal.calls != 1 {
				t.Fatalf("acceptance hid evidence failure or invented work: %v", err)
			}
			outcome, err = service.RemoveEntry(t.Context(), request)
			if err != nil || outcome.TaskID != removal.taskID || removal.calls != 2 {
				t.Fatalf("explicit retry lost accepted identity: %v", err)
			}
			for _, buffer := range removal.buffers {
				if !allBytesZero(buffer) {
					t.Fatal("acceptance retained protected intent bytes")
				}
			}
		})
	}
}

type entryRemovalRetryEvidence struct {
	entryDesiredRemovalIdempotency
	failures, calls int
	kind            errs.Kind
	taskID          string
	buffers         [][]byte
}

func (owner *entryRemovalRetryEvidence) Prepare(
	context.Context,
	string,
	string,
	[]string,
) (entryDesiredRemovalEvidence, error) {
	value := []byte("protected-removal-intent")
	owner.buffers = append(owner.buffers, value)
	return entryDesiredRemovalEvidence{durable: idempotency.ProtectedIntentRecord{Ciphertext: value}}, nil
}

func (*entryRemovalRetryEvidence) ResolveReplayLocator(
	context.Context,
	idempotency.IdempotencyReplayTarget,
	string,
	string,
	string,
) (idempotency.IdempotencyLocator, bool, error) {
	return idempotency.IdempotencyLocator{}, false, nil
}

func (owner *entryRemovalRetryEvidence) ResolveExisting(
	context.Context,
	idempotency.IdempotencyLocator,
	entryDesiredRemovalEvidence,
) (requestidempotency.Resolution, bool, error) {
	owner.calls++
	if owner.calls <= owner.failures {
		return requestidempotency.Resolution{}, false, errs.New(owner.kind, "injected removal evidence conflict")
	}
	return requestidempotency.Resolution{
		Kind: requestidempotency.ResolutionReplay,
		Response: idempotency.IdempotencyResponse{
			Status: 202, ContentKind: "application/json", Body: []byte(fmt.Sprintf(`{"task_id":%q}`, owner.taskID))},
	}, true, nil
}

// The embedded interface deliberately supplies no execution implementation.
// These cases stop at canonical evidence/replay and cannot publish native work.
type entryRemovalRetryRepository struct {
	entryDesiredMutationRepository
	entryID, environmentID string
}

func (repository *entryRemovalRetryRepository) ResolveBlueprintEntryEnvironment(
	context.Context,
	string,
) (string, bool, error) {
	return repository.environmentID, true, nil
}
