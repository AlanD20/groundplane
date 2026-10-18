package operations

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-54: evidence conflicts must reprepare a bounded request; replay after a
// retry returns the original action, and a non-conflict error is never retried.
// This does not qualify fresh Task publication or host cleanup.
func TestRemoveEntryRetriesEvidenceConflictWithinBound(t *testing.T) {
	for _, test := range []struct {
		name     string
		failures int
		kind     errs.Kind
		calls    int
		success  bool
	}{
		{"replay after conflict", 1, errs.KindStateConflict, 2, true},
		{"bound exhausted", maximumEntryEditAttempts, errs.KindStateConflict, maximumEntryEditAttempts, false},
		{"non-conflict", 1, errs.KindStorageUnavailable, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := &entryRemovalRetryRepository{
				entryID:       ids.New(ids.KindEnvEntry),
				environmentID: ids.New(ids.KindEnvironment),
			}
			removal := &entryRemovalRetryEvidence{
				failures: test.failures,
				kind:     test.kind,
				taskID:   ids.New(ids.KindTask),
			}
			service := &entryDesiredMutationService{repository: repository, removal: removal}
			outcome, err := service.RemoveEntry(t.Context(), entrycontroller.RemoveRequest{
				EntryID: repository.entryID, IdempotencyKey: "native-entry-removal-retry"})
			if test.success {
				if err != nil || outcome.TaskID != removal.taskID {
					t.Fatalf("retry lost the original accepted action: %v", err)
				}
			} else if !errors.Is(err, errs.New(test.kind, "")) || outcome.TaskID != "" {
				t.Fatalf("retry changed failure classification or invented an action: %v", err)
			}
			if removal.calls != test.calls || len(removal.buffers) != test.calls {
				t.Fatalf(
					"request did not honor retry bound: resolves=%d prepares=%d",
					removal.calls,
					len(removal.buffers),
				)
			}
			for _, buffer := range removal.buffers {
				if !allBytesZero(buffer) {
					t.Fatal("request retry retained protected intent bytes")
				}
			}
		})
	}
}

type entryRemovalRetryEvidence struct {
	failures, calls int
	kind            errs.Kind
	taskID          string
	buffers         [][]byte
}

func (owner *entryRemovalRetryEvidence) Prepare(context.Context, string, string) (entryDesiredRemovalEvidence, error) {
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

func (*entryRemovalRetryEvidence) MatchesStaged(
	context.Context,
	entryDesiredRemovalEvidence,
	idempotency.ProtectedIntentRecord,
) (bool, error) {
	return false, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryEvidence) ResolveKnown(
	context.Context,
	entryDesiredRemovalEvidence,
	etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return requestidempotency.Resolution{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryEvidence) ResolveUnknown(
	context.Context,
	idempotency.IdempotencyLocator,
	entryDesiredRemovalEvidence,
	error,
) (requestidempotency.Resolution, error) {
	return requestidempotency.Resolution{}, unexpectedEntryRemovalRetryOperation()
}

// Explicit failures keep this retry-only fixture from silently pretending that
// it exercises admission, publication, storage transactions or rendering.
type entryRemovalRetryRepository struct{ entryID, environmentID string }

func (repository *entryRemovalRetryRepository) ResolveBlueprintEntryEnvironment(
	context.Context,
	string,
) (string, bool, error) {
	return repository.environmentID, true, nil
}

func (repository *entryRemovalRetryRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], bool, error) {
	return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{
		Record: environmentprojection.EnvironmentComposeProjection{
			EnvironmentID: repository.environmentID, Entries: []entries.Record{{Entry: core.EnvEntry{ID: repository.entryID}}}},
	}, true, nil
}

func (*entryRemovalRetryRepository) InitializeEnvironmentDesiredState(
	context.Context,
	string,
	idempotency.IdempotencyLocator,
	idempotency.ProtectedIntentRecord,
	time.Time,
) error {
	return unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) ClaimEnvironmentBlueprintStage(
	context.Context,
	blueprints.EnvironmentBlueprintStageClaimRequest,
) (blueprints.EnvironmentBlueprintStageClaim, error) {
	return blueprints.EnvironmentBlueprintStageClaim{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) StageEnvironmentBlueprintRevision(
	context.Context,
	blueprints.EnvironmentBlueprintStageRequest,
) (blueprints.EnvironmentBlueprintSeal, error) {
	return blueprints.EnvironmentBlueprintSeal{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) GetEnvironmentDesiredInput(
	context.Context,
	string,
) (keyvalue.Versioned[environmentprojection.EnvironmentDesiredInput], bool, error) {
	return keyvalue.Versioned[environmentprojection.EnvironmentDesiredInput]{}, false, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) GetTenant(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.TenantRecord], error) {
	return keyvalue.Versioned[hierarchy.TenantRecord]{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) GetProject(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.ProjectRecord], error) {
	return keyvalue.Versioned[hierarchy.ProjectRecord]{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) GetEnvironment(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.EnvironmentRecord], error) {
	return keyvalue.Versioned[hierarchy.EnvironmentRecord]{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (keyvalue.Versioned[blueprints.EnvironmentBlueprintHead], bool, error) {
	return keyvalue.Versioned[blueprints.EnvironmentBlueprintHead]{}, false, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) ListServices(
	context.Context,
	string,
	keyvalue.PageRequest,
) (keyvalue.Page[services.ServiceRecord], error) {
	return keyvalue.Page[services.ServiceRecord]{}, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) ListEnvironmentComponents(
	context.Context,
	string,
	keyvalue.PageRequest,
) (keyvalue.Page[components.Record], error) {
	return keyvalue.Page[components.Record]{}, unexpectedEntryRemovalRetryOperation()
}
func (*entryRemovalRetryRepository) BlueprintEntryValueGenerationExists(context.Context, entries.Record) (bool, error) {
	return false, unexpectedEntryRemovalRetryOperation()
}

func (*entryRemovalRetryRepository) CreateBlueprintEntryValueGeneration(
	context.Context,
	entries.EntryValueGeneration,
) error {
	return unexpectedEntryRemovalRetryOperation()
}
func (*entryRemovalRetryRepository) BindBlueprintEntryEnvironment(context.Context, string, string) error {
	return unexpectedEntryRemovalRetryOperation()
}
func unexpectedEntryRemovalRetryOperation() error {
	return errs.New(errs.KindInternal, "unexpected side effect in Entry removal retry fixture")
}
