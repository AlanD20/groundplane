package entry

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type removalRepositoryFake struct {
	candidate    RemovalCandidate
	outcome      RemovalOutcome
	inspectCalls int
	publishCalls int
}

func (repository *removalRepositoryFake) InspectRemoval(
	context.Context,
	RemoveRequest,
) (RemovalInspection, error) {
	repository.inspectCalls++
	return RemovalInspection{Candidate: repository.candidate}, nil
}

func (repository *removalRepositoryFake) PublishRemoval(
	context.Context,
	RemovalPublication,
) (RemovalOutcome, error) {
	repository.publishCalls++
	return repository.outcome, nil
}

type removalPlannerFake struct {
	failures int
	calls    int
}

func (planner *removalPlannerFake) PrepareEntryRemoval(
	_ context.Context,
	request RemovalPlanRequest,
) (RemovalTaskPlan, error) {
	planner.calls++
	if planner.calls <= planner.failures {
		return RemovalTaskPlan{}, errs.New(errs.KindStateConflict, "entry removal projection changed")
	}
	return RemovalTaskPlan{
		Executor: RemovalExecutorAgent, PlanHash: strings.Repeat("a", 64), RenderGeneration: 2,
		EnvironmentID:       request.EnvironmentID,
		BlueprintRevisionID: ids.NewAt(ids.KindTask, request.CreatedAt, 91),
		ArtifactID:          request.ArtifactID, Identity: request.Identity,
		Steps: []RemovalStep{{ID: ids.NewAt(ids.KindStep, request.CreatedAt, 92)}}, TimeoutSeconds: 120,
	}, nil
}

// Rationale: projection CAS conflicts are ordinary bounded-retry events. The
// use case must preserve their kind, retry the complete inspection and plan,
// and publish only the successful attempt.
func TestRemovalServiceRetriesPlannerStateConflict(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 26, 8, 0, 0, 0, time.UTC)
	candidate := removalServiceTestCandidate(at)
	want := RemovalOutcome{TaskID: ids.NewAt(ids.KindTask, at, 80)}
	repository := &removalRepositoryFake{candidate: candidate, outcome: want}
	planner := &removalPlannerFake{failures: 1}
	service, err := NewRemovalService(repository, planner)
	if err != nil {
		t.Fatalf("NewRemovalService() error = %v", err)
	}
	service.now = func() time.Time { return at }
	got, err := service.RemoveEntry(context.Background(), RemoveRequest{
		EntryID: candidate.EntryID, IdempotencyKey: "entry-remove-key-0003",
	})
	if err != nil || got != want || repository.inspectCalls != 2 || planner.calls != 2 || repository.publishCalls != 1 {
		t.Fatalf("RemoveEntry() = %#v/%v, inspect/plan/publish = %d/%d/%d", got, err,
			repository.inspectCalls, planner.calls, repository.publishCalls)
	}
}

// Rationale: exhausting the bounded planner retry must return the original
// state-conflict classification rather than mapping it to an internal error.
func TestRemovalServicePreservesPlannerConflictAtRetryBound(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 26, 9, 0, 0, 0, time.UTC)
	candidate := removalServiceTestCandidate(at)
	repository := &removalRepositoryFake{candidate: candidate}
	planner := &removalPlannerFake{failures: maximumEntryDeletionAttempts}
	service, err := NewRemovalService(repository, planner)
	if err != nil {
		t.Fatalf("NewRemovalService() error = %v", err)
	}
	_, err = service.RemoveEntry(context.Background(), RemoveRequest{
		EntryID: candidate.EntryID, IdempotencyKey: "entry-remove-key-0004",
	})
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) ||
		repository.inspectCalls != maximumEntryDeletionAttempts || repository.publishCalls != 0 {
		t.Fatalf("RemoveEntry() error/calls = %v/%d/%d", err, repository.inspectCalls, repository.publishCalls)
	}
}

// Rationale: the capability service constructor and repository seam must not
// expose etcd DTOs; persistence translation is owned only by EtcdRepository.
func TestRemovalCapabilityBoundaryContainsNoEtcdDTOs(t *testing.T) {
	t.Parallel()
	types := []reflect.Type{reflect.TypeFor[Repository](), reflect.TypeOf(NewRemovalService)}
	for _, boundary := range types {
		if containsEntryRemovalPersistenceType(boundary, make(map[reflect.Type]bool)) {
			t.Fatalf("entry removal boundary exposes persistence type: %s", boundary)
		}
	}
}

func containsEntryRemovalPersistenceType(typ reflect.Type, seen map[reflect.Type]bool) bool {
	if typ == nil {
		return false
	}
	if typ.PkgPath() == "github.com/AlanD20/groundplane/internal/infra/etcd" {
		return true
	}
	if seen[typ] {
		return false
	}
	seen[typ] = true
	switch typ.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Pointer, reflect.Slice:
		if typ.Kind() == reflect.Map && containsEntryRemovalPersistenceType(typ.Key(), seen) {
			return true
		}
		return containsEntryRemovalPersistenceType(typ.Elem(), seen)
	case reflect.Func:
		for index := 0; index < typ.NumIn(); index++ {
			if containsEntryRemovalPersistenceType(typ.In(index), seen) {
				return true
			}
		}
		for index := 0; index < typ.NumOut(); index++ {
			if containsEntryRemovalPersistenceType(typ.Out(index), seen) {
				return true
			}
		}
	case reflect.Interface:
		for index := 0; index < typ.NumMethod(); index++ {
			if containsEntryRemovalPersistenceType(typ.Method(index).Type, seen) {
				return true
			}
		}
	case reflect.Struct:
		for index := 0; index < typ.NumField(); index++ {
			if containsEntryRemovalPersistenceType(typ.Field(index).Type, seen) {
				return true
			}
		}
	}
	return false
}

func removalServiceTestCandidate(at time.Time) RemovalCandidate {
	return RemovalCandidate{
		EntryID: ids.NewAt(ids.KindEnvEntry, at, 1), EntryRevision: 11,
		EnvironmentRevision: 12, ProjectRevision: 13, TenantRevision: 14, ProjectionRevision: 15,
		Identity: RemovalEnvironmentIdentity{
			TenantID: ids.NewAt(ids.KindTenant, at, 2), TenantSlug: "tenant",
			ProjectID: ids.NewAt(ids.KindProject, at, 3), ProjectSlug: "project",
			EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 4), EnvironmentName: "production",
			AuthorizedVolumeDir: "/var/lib/groundplane/vol/environment",
		},
	}
}
