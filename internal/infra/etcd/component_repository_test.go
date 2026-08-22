package etcd

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestComponentRepositoryCreatesReadsAndPagesEnvironmentSingletons(t *testing.T) {
	// Rationale: one write must atomically publish Component primary, owner,
	// and per-kind singleton indexes consumed by fixed-revision reads.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := componentRepositoryTestHierarchy(t)
	record := componentRepositoryTestRecord(t, environment.Record.ID, 1010)
	created, err := repository.CreateEnvironmentComponent(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	stored, err := repository.GetComponent(ctx, record.Desired.ID)
	if err != nil || !reflectComponentRecordEqual(stored.Record, record) || stored.Revision != created.Revision {
		t.Fatalf("GetComponent() = %#v, %v", stored, err)
	}
	page, err := repository.ListEnvironmentComponents(ctx, environment.Record.ID, PageRequest{Limit: 20})
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Desired.ID != record.Desired.ID {
		t.Fatalf("ListEnvironmentComponents() = %#v, %v", page, err)
	}
	duplicate := componentRepositoryTestRecord(t, environment.Record.ID, 1011)
	if _, err := repository.CreateEnvironmentComponent(
		ctx,
		environment,
		project,
		duplicate,
	); !isKind(err, errs.KindNameConflict) {
		t.Fatalf("CreateEnvironmentComponent(duplicate kind) error = %v", err)
	}
}

func TestComponentRepositoryFencesDesiredAndRuntimeCAS(t *testing.T) {
	// Rationale: concurrent config and runtime observations must serialize on
	// one Component revision without crossing their authorship domains.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := componentRepositoryTestHierarchy(t)
	record := componentRepositoryTestRecord(t, environment.Record.ID, 1020)
	current, err := repository.CreateEnvironmentComponent(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	component, err := ProjectComponentRecord(current.Record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord() error = %v", err)
	}
	component.Enabled = false
	updated, err := repository.ReplaceDesired(ctx, environment, project, current, component)
	if err != nil || !reflect.DeepEqual(updated.Record.Runtime, current.Record.Runtime) ||
		updated.Record.Desired.Enabled {
		t.Fatalf("ReplaceDesired() = %#v, %v", updated, err)
	}
	if _, err := repository.ReplaceRuntime(
		ctx,
		environment,
		project,
		current,
		[]string{ids.NewAt(ids.KindService, serviceRecordTestTime(), 1021)},
		"10.34.20.3",
		false,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("ReplaceRuntime(stale) error = %v", err)
	}
}

func componentRepositoryTestHierarchy(
	t *testing.T,
) (*ComponentRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	repository, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	return repository, store, environment, project
}

func componentRepositoryTestRecord(t *testing.T, environmentID string, offset int64) ComponentRecord {
	t.Helper()
	record := componentRecordTestRecord(t, offset)
	record.Desired.OwnerID = environmentID
	if err := validateComponentRecord(record); err != nil {
		t.Fatalf("validateComponentRecord() error = %v", err)
	}
	return record
}
