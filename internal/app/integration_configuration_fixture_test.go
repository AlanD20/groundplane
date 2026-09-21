package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	// Rationale: SVC-15/JOURNEY-02 Route create/edit publication must persist the
	// exact configuration authority on the Task that owns its generated file.
)

func stageTestRuntimeConfiguration(
	t *testing.T, store *memoryHierarchyStore, environmentID string, generation uint64,
) []byte {
	t.Helper()
	repository, err := runtimeconfiguration.New(store)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.Stage(context.Background(), runtimeconfiguration.Snapshot{
		ID: ids.New(ids.KindConfig), EnvironmentID: environmentID, Generation: generation,
		Files: []testtaskmaterialization.Record{},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := runtimeconfiguration.EncodeReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
