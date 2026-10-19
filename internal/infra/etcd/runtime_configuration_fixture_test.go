package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
)

func stageTestRuntimeConfiguration(
	t *testing.T, store *memoryHierarchyStore, environmentID string, generation uint64,
) []byte {
	t.Helper()
	repository, err := runtimeconfiguration.New(store)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.Stage(t.Context(), runtimeconfiguration.Snapshot{
		ID: ids.New(ids.KindConfig), EnvironmentID: environmentID, Generation: generation,
		Files: []taskmaterialization.Record{},
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
