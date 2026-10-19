package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

func seedBlueprintUnitInitialAbsence(
	t *testing.T, store *memoryTaskStore, environmentID, parentID string, targets ...blueprintunits.ResourceKey,
) {
	t.Helper()
	publication, err := blueprintunits.PrepareInitialAbsencePublication(environmentID, parentID, targets)
	if err != nil {
		t.Fatal(err)
	}
	defer publication.Clear()
	if result, err := store.Transact(context.Background(), publication.Conditions(), publication.Mutations()); err != nil ||
		!result.Succeeded {
		t.Fatalf("seed unit initial absence = %#v, %v", result, err)
	}
}
