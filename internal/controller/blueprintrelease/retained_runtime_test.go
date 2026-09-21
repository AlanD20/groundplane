package blueprintrelease

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func TestRetainedCaptureDoesNotReadSelectedOrNewServices(t *testing.T) {
	service := &Service{} // A selected/new Service needs no retained-source I/O.
	environmentID := ids.New(ids.KindEnvironment)
	record := testservices.ServiceRecord{
		EnvironmentID: environmentID,
		Desired:       core.Service{ID: ids.New(ids.KindService)},
	}
	current := testkeyvalue.Versioned[testservices.ServiceRecord]{Record: record, Revision: 1, ReadRevision: 1}
	selected := testblueprints.EnvironmentBlueprintServiceChange{Current: &current, Record: record}
	for _, input := range []struct {
		changes, candidates []testblueprints.EnvironmentBlueprintServiceChange
	}{
		{changes: []testblueprints.EnvironmentBlueprintServiceChange{selected}, candidates: []testblueprints.EnvironmentBlueprintServiceChange{selected}},
		{changes: []testblueprints.EnvironmentBlueprintServiceChange{{Record: record}}},
	} {
		captured, err := service.captureRetainedRuntime(
			context.Background(),
			environmentID,
			input.changes,
			input.candidates,
		)
		if err != nil || captured != nil {
			t.Fatalf("unnecessary retained capture: %v %v", captured, err)
		}
	}
}
