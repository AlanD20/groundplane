package blueprintrelease

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestRetainedCaptureDoesNotReadSelectedOrNewServices(t *testing.T) {
	service := &Service{} // A selected/new Service needs no retained-source I/O.
	environmentID := ids.New(ids.KindEnvironment)
	record := etcd.ServiceRecord{EnvironmentID: environmentID, Desired: core.Service{ID: ids.New(ids.KindService)}}
	current := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 1, ReadRevision: 1}
	selected := etcd.EnvironmentBlueprintServiceChange{Current: &current, Record: record}
	for _, input := range []struct {
		changes, candidates []etcd.EnvironmentBlueprintServiceChange
	}{
		{changes: []etcd.EnvironmentBlueprintServiceChange{selected}, candidates: []etcd.EnvironmentBlueprintServiceChange{selected}},
		{changes: []etcd.EnvironmentBlueprintServiceChange{{Record: record}}},
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
