package scriptsourcequeries

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type unreadScriptAttachStore struct{}

func (unreadScriptAttachStore) Get(context.Context, string) (*keyvalue.GetResult, error) {
	panic("unexpected Script Attach source read")
}

func (unreadScriptAttachStore) GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	panic("unexpected Script Attach source read")
}

func (unreadScriptAttachStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	panic("unexpected Script Attach source read")
}

// Rationale: a Script may bind only Attaches owned by its Environment and
// intended for its Service; unrelated Service Attaches must not trigger reads.
func TestResolveScriptAttachNetworksSelectsOnlyIntendedService(t *testing.T) {
	at := time.Date(2026, 9, 1, 2, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	otherServiceID := ids.NewAt(ids.KindService, at, 3)
	attach := attachrecord.Record{
		ID: ids.NewAt(ids.KindAttach, at, 4), EnvironmentID: environmentID,
		ServiceID: otherServiceID,
	}
	resolved, err := resolveScriptAttachNetworks(
		context.Background(), unreadScriptAttachStore{}, environmentID, serviceID,
		[]keyvalue.Versioned[attachrecord.Record]{{Record: attach}}, 1,
	)
	if err != nil || len(resolved.Networks) != 0 || len(resolved.Heads) != 0 || len(resolved.Attaches) != 0 {
		t.Fatalf("unrelated Attach selected as Script source: %#v, %v", resolved, err)
	}

	attach.EnvironmentID = ids.NewAt(ids.KindEnvironment, at, 5)
	if _, err := resolveScriptAttachNetworks(
		context.Background(), unreadScriptAttachStore{}, environmentID, serviceID,
		[]keyvalue.Versioned[attachrecord.Record]{{Record: attach}}, 1,
	); err == nil {
		t.Fatal("foreign Environment Attach accepted as Script source")
	}
}
