package network

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// Rationale: a dependent Attach must detach before every grant target while
// independent nodes retain a deterministic stable-id order.
func TestBackingZoneCascadeOrdersDependentsBeforeGrantTargets(t *testing.T) {
	now := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	first := ids.NewAt(ids.KindAttach, now, 1)
	grant := ids.NewAt(ids.KindAttach, now, 2)
	dependent := ids.NewAt(ids.KindAttach, now, 3)
	ordered, err := orderBackingZoneCascadeAttaches([]testkeyvalue.Versioned[testattachments.Record]{
		{Record: testattachments.Record{ID: grant}},
		{Record: testattachments.Record{ID: dependent, GrantAttachIDs: []string{grant}}},
		{Record: testattachments.Record{ID: first}},
	})
	if err != nil {
		t.Fatalf("orderBackingZoneCascadeAttaches() error = %v", err)
	}
	positions := map[string]int{}
	for index, attach := range ordered {
		positions[attach.Record.ID] = index
	}
	if positions[dependent] >= positions[grant] {
		t.Fatalf("cascade order = %#v", ordered)
	}
	repeated, err := orderBackingZoneCascadeAttaches([]testkeyvalue.Versioned[testattachments.Record]{
		{Record: testattachments.Record{ID: first}},
		{Record: testattachments.Record{ID: dependent, GrantAttachIDs: []string{grant}}},
		{Record: testattachments.Record{ID: grant}},
	})
	if err != nil || len(repeated) != len(ordered) {
		t.Fatalf("orderBackingZoneCascadeAttaches(repeated) = %#v, %v", repeated, err)
	}
	for index := range ordered {
		if repeated[index].Record.ID != ordered[index].Record.ID {
			t.Fatalf("cascade order is not deterministic: %#v / %#v", ordered, repeated)
		}
	}
}

type inertBackingZoneCascadeRepository struct{}

func (inertBackingZoneCascadeRepository) GetZone(
	context.Context, string,

) (testkeyvalue.Versioned[testzones.Record], error) {
	return testkeyvalue.Versioned[testzones.Record]{}, nil
}

func (inertBackingZoneCascadeRepository) GetDeletionTombstone(
	context.Context, testdeletions.DeletionTargetKind, string,

) (testkeyvalue.Versioned[testdeletions.DeletionTombstoneRecord], bool, error) {
	return testkeyvalue.Versioned[testdeletions.DeletionTombstoneRecord]{}, false, nil
}

func (inertBackingZoneCascadeRepository) ListAttachesByBackingNetworkAtRevision(
	context.Context, string, string,

	int64,
) ([]testkeyvalue.Versioned[testattachments.Record], error) {
	return nil, nil
}

func (inertBackingZoneCascadeRepository) GetTask(
	context.Context, string,

) (testkeyvalue.Versioned[etcd.TaskRecord], error) {
	return testkeyvalue.Versioned[etcd.TaskRecord]{}, nil
}

func (inertBackingZoneCascadeRepository) GetSystemTaskInitiation(
	context.Context, string,

) (etcd.TaskInitiation, error) {
	return etcd.TaskInitiation{}, nil
}

func (inertBackingZoneCascadeRepository) GetZoneRemovalIntent(
	context.Context, string,
) (testkeyvalue.Versioned[testenvironmentchanges.ZoneRemovalIntent], bool, error) {
	return testkeyvalue.Versioned[testenvironmentchanges.ZoneRemovalIntent]{}, false, nil
}

func (inertBackingZoneCascadeRepository) HandoffBackingZoneDeletion(
	context.Context,
	testkeyvalue.Versioned[testzones.Record],
	string,
	testkeyvalue.Versioned[testdeletions.DeletionTombstoneRecord],
	testenvironmentchanges.ZoneRemovalIntent,
	etcd.TaskRecord,
	testidempotencyowner.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return etcd.IdempotencyTransactionResult{}, nil
}

type inertBackingZoneCascadeMutations struct{}

func (inertBackingZoneCascadeMutations) DetachAttachWithInitiation(
	context.Context, string, string,

	etcd.TaskInitiation,
) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, nil
}

func (inertBackingZoneCascadeMutations) RetryTaskWithInitiation(
	context.Context, string, string,

	etcd.TaskInitiation,
) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, nil
}

func testBackingZoneCascade(t *testing.T) *backingZoneCascadeService {
	t.Helper()
	service, err := newBackingZoneCascadeService(
		inertBackingZoneCascadeRepository{},
		inertBackingZoneCascadeMutations{},
		inertBackingZoneCascadeMutations{},
		&fakeZoneDeletionPlans{},
		&fakeZoneDeletionIdempotency{},
	)
	if err != nil {
		t.Fatalf("newBackingZoneCascadeService() error = %v", err)
	}
	return service
}
