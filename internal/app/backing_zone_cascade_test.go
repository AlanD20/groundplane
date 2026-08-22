package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: a dependent Attach must detach before every grant target while
// independent nodes retain a deterministic stable-id order.
func TestBackingZoneCascadeOrdersDependentsBeforeGrantTargets(t *testing.T) {
	now := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	first := ids.NewAt(ids.KindAttach, now, 1)
	grant := ids.NewAt(ids.KindAttach, now, 2)
	dependent := ids.NewAt(ids.KindAttach, now, 3)
	ordered, err := orderBackingZoneCascadeAttaches([]etcd.Versioned[etcd.AttachRecord]{
		{Record: etcd.AttachRecord{ID: grant}},
		{Record: etcd.AttachRecord{ID: dependent, GrantAttachIDs: []string{grant}}},
		{Record: etcd.AttachRecord{ID: first}},
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
	repeated, err := orderBackingZoneCascadeAttaches([]etcd.Versioned[etcd.AttachRecord]{
		{Record: etcd.AttachRecord{ID: first}},
		{Record: etcd.AttachRecord{ID: dependent, GrantAttachIDs: []string{grant}}},
		{Record: etcd.AttachRecord{ID: grant}},
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
	context.Context,
	string,
) (etcd.Versioned[etcd.ZoneRecord], error) {
	return etcd.Versioned[etcd.ZoneRecord]{}, nil
}

func (inertBackingZoneCascadeRepository) GetDeletionTombstone(
	context.Context,
	etcd.DeletionTargetKind,
	string,
) (etcd.Versioned[etcd.DeletionTombstoneRecord], bool, error) {
	return etcd.Versioned[etcd.DeletionTombstoneRecord]{}, false, nil
}

func (inertBackingZoneCascadeRepository) ListAttachesByBackingNetworkAtRevision(
	context.Context,
	string,
	string,
	int64,
) ([]etcd.Versioned[etcd.AttachRecord], error) {
	return nil, nil
}

func (inertBackingZoneCascadeRepository) GetTask(
	context.Context,
	string,
) (etcd.Versioned[etcd.TaskRecord], error) {
	return etcd.Versioned[etcd.TaskRecord]{}, nil
}

func (inertBackingZoneCascadeRepository) HandoffBackingZoneDeletion(
	context.Context,
	etcd.Versioned[etcd.ZoneRecord],
	string,
	etcd.Versioned[etcd.DeletionTombstoneRecord],
	etcd.TaskRecord,
	etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return etcd.IdempotencyTransactionResult{}, nil
}

type inertBackingZoneCascadeMutations struct{}

func (inertBackingZoneCascadeMutations) DetachAttach(
	context.Context,
	string,
	string,
) (etcd.IdempotencyResponse, error) {
	return etcd.IdempotencyResponse{}, nil
}

func (inertBackingZoneCascadeMutations) RetryTask(
	context.Context,
	string,
	string,
) (etcd.IdempotencyResponse, error) {
	return etcd.IdempotencyResponse{}, nil
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
