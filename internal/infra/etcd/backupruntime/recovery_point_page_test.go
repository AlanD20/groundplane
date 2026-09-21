package backupruntime

import (
	context "context"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testing "testing"
	time "time"
)

// Rationale: the public Recovery Point paging seam carries only a stable id;
// inverted ULID bodies and environment-index prefixes remain repository-private.
func TestRecoveryPointContinuationKeyRoundTripsToStableID(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC), 1)
	pointID := ids.NewAt(ids.KindRecoveryPoint, time.Date(2026, 8, 27, 12, 1, 0, 0, time.UTC), 2)
	key, err := BackupRecoveryPointEnvironmentIndexKey(environmentID, pointID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BackupRecoveryPointIDFromEnvironmentIndexKey(environmentID, key)
	if err != nil || got != pointID {
		t.Fatalf("stable continuation = %q, %v; want %q", got, err, pointID)
	}
	if _, err := BackupRecoveryPointIDFromEnvironmentIndexKey(
		environmentID, BackupRecoveryPointEnvironmentPrefix+
			environmentID+"/not-an-inverted-ulid",
	); err == nil {
		t.Fatal("continuation decoder accepted a storage-layout-shaped invalid boundary")
	}
}

// Rationale: a raw page can consist entirely of prune-hidden records. The
// repository must keep the first fixed revision, advance the private boundary,
// and scan until it fills the visible page or exhausts the index.
func TestVerifiedRecoveryPointPageScansHiddenRawPageBeforeVisiblePoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 10)
	hiddenID := ids.NewAt(ids.KindRecoveryPoint, at.Add(2*time.Minute), 11)
	visibleID := ids.NewAt(ids.KindRecoveryPoint, at.Add(time.Minute), 12)
	hiddenBoundary, err := BackupRecoveryPointEnvironmentIndexKey(environmentID, hiddenID)
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]BackupRuntimeListRequest, 0, 2)
	pages := []BackupRuntimePage[BackupRecoveryPointRecord]{
		{Revision: 41, Next: hiddenBoundary},
		{
			Items: []testkeyvalue.Versioned[BackupRecoveryPointRecord]{{
				Record: BackupRecoveryPointRecord{BackupRecoveryPointSnapshot: BackupRecoveryPointSnapshot{
					ID: visibleID,
				}},
				Revision: 40, ReadRevision: 41,
			}},
			Revision: 41,
		},
	}
	page, err := collectVerifiedRecoveryPointPage(
		ctx,
		environmentID, BackupRecoveryPointPageRequest{Limit: 1}, func(
			_ context.Context,
			gotEnvironmentID string,
			request BackupRuntimeListRequest,
		) (BackupRuntimePage[BackupRecoveryPointRecord], error) {
			if gotEnvironmentID != environmentID {
				t.Fatalf("environment id = %q, want %q", gotEnvironmentID, environmentID)
			}
			requests = append(requests, request)
			next := pages[0]
			pages = pages[1:]
			return next, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Record.ID != visibleID ||
		page.Revision != 41 || page.NextID != "" {
		t.Fatalf("visible page = %#v", page)
	}
	want := []BackupRuntimeListRequest{
		{Limit: 1},
		{Limit: 1, StartExclusive: hiddenBoundary, Revision: 41},
	}
	if len(requests) != len(want) {
		t.Fatalf("storage requests = %#v, want %#v", requests, want)
	}
	for index := range want {
		if requests[index] != want[index] {
			t.Fatalf("storage request %d = %#v, want %#v", index, requests[index], want[index])
		}
	}
}
