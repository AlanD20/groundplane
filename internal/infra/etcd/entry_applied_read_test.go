package etcd

import (
	"context"
	"reflect"
	"testing"
)

func TestHierarchyReadsAppliedEntryProjectionFromRuntimeRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store, environment, _, current, _ := entryMetadataFixture(t)
	want := seedAppliedEntryProjection(t, store, current)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	got, found, err := hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || got.Revision != want.Revision ||
		got.Record.EnvironmentID != want.Record.EnvironmentID || got.Record.RevisionID != want.Record.RevisionID ||
		!reflect.DeepEqual(got.Record.Entries, want.Record.Entries) {
		t.Fatalf(
			"GetEnvironmentAppliedComposeProjection() = %#v, %v, %v; want revision %d",
			got,
			found,
			err,
			want.Revision,
		)
	}
}
