package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	keyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// A shared Backing view must page only its own indexed consumer connections;
// the cursor cannot be reused to read another Backing Project.
func TestBackingAttachPaginationKeepsOwnerAndRevision(t *testing.T) {
	store := newAttachTestStore()
	scope := seedAttachScope(t, t.Context(), store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	firstRecord, firstFacts := testPendingAttach(t, scope, 31, "first-db", nil)
	secondRecord, secondFacts := testPendingAttach(t, scope, 32, "second-db", nil)
	seedAttachMetadata(t, repository, firstRecord, &firstFacts)
	seedAttachMetadata(t, repository, secondRecord, &secondFacts)
	first, err := repository.ListAttachesByBackingProject(
		t.Context(),
		firstRecord.BackingProjectID,
		keyvalue.PageRequest{Limit: 1},
	)
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first Backing page = %#v, %v", first, err)
	}
	second, err := repository.ListAttachesByBackingProject(t.Context(), firstRecord.BackingProjectID,
		keyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision ||
		second.Items[0].Record.ID == first.Items[0].Record.ID {
		t.Fatalf("second Backing page = %#v, %v", second, err)
	}
	other := ids.NewAt(ids.KindProject, testAttachTime, 999)
	if _, err := repository.ListAttachesByBackingProject(t.Context(), other,
		keyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor}); err == nil {
		t.Fatal("Backing cursor crossed owner boundary")
	}
}
