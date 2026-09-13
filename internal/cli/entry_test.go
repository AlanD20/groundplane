package cli

import (
	"context"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type pagedEntryLister struct {
	cursors []string
}

func (lister *pagedEntryLister) ListEntries(
	_ context.Context,
	_ string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Entry], error) {
	lister.cursors = append(lister.cursors, cursor)
	if limit != 200 {
		panic("entry list did not request the maximum page size")
	}
	if cursor == "" {
		return apiTypes.Page[apiTypes.Entry]{
			Items: []apiTypes.Entry{{ID: "ev_first"}, {ID: "ev_second"}}, NextCursor: "next",
		}, nil
	}
	return apiTypes.Page[apiTypes.Entry]{Items: []apiTypes.Entry{{ID: "ev_third"}}}, nil
}

// QA: UI-01; local CLI pagination helper only, not rendered output or live surface parity.
// Rationale: Entry-backed commands must follow opaque cursors so later-page
// resources are not silently omitted from list output or Script grant resolution.
func TestListAllCLIEntriesDrainsOpaquePagination(t *testing.T) {
	lister := &pagedEntryLister{}
	entries, err := listAllCLIEntries(context.Background(), lister, "env_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatalf("listAllCLIEntries() error = %v", err)
	}
	if len(entries) != 3 || entries[2].ID != "ev_third" ||
		len(lister.cursors) != 2 || lister.cursors[0] != "" || lister.cursors[1] != "next" {
		t.Fatalf("listAllCLIEntries() = %#v, cursors = %#v", entries, lister.cursors)
	}
}

// QA: ENT-04, UI-03; pure CLI flag validation only, not file materialization or host ownership.
// Rationale: zero is a valid numeric owner, so presence must come from the
// flags rather than a nonzero-value heuristic and neither half may default.
func TestEntryFileOwnershipRequiresBothExplicitValues(t *testing.T) {
	for _, presence := range []struct {
		uid bool
		gid bool
	}{
		{},
		{uid: true},
		{gid: true},
	} {
		if _, _, err := entryFileOwnership("file", 0, 0, presence.uid, presence.gid); err == nil {
			t.Fatalf("entryFileOwnership() accepted ownership presence uid=%t gid=%t", presence.uid, presence.gid)
		}
	}
	uid, gid, err := entryFileOwnership("file", 0, 0, true, true)
	if err != nil {
		t.Fatalf("entryFileOwnership() rejected explicit root ownership: %v", err)
	}
	if uid == nil || gid == nil || *uid != 0 || *gid != 0 {
		t.Fatalf("entryFileOwnership() = %#v/%#v", uid, gid)
	}
}

// QA: ENT-02, UI-01; pure CLI request shaping only, not source resolution, capture, or materialization.
// Rationale: docs/api-cli.md makes attach_id and fact direct members of
// the discriminated human-API source; a nested Blueprint shape is not wire-compatible.
func TestBuildEntrySourceUsesFlatHumanAPIFactShape(t *testing.T) {
	source, err := buildEntrySource(entrySourceOptions{
		factAttach: "att_01J", factKey: "pg16_URL", factSet: true,
	})
	if err != nil {
		t.Fatalf("buildEntrySource() error = %v", err)
	}
	if source.AttachID != "att_01J" || source.Fact != "pg16_URL" {
		t.Fatalf("buildEntrySource() = %#v", source)
	}
	if source.Kind != "fact" {
		t.Fatalf("buildEntrySource() retained nested fact source: %#v", source)
	}
}
