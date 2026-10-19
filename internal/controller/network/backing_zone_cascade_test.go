package network

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: a dependent Attach must detach before every grant target while
// independent nodes retain a deterministic stable-id order.
func TestBackingZoneCascadeOrdersDependentsBeforeGrantTargets(t *testing.T) {
	now := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	first := ids.NewAt(ids.KindAttach, now, 1)
	grant := ids.NewAt(ids.KindAttach, now, 2)
	dependent := ids.NewAt(ids.KindAttach, now, 3)
	service := ids.NewAt(ids.KindService, now, 4)
	ordered, err := orderBackingZoneCascadeAttaches([]testkeyvalue.Versioned[testattachments.Record]{
		{Record: testattachments.Record{ID: grant, CredentialAttachID: grant, BackingServiceID: service}},
		{Record: testattachments.Record{ID: dependent, CredentialAttachID: dependent, BackingServiceID: service,
			GrantAttachIDs: []string{grant}}},
		{Record: testattachments.Record{ID: first, CredentialAttachID: first, BackingServiceID: service}},
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
		{Record: testattachments.Record{ID: first, CredentialAttachID: first, BackingServiceID: service}},
		{Record: testattachments.Record{ID: dependent, CredentialAttachID: dependent, BackingServiceID: service,
			GrantAttachIDs: []string{grant}}},
		{Record: testattachments.Record{ID: grant, CredentialAttachID: grant, BackingServiceID: service}},
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
