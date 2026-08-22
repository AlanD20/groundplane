package cli

import "testing"

func TestEntryFileOwnershipRequiresBothExplicitValues(t *testing.T) {
	// Rationale: zero is a valid numeric owner, so presence must come from the
	// flags rather than a nonzero-value heuristic and neither half may default.
	if _, _, err := entryFileOwnership("file", 0, 0, false, false); err == nil {
		t.Fatal("entryFileOwnership() accepted omitted ownership")
	}
	uid, gid, err := entryFileOwnership("file", 0, 0, true, true)
	if err != nil {
		t.Fatalf("entryFileOwnership() rejected explicit root ownership: %v", err)
	}
	if uid == nil || gid == nil || *uid != 0 || *gid != 0 {
		t.Fatalf("entryFileOwnership() = %#v/%#v", uid, gid)
	}
}

func TestBuildEntrySourceUsesFlatHumanAPIFactShape(t *testing.T) {
	// Rationale: docs/api-cli.md makes attach_id and fact direct members of
	// the discriminated human-API source; a nested Blueprint shape is not wire-compatible.
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
