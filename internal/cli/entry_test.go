package cli

import "testing"

func TestEntryFileOwnershipRequiresBothExplicitValues(t *testing.T) {
	// Rationale: zero is a valid numeric owner, so presence must come from the
	// flags rather than a nonzero-value heuristic and neither half may default.
	if _, err := entryFileOwnership("file", 0, 0, false, false); err == nil {
		t.Fatal("entryFileOwnership() accepted omitted ownership")
	}
	ownership, err := entryFileOwnership("file", 0, 0, true, true)
	if err != nil {
		t.Fatalf("entryFileOwnership() rejected explicit root ownership: %v", err)
	}
	if ownership["uid"] != uint32(0) || ownership["gid"] != uint32(0) {
		t.Fatalf("entryFileOwnership() = %#v", ownership)
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
	if source["attach_id"] != "att_01J" || source["fact"] != "pg16_URL" {
		t.Fatalf("buildEntrySource() = %#v", source)
	}
	if _, nested := source["source"]; nested {
		t.Fatalf("buildEntrySource() retained nested fact source: %#v", source)
	}
}
