package core

import "testing"

// A credential reference must survive edits, while deleting or widening its
// selected Entry must fail before desired-state publication.
func TestTunnelTokenReferenceProtectsEnvironmentEntry(t *testing.T) {
	input := BlueprintDesiredInput{Components: map[string]ComponentSpec{"edge-tunnel": {
		Implementation: ComponentKindEdgeCloudflare,
		Settings:       ComponentCapabilitySettings{TokenEntry: "TUNNEL_TOKEN"},
	}}, Entries: map[string]EntrySpec{"TUNNEL_TOKEN": {
		Kind: EntryKindEnv, Secret: true, Source: EntrySourceSpec{Literal: ""},
		Exposure: []string{EntryExposureEdgeTunnel},
	}}}
	if err := input.ValidateComponentEntryReferences(); err != nil {
		t.Fatal(err)
	}
	input.Entries["TUNNEL_TOKEN"] = EntrySpec{Kind: EntryKindEnv, Secret: true, Exposure: []string{"all"}}
	if input.ValidateComponentEntryReferences() == nil {
		t.Fatal("accepted broadened credential exposure")
	}
	delete(input.Entries, "TUNNEL_TOKEN")
	if input.ValidateComponentEntryReferences() == nil {
		t.Fatal("accepted missing credential")
	}
}
