package core

import "testing"

func TestProjectEntrySpecNormalizesExposureAndRejectsMixedSources(t *testing.T) {
	entry, err := ProjectEntrySpec("APP_MODE", EntrySpec{
		Kind: EntryKindEnv, Source: EntrySourceSpec{Literal: "production"},
		Exposure: []string{"worker", "api"},
	}, "ev_00000000000000000000000000")
	if err != nil || len(entry.Exposure) != 2 || entry.Exposure[0] != "api" || entry.Exposure[1] != "worker" {
		t.Fatalf("ProjectEntrySpec() = %#v, %v", entry, err)
	}
	_, err = ProjectEntrySpec("APP_MODE", EntrySpec{
		Kind:     EntryKindEnv,
		Source:   EntrySourceSpec{Literal: "production", SecretRef: "DATABASE_PASSWORD"},
		Exposure: []string{"all"}, Secret: true,
	}, "ev_00000000000000000000000000")
	if err == nil {
		t.Fatal("mixed Entry source was accepted")
	}
}
