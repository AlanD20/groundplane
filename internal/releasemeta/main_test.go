package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
)

// Rationale: release compatibility comes from the built source's Go constants,
// not a duplicated deployment default or execution of a candidate on the host.
func TestReleaseBuildMetadataPinsSourceAndBinary(t *testing.T) {
	directory := t.TempDir()
	binary, output := filepath.Join(directory, "controller"), filepath.Join(directory, "metadata.json")
	raw := []byte("candidate bytes, not executable")
	if err := os.WriteFile(binary, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-controller", binary, "-version", "0.1.0-test", "-output", output}); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var metadata upgrade.BuildMetadata
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.ControllerSHA256 != upgrade.Hash(raw) || metadata.ControllerVersion != "0.1.0-test" ||
		metadata.StorageEpoch != upgrade.StorageEpoch || metadata.ChannelSchema != executionplan.SchemaVersion {
		t.Fatalf("build metadata = %#v", metadata)
	}
	if err := run([]string{"-controller", binary, "-version", "bad version", "-output", output}); err == nil {
		t.Fatal("invalid version accepted")
	}
}
