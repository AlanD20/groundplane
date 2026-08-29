package core

import (
	"fmt"
	"sort"
)

const maximumAuthoredEntryValueBytes = 256 << 10

// ProjectEntrySpec converts one authored x-gp-entry map member into the
// unified desired Entry model. logicalKey is identity input; for env Entries
// it is also the runtime variable name.
func ProjectEntrySpec(logicalKey string, spec EntrySpec, entryID string) (EnvEntry, error) {
	if logicalKey == "" {
		return EnvEntry{}, fmt.Errorf("entry: logical key is required")
	}
	sourceKinds := 0
	source := EntrySource{Kind: SourceLiteral, Literal: spec.Source.Literal}
	if spec.Source.Literal != "" {
		sourceKinds++
	}
	if spec.Source.SecretRef != "" {
		sourceKinds++
		source = EntrySource{Kind: SourceSecretRef, SecretRef: spec.Source.SecretRef}
	}
	if spec.Source.Fact != nil {
		sourceKinds++
		fact := *spec.Source.Fact
		source = EntrySource{Kind: SourceFact, Fact: &fact}
	}
	if sourceKinds > 1 {
		return EnvEntry{}, fmt.Errorf("entry %s: source kinds are mutually exclusive", logicalKey)
	}
	if len(spec.Source.Literal) > maximumAuthoredEntryValueBytes {
		return EnvEntry{}, fmt.Errorf("entry %s: literal exceeds 256 KiB", logicalKey)
	}
	if source.Kind == SourceSecretRef && !spec.Secret {
		return EnvEntry{}, fmt.Errorf("entry %s: secret_ref requires a secret destination", logicalKey)
	}
	exposure := append([]string(nil), spec.Exposure...)
	sort.Strings(exposure)
	for index, value := range exposure {
		if value == "" || index > 0 && value == exposure[index-1] {
			return EnvEntry{}, fmt.Errorf("entry %s: exposure is empty or duplicated", logicalKey)
		}
		if value == "all" && len(exposure) != 1 {
			return EnvEntry{}, fmt.Errorf("entry %s: exposure all must be the only value", logicalKey)
		}
	}
	entry := EnvEntry{
		ID: entryID, Kind: spec.Kind, Path: spec.Path, UID: spec.UID, GID: spec.GID,
		Source: source, Exposure: exposure, Secret: spec.Secret,
	}
	if entry.Kind == EntryKindEnv {
		entry.Key = logicalKey
	}
	validation := entry
	if validation.Secret && validation.Source.Kind == SourceLiteral {
		validation.Source.Literal = ""
	}
	if err := validation.Validate(); err != nil {
		return EnvEntry{}, err
	}
	return entry, nil
}
