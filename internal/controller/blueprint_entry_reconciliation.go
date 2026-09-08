package controller

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintEntryValueCandidate struct {
	Record  etcd.EntryRecord
	Desired core.EnvEntry
}

type BlueprintEntryReconciliation struct {
	Current []etcd.EntryRecord
	Values  []BlueprintEntryValueCandidate
	Removed []etcd.EntryRecord
}

// ReconcileBlueprintEntries owns only records previously authored through
// x-gp-entry. Direct and component-owned Entries have no BlueprintKey and are
// preserved. Omission removes an authored record from the next projection.
func ReconcileBlueprintEntries(
	environmentID string,
	authored map[string]core.EntrySpec,
	current []etcd.EntryRecord,
	allocate func(ids.Kind, string) string,
) (BlueprintEntryReconciliation, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || allocate == nil {
		return BlueprintEntryReconciliation{}, errs.New(
			errs.KindInternal,
			"Blueprint Entry reconciliation input is invalid",
		)
	}
	currentByKey := make(map[string]etcd.EntryRecord)
	result := BlueprintEntryReconciliation{Current: make([]etcd.EntryRecord, 0, len(current)+len(authored))}
	for _, record := range current {
		if record.EnvironmentID != environmentID || record.Entry.Validate() != nil {
			return BlueprintEntryReconciliation{}, errs.New(
				errs.KindInternal,
				"durable Blueprint Entry state is inconsistent",
			)
		}
		if record.BlueprintKey == "" {
			result.Current = append(result.Current, record)
			continue
		}
		if _, duplicate := currentByKey[record.BlueprintKey]; duplicate {
			return BlueprintEntryReconciliation{}, errs.New(
				errs.KindInternal,
				"durable Blueprint Entry key is duplicated",
			)
		}
		currentByKey[record.BlueprintKey] = record
	}
	keys := make([]string, 0, len(authored))
	for key := range authored {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		existing, found := currentByKey[key]
		entryID := ""
		if found {
			entryID = existing.Entry.ID
		} else {
			entryID = allocate(ids.KindEnvEntry, "entry/"+key)
		}
		desired, err := core.ProjectEntrySpec(key, authored[key], entryID)
		if err != nil {
			return BlueprintEntryReconciliation{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		if desired.Kind == core.EntryKindFile && entrymaterialization.ValidateDesiredDestination(desired.Path) != nil {
			return BlueprintEntryReconciliation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint file Entry destination is invalid",
			)
		}
		if found && blueprintEntryIdentityChanged(existing.Entry, desired) {
			result.Removed = append(result.Removed, existing)
			desired.ID = allocate(ids.KindEnvEntry, "entry-replacement/"+key)
			found = false
		}
		persisted := desired
		if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
			persisted.Source.Literal = ""
		}
		changed := !found || !sameBlueprintEntryDesired(existing.Entry, persisted) ||
			desired.Secret && desired.Source.Kind == core.SourceLiteral && desired.Source.Literal != ""
		generationID := existing.CurrentValueGenerationID
		if changed {
			generationID = allocate(ids.KindConfig, "entry-generation/"+key)
		}
		record, err := etcd.NewBlueprintEntryRecord(environmentID, key, persisted, generationID)
		if err != nil {
			return BlueprintEntryReconciliation{}, err
		}
		result.Current = append(result.Current, record)
		if changed {
			result.Values = append(result.Values, BlueprintEntryValueCandidate{Record: record, Desired: desired})
		}
		delete(currentByKey, key)
	}
	for _, record := range currentByKey {
		result.Removed = append(result.Removed, record)
	}
	sort.Slice(
		result.Current,
		func(left, right int) bool { return result.Current[left].Entry.ID < result.Current[right].Entry.ID },
	)
	sort.Slice(
		result.Removed,
		func(left, right int) bool { return result.Removed[left].Entry.ID < result.Removed[right].Entry.ID },
	)
	return result, nil
}

func blueprintEntryIdentityChanged(current core.EnvEntry, desired core.EnvEntry) bool {
	return current.Kind != desired.Kind || current.Key != desired.Key || current.Path != desired.Path ||
		current.Secret != desired.Secret || !sameUint32(current.UID, desired.UID) || !sameUint32(current.GID, desired.GID)
}

func sameBlueprintEntryDesired(current core.EnvEntry, desired core.EnvEntry) bool {
	if blueprintEntryIdentityChanged(current, desired) || current.Source.Kind != desired.Source.Kind ||
		current.Source.Literal != desired.Source.Literal || current.Source.SecretRef != desired.Source.SecretRef ||
		len(current.Exposure) != len(desired.Exposure) {
		return false
	}
	if (current.Source.Fact == nil) != (desired.Source.Fact == nil) {
		return false
	}
	if current.Source.Fact != nil && *current.Source.Fact != *desired.Source.Fact {
		return false
	}
	for index := range current.Exposure {
		if current.Exposure[index] != desired.Exposure[index] {
			return false
		}
	}
	return true
}

func sameUint32(left, right *uint32) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
