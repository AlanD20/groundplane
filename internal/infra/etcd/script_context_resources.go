package etcd

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// validateScriptContextResources binds selected resource identities and Entry
// metadata to the same fixed projection that final publication fences.
func validateScriptContextResources(sources ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) error {
	context := snapshot.ExplicitExecution.Context
	projection := sources.DesiredProjection.Record
	environmentID, serviceID := sources.Environment.Record.ID, sources.Service.Record.Desired.ID
	if environmentID == "" || serviceID == "" || projection.EnvironmentID != environmentID ||
		sources.Service.Record.EnvironmentID != environmentID || sources.Script.Record.EnvironmentID != environmentID ||
		sources.Script.Record.ServiceID != serviceID || sources.DesiredProjection.ReadRevision != sources.Revision {
		return errs.New(errs.KindValidationFailed, "captured Script resources have a different source scope")
	}
	volumes := make(map[string]int, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		volumes[volume.ID]++
	}
	for _, grant := range context.Volumes {
		if volumes[grant.VolumeId] != 1 {
			return errs.New(errs.KindValidationFailed, "captured Script Volume is not uniquely available in its source")
		}
	}
	entries := make(map[string]EntryRecord, len(context.EntryIds))
	for _, record := range projection.Entries {
		if !slices.Contains(context.EntryIds, record.Entry.ID) {
			continue
		}
		if _, duplicate := entries[record.Entry.ID]; duplicate || record.EnvironmentID != environmentID ||
			validateEntryRecord(record) != nil || (!record.Entry.ExposesAll() &&
			!slices.Contains(record.Entry.Exposure, sources.Service.Record.Desired.Name)) {
			return errs.New(errs.KindValidationFailed, "captured Script Entry is not uniquely exposed in its source")
		}
		entries[record.Entry.ID] = record
	}
	if len(entries) != len(context.EntryIds) || len(snapshot.EntryBindings) != len(context.EntryIds) {
		return errs.New(errs.KindValidationFailed, "captured Script Entry source coverage is incomplete")
	}
	for index, id := range context.EntryIds {
		binding := snapshot.EntryBindings[index]
		if binding == nil || binding.EntryId != id || !scriptContextEntryBindingMatches(entries[id], binding) {
			return errs.New(errs.KindValidationFailed, "captured Script Entry differs from its stored metadata")
		}
	}
	return nil
}

func scriptContextEntryBindingMatches(record EntryRecord, binding *agentpb.ScriptRunnerEntryBinding) bool {
	expected := &agentpb.ScriptRunnerEntryBinding{
		EntryId: record.Entry.ID, ValueGenerationId: record.CurrentValueGenerationID, Secret: record.Entry.Secret,
		// Private value preparation owns the plaintext hash; metadata must match
		// independently without decrypting anything at the publication boundary.
		Sha256: binding.Sha256,
	}
	if record.Entry.Kind == "file" {
		if entrymaterialization.ValidateDesiredDestination(record.Entry.Path) != nil {
			return false
		}
		expected.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE
		expected.FileTarget, expected.Uid, expected.Gid = "/"+record.Entry.Path, *record.Entry.UID, *record.Entry.GID
		expected.Mode = 0o444
		if record.Entry.Secret {
			expected.Mode = 0o600
		}
	} else {
		expected.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV
		expected.EnvironmentKey = record.Entry.Key
	}
	return proto.Equal(expected, binding)
}
