package taskplanning

import (
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// selectedScriptEntries validates the complete resource selection before the
// caller resolves any value. Inherited selection keeps its existing exposure rule.
func selectedScriptEntries(sources scriptsourcequeries.ScriptExecutionSources) ([]entryrecord.Record, error) {
	execution := sources.Script.Record.Desired.Execution
	if execution != nil {
		if err := execution.Validate(); err != nil {
			return nil, err
		}
		if execution.Mode == core.ScriptExecutionExplicit {
			return explicitScriptEntries(sources, *execution)
		}
	}
	entries := make([]entryrecord.Record, 0, len(sources.DesiredProjection.Record.Entries))
	for _, record := range sources.DesiredProjection.Record.Entries {
		if scriptEntryExposesService(record.Entry, sources.Service.Record.Desired.Name) {
			entries = append(entries, record)
		}
	}
	return entries, nil
}

func explicitScriptEntries(
	sources scriptsourcequeries.ScriptExecutionSources,
	execution core.ScriptExecution,
) ([]entryrecord.Record, error) {
	environmentID, serviceID := sources.Environment.Record.ID, sources.Service.Record.Desired.ID
	projection := sources.DesiredProjection.Record
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		projection.EnvironmentID != environmentID || sources.Service.Record.EnvironmentID != environmentID ||
		sources.Script.Record.EnvironmentID != environmentID || sources.Script.Record.ServiceID != serviceID {
		return nil, errs.New(errs.KindValidationFailed, "explicit Script resource scope is invalid")
	}
	volumes := make(map[string]int, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		volumes[volume.ID]++
	}
	for _, grant := range execution.Volumes {
		if volumes[grant.VolumeID] != 1 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"explicit Script Volume is not uniquely available in its Environment",
			)
		}
	}
	selected := make(map[string]entryrecord.Record, len(execution.EntryIDs))
	for _, id := range execution.EntryIDs {
		selected[id] = entryrecord.Record{}
	}
	for _, record := range projection.Entries {
		previous, wanted := selected[record.Entry.ID]
		if !wanted {
			continue
		}
		if previous.Entry.ID != "" || record.EnvironmentID != environmentID ||
			!scriptEntryExposesService(record.Entry, sources.Service.Record.Desired.Name) ||
			record.Entry.Validate() != nil || ids.Validate(ids.KindConfig, record.CurrentValueGenerationID) != nil {
			return nil, errs.New(
				errs.KindValidationFailed,
				"explicit Script Entry ownership, exposure or metadata is invalid",
			)
		}
		selected[record.Entry.ID] = record
	}
	entries := make([]entryrecord.Record, 0, len(selected))
	fileTargets := make([]string, 0, len(selected))
	environmentKeys := make(map[string]struct{}, len(selected))
	for _, id := range execution.EntryIDs {
		record := selected[id]
		if record.Entry.ID == "" {
			return nil, errs.New(errs.KindValidationFailed, "explicit Script Entry is unavailable")
		}
		if record.Entry.Kind == core.EntryKindFile {
			if err := validateExplicitScriptEntryTarget(record.Entry.Path, execution.Volumes, fileTargets); err != nil {
				return nil, err
			}
			fileTargets = append(fileTargets, "/"+record.Entry.Path)
		} else {
			key := record.Entry.Key
			if _, duplicate := environmentKeys[key]; duplicate || !utf8.ValidString(key) || strings.ContainsAny(key, "=\x00") {
				return nil, errs.New(errs.KindValidationFailed, "explicit Script Environment Entry key is invalid or duplicated")
			}
			environmentKeys[key] = struct{}{}
		}
		entries = append(entries, record)
	}
	return entries, nil
}

func validateExplicitScriptEntryTarget(destination string, volumes []core.ScriptVolumeGrant, previous []string) error {
	if err := entrymaterialization.ValidateDesiredDestination(destination); err != nil {
		return err
	}
	target := "/" + destination
	if err := scriptpolicy.ValidateMountTarget(target); err != nil {
		return err
	}
	for _, volume := range volumes {
		if scriptpolicy.PathsOverlap(target, volume.Target) {
			return errs.New(errs.KindValidationFailed, "explicit Script Entry overlaps a Volume target")
		}
	}
	for _, other := range previous {
		if scriptpolicy.PathsOverlap(target, other) {
			return errs.New(errs.KindValidationFailed, "explicit Script Entry file targets overlap")
		}
	}
	return nil
}
