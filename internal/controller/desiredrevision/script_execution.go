package desiredrevision

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"

	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintScriptResources is the intended same-Environment resource projection,
// after Entry reconciliation and managed Compose identity allocation. No storage
// or value lookup is permitted while resolving authored context keys.
type BlueprintScriptResources struct {
	Volumes []composeidentity.Resource
	Entries []entryrecord.Record
}

func (resources BlueprintScriptResources) resolveExecution(
	environmentID string, script core.ScriptSpec,
) (*core.ScriptExecution, error) {
	spec := script.Execution
	if spec == nil {
		return nil, nil
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if spec.Mode == core.ScriptExecutionInherited {
		return nil, nil
	}
	execution := &core.ScriptExecution{Mode: spec.Mode, Image: spec.Image, User: spec.User}
	for _, grant := range spec.Volumes {
		volumeID, count := "", 0
		for _, volume := range resources.Volumes {
			if volume.Name == grant.Volume {
				volumeID, count = volume.ID, count+1
			}
		}
		if count != 1 {
			return nil, errs.New(errs.KindValidationFailed, "script execution requires a unique candidate Volume key")
		}
		execution.Volumes = append(execution.Volumes, core.ScriptVolumeGrant{
			VolumeID: volumeID, Target: grant.Target, ReadOnly: *grant.ReadOnly,
		})
	}
	for _, key := range spec.Entries {
		entry, err := resources.resolveExecutionEntry(environmentID, script.Service, key)
		if err != nil {
			return nil, err
		}
		if entry.Kind == core.EntryKindFile {
			for _, grant := range execution.Volumes {
				if scriptpolicy.PathsOverlap(grant.Target, "/"+entry.Path) {
					return nil, errs.New(
						errs.KindValidationFailed,
						"script execution Volume overlaps an Entry file target",
					)
				}
			}
		}
		execution.EntryIDs = append(execution.EntryIDs, entry.ID)
	}
	if err := execution.Validate(); err != nil {
		return nil, err
	}
	return execution, nil
}

func (resources BlueprintScriptResources) resolveExecutionEntry(
	environmentID, serviceName, key string,
) (core.EnvEntry, error) {
	var entry core.EnvEntry
	count := 0
	for _, candidate := range resources.Entries {
		if candidate.BlueprintKey != key {
			continue
		}
		if candidate.EnvironmentID != environmentID || candidate.Entry.Validate() != nil ||
			!candidate.Entry.ExposesAll() && !slices.Contains(candidate.Entry.Exposure, serviceName) {
			return core.EnvEntry{}, errs.New(
				errs.KindValidationFailed,
				"script execution Entry is not exposed in its Environment",
			)
		}
		entry, count = candidate.Entry, count+1
	}
	if count != 1 {
		return core.EnvEntry{}, errs.New(
			errs.KindValidationFailed,
			"script execution requires a unique candidate Entry key",
		)
	}
	return entry, nil
}
