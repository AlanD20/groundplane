package scriptdefinition

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func authoringExecution(
	script etcd.ScriptRecord, volumes []etcd.EnvironmentVolumeIdentity, entries []entryrecord.Record,
) (*core.ScriptExecutionSpec, error) {
	execution := script.Desired.Execution
	if execution == nil {
		return nil, nil
	}
	if err := execution.Validate(); err != nil {
		return nil, errs.New(errs.KindInternal, "stored Script execution is invalid")
	}
	if execution.Mode == core.ScriptExecutionInherited {
		return nil, nil
	}
	result := &core.ScriptExecutionSpec{Mode: execution.Mode, Image: execution.Image, User: execution.User}
	for _, grant := range execution.Volumes {
		key, count := "", 0
		for _, volume := range volumes {
			if volume.ID == grant.VolumeID {
				key, count = volume.Key, count+1
			}
		}
		if count != 1 || key == "" {
			return nil, errs.New(errs.KindValidationFailed, "Script execution Volume has no unique Blueprint key")
		}
		readOnly := grant.ReadOnly
		result.Volumes = append(result.Volumes, core.ScriptVolumeGrantSpec{
			Volume: key, Target: grant.Target, ReadOnly: &readOnly,
		})
	}
	for _, id := range execution.EntryIDs {
		key, count := "", 0
		for _, entry := range entries {
			if entry.Entry.ID == id && entry.EnvironmentID == script.EnvironmentID {
				key, count = entry.BlueprintKey, count+1
			}
		}
		if count != 1 || key == "" {
			return nil, errs.New(errs.KindValidationFailed, "Script execution Entry has no unique Blueprint key")
		}
		result.Entries = append(result.Entries, key)
	}
	if err := result.Validate(); err != nil {
		return nil, err
	}
	return result, nil
}
