package scriptauthoring

import (
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DesiredMutation func(
	*core.BlueprintDesiredInput,
	*projectionrecord.EnvironmentComposeProjection,
) error

func Create(record scriptrecord.Record) DesiredMutation {
	return func(input *core.BlueprintDesiredInput, projection *projectionrecord.EnvironmentComposeProjection) error {
		key, spec, err := Script(record, projection.Volumes, projection.Entries)
		if err != nil {
			return err
		}
		if input.Scripts == nil {
			input.Scripts = make(map[string]core.ScriptSpec)
		}
		if _, exists := input.Scripts[key]; exists {
			return errs.New(errs.KindNameConflict, "Script authored key is already in use")
		}
		input.Scripts[key] = spec
		return nil
	}
}

func Replace(record scriptrecord.Record) DesiredMutation {
	return func(input *core.BlueprintDesiredInput, projection *projectionrecord.EnvironmentComposeProjection) error {
		key, spec, err := Script(record, projection.Volumes, projection.Entries)
		if err != nil {
			return err
		}
		if input.Scripts == nil {
			input.Scripts = make(map[string]core.ScriptSpec)
		}
		input.Scripts[key] = spec
		return nil
	}
}

func Remove(record scriptrecord.Record) DesiredMutation {
	return func(input *core.BlueprintDesiredInput, _ *projectionrecord.EnvironmentComposeProjection) error {
		key, err := scriptrecord.BlueprintAuthoringKey(record)
		if err != nil {
			return err
		}
		delete(input.Scripts, key)
		return nil
	}
}

// Script projects one durable Script back to its canonical authored form.
func Script(
	record scriptrecord.Record,
	volumes []projectionrecord.EnvironmentVolumeIdentity,
	entries []entryrecord.Record,
) (string, core.ScriptSpec, error) {
	if err := scriptrecord.ValidateRecord(record); err != nil {
		return "", core.ScriptSpec{}, err
	}
	key, err := scriptrecord.BlueprintAuthoringKey(record)
	if err != nil {
		return "", core.ScriptSpec{}, err
	}
	execution, err := authoringExecution(record, volumes, entries)
	if err != nil {
		return "", core.ScriptSpec{}, err
	}
	return key, core.ScriptSpec{
		Slug: record.Desired.Slug, Service: record.Desired.ServiceName,
		When: record.Desired.When, Order: record.Desired.Order,
		Script: record.Desired.Body, Execution: execution,
	}, nil
}

func authoringExecution(
	script scriptrecord.Record,
	volumes []projectionrecord.EnvironmentVolumeIdentity,
	entries []entryrecord.Record,
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
				key, count = entryrecord.BlueprintIdentityKey(entry), count+1
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
