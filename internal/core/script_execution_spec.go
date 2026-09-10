package core

import (
	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptSpec is one x-gp-scripts entry keyed by immutable reconciliation key.
type ScriptSpec struct {
	Slug      string               `yaml:"slug"`
	Service   string               `yaml:"service"`
	When      ScriptHook           `yaml:"when"`
	Script    string               `yaml:"script"`
	Order     uint16               `yaml:"order,omitempty"`
	Execution *ScriptExecutionSpec `yaml:"execution,omitempty"`
}

// ScriptExecutionSpec is authored desired input. Resource keys are resolved by
// the Controller; no generated stable ids or image-resolution receipts belong here.
type ScriptExecutionSpec struct {
	Mode    ScriptExecutionMode     `yaml:"mode"`
	Image   string                  `yaml:"image,omitempty"`
	User    string                  `yaml:"user,omitempty"`
	Volumes []ScriptVolumeGrantSpec `yaml:"volumes,omitempty"`
	Entries []string                `yaml:"entries,omitempty"`
}

// ReadOnly retains the author's decision until the typed desired projection.
type ScriptVolumeGrantSpec struct {
	Volume   string `yaml:"volume"`
	Target   string `yaml:"target"`
	ReadOnly *bool  `yaml:"read_only"`
}

func (spec ScriptExecutionSpec) Validate() error {
	if err := validateScriptExecutionIdentity(spec.Mode, spec.Image, spec.User,
		spec.Volumes != nil, spec.Entries != nil); err != nil {
		return err
	}
	if len(spec.Volumes) > MaximumScriptExecutionVolumes || len(spec.Entries) > MaximumScriptExecutionEntries {
		return errs.New(errs.KindValidationFailed, "script execution resource grants exceed the supported bounds")
	}
	volumes := make(map[string]struct{}, len(spec.Volumes))
	for index, grant := range spec.Volumes {
		if grant.Volume == "" || grant.ReadOnly == nil {
			return errs.New(errs.KindValidationFailed, "script execution Volume requires a key and read_only decision")
		}
		if _, duplicate := volumes[grant.Volume]; duplicate {
			return errs.New(errs.KindValidationFailed, "script execution Volume grant is duplicated")
		}
		volumes[grant.Volume] = struct{}{}
		if err := scriptpolicy.ValidateMountTarget(grant.Target); err != nil {
			return err
		}
		for _, previous := range spec.Volumes[:index] {
			if scriptpolicy.PathsOverlap(previous.Target, grant.Target) {
				return errs.New(errs.KindValidationFailed, "script execution Volume mount targets overlap")
			}
		}
	}
	entries := make(map[string]struct{}, len(spec.Entries))
	for _, key := range spec.Entries {
		if key == "" {
			return errs.New(errs.KindValidationFailed, "script execution Entry grant requires a key")
		}
		if _, duplicate := entries[key]; duplicate {
			return errs.New(errs.KindValidationFailed, "script execution Entry grant is duplicated")
		}
		entries[key] = struct{}{}
	}
	return nil
}
