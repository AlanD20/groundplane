package core

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumScriptExecutionVolumes = scriptpolicy.MaximumVolumes
	MaximumScriptExecutionEntries = scriptpolicy.MaximumEntries
)

// ScriptExecutionMode is a complete choice, never a partially merged context.
type ScriptExecutionMode string

const (
	ScriptExecutionInherited ScriptExecutionMode = "inherited"
	ScriptExecutionExplicit  ScriptExecutionMode = "explicit"
)

// ScriptExecution is desired authority only. Local image ids, source revisions
// and materialized Entry values belong to the immutable runner snapshot.
type ScriptExecution struct {
	Mode     ScriptExecutionMode `json:"mode"`
	Image    string              `json:"image,omitempty"`
	User     string              `json:"user,omitempty"`
	Volumes  []ScriptVolumeGrant `json:"volumes,omitempty"`
	EntryIDs []string            `json:"entry_ids,omitempty"`
}

// ScriptVolumeGrant grants one same-Environment managed Volume at one target.
// Write-boundary types must distinguish an omitted read_only from authored false.
type ScriptVolumeGrant struct {
	VolumeID string `json:"volume_id"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}

// Validate checks format and bounds. Fixed-source preparation separately proves
// Environment ownership, Entry exposure, availability and file-target isolation.
func (execution ScriptExecution) Validate() error {
	switch execution.Mode {
	case ScriptExecutionInherited:
		if execution.Image != "" || execution.User != "" || execution.Volumes != nil || execution.EntryIDs != nil {
			return errs.New(errs.KindValidationFailed, "inherited script execution accepts no explicit fields")
		}
		return nil
	case ScriptExecutionExplicit:
		if !imageref.IsDigestPinned(execution.Image) {
			return errs.New(
				errs.KindValidationFailed,
				"script execution image must be a repository SHA-256 digest reference",
			)
		}
	default:
		return errs.New(errs.KindValidationFailed, "script execution mode is invalid")
	}
	if _, _, err := scriptpolicy.NumericUser(execution.User); err != nil {
		return err
	}
	if len(execution.Volumes) > MaximumScriptExecutionVolumes ||
		len(execution.EntryIDs) > MaximumScriptExecutionEntries {
		return errs.New(errs.KindValidationFailed, "script execution resource grants exceed the supported bounds")
	}
	volumes := make(map[string]struct{}, len(execution.Volumes))
	for index, grant := range execution.Volumes {
		if ids.Validate(ids.KindVolume, grant.VolumeID) != nil {
			return errs.New(errs.KindValidationFailed, "script execution Volume grant requires a stable Volume id")
		}
		if _, duplicate := volumes[grant.VolumeID]; duplicate {
			return errs.New(errs.KindValidationFailed, "script execution Volume grant is duplicated")
		}
		volumes[grant.VolumeID] = struct{}{}
		if err := scriptpolicy.ValidateMountTarget(grant.Target); err != nil {
			return err
		}
		for _, previous := range execution.Volumes[:index] {
			if scriptpolicy.PathsOverlap(previous.Target, grant.Target) {
				return errs.New(errs.KindValidationFailed, "script execution Volume mount targets overlap")
			}
		}
	}
	entries := make(map[string]struct{}, len(execution.EntryIDs))
	for _, entryID := range execution.EntryIDs {
		if ids.Validate(ids.KindEnvEntry, entryID) != nil {
			return errs.New(errs.KindValidationFailed, "script execution Entry grant requires a stable Entry id")
		}
		if _, duplicate := entries[entryID]; duplicate {
			return errs.New(errs.KindValidationFailed, "script execution Entry grant is duplicated")
		}
		entries[entryID] = struct{}{}
	}
	return nil
}
