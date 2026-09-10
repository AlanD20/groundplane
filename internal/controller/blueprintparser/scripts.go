package blueprintparser

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/compose-spec/compose-go/v2/types"
)

func normalizeScripts(values map[string]core.ScriptSpec) (map[string]core.ScriptSpec, error) {
	if len(values) > 64 {
		return nil, validationError("x-gp-scripts exceeds the 64 Script limit")
	}
	seenSlugs := make(map[string]struct{}, len(values))
	for key, value := range values {
		if core.ValidateScriptLabel("script reconciliation key", key) != nil {
			return nil, validationError("x-gp-scripts reconciliation key is invalid")
		}
		if core.ValidateScriptLabel("script slug", value.Slug) != nil {
			return nil, validationError("x-gp-scripts slug is invalid")
		}
		if _, duplicate := seenSlugs[value.Slug]; duplicate {
			return nil, validationError("x-gp-scripts slugs must be unique")
		}
		seenSlugs[value.Slug] = struct{}{}
		if value.Service == "" {
			return nil, validationError("x-gp-scripts service is required")
		}
		if strings.TrimSpace(value.Script) == "" || !utf8.ValidString(value.Script) ||
			strings.ContainsRune(value.Script, '\x00') || len(value.Script) > core.MaximumScriptBodyBytes {
			return nil, validationError("x-gp-scripts body is invalid")
		}
		candidate := core.Script{
			ID: "script-validation", Slug: value.Slug, ServiceName: value.Service,
			Body: value.Script, When: value.When, Order: value.Order,
		}
		if err := candidate.Validate(); err != nil {
			return nil, validationError("x-gp-scripts entry is invalid")
		}
		if value.Execution != nil {
			if err := value.Execution.Validate(); err != nil {
				return nil, err
			}
		}
	}
	return values, nil
}

func validateScriptReferences(
	values map[string]core.ScriptSpec, project *types.Project, entries map[string]core.EntrySpec,
) error {
	for _, value := range values {
		service, exists := project.Services[value.Service]
		if !exists {
			return validationError("x-gp-scripts target must be an enabled Service in the same Environment")
		}
		if service.GetScale() < 1 {
			return validationError("x-gp-scripts target Service must have positive effective replicas")
		}
		if err := validateScriptExecutionReferences(value, project.Volumes, entries); err != nil {
			return err
		}
	}
	return nil
}

func validateScriptExecutionReferences(
	script core.ScriptSpec, volumes types.Volumes, entries map[string]core.EntrySpec,
) error {
	if script.Execution == nil {
		return nil
	}
	for _, grant := range script.Execution.Volumes {
		if _, exists := volumes[grant.Volume]; !exists {
			return validationError("script execution references an unknown Compose Volume key")
		}
	}
	for _, key := range script.Execution.Entries {
		entry, exists := entries[key]
		if !exists || !(len(entry.Exposure) == 1 && entry.Exposure[0] == "all") &&
			!slices.Contains(entry.Exposure, script.Service) {
			return validationError("script execution Entry must exist and be exposed to its Service")
		}
		if entry.Kind != core.EntryKindFile {
			continue
		}
		for _, grant := range script.Execution.Volumes {
			if scriptpolicy.PathsOverlap(grant.Target, "/"+entry.Path) {
				return validationError("script execution Volume overlaps an Entry file target")
			}
		}
	}
	return nil
}
