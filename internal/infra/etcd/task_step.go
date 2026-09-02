package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateTaskSteps(steps []TaskStepRecord) error {
	seenSteps := make(map[string]struct{}, len(steps))
	seenScriptIDs := make(map[string]struct{}, len(steps))
	seenScriptSlugs := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if err := validateStableID(ids.KindStep, step.ID); err != nil {
			return err
		}
		if _, exists := seenSteps[step.ID]; exists {
			return errs.New(errs.KindValidationFailed, "task step ids must be unique")
		}
		seenSteps[step.ID] = struct{}{}

		hasScriptID := step.ScriptID != ""
		hasScriptSlug := step.ScriptSlug != ""
		switch step.Kind {
		case TaskStepOperation:
			if hasScriptID || hasScriptSlug {
				return errs.New(errs.KindValidationFailed, "task operation step cannot carry Script identity")
			}
			continue
		case TaskStepScript:
			if !hasScriptID || !hasScriptSlug {
				return errs.New(errs.KindValidationFailed, "task Script step identity must include both id and slug")
			}
		default:
			return errs.New(errs.KindValidationFailed, "task step kind is invalid")
		}
		if err := validateStableID(ids.KindScript, step.ScriptID); err != nil {
			return err
		}
		if err := core.ValidateScriptLabel("task Script step slug", step.ScriptSlug); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
		if _, exists := seenScriptIDs[step.ScriptID]; exists {
			return errs.New(errs.KindValidationFailed, "task Script step ids must be unique")
		}
		if _, exists := seenScriptSlugs[step.ScriptSlug]; exists {
			return errs.New(errs.KindValidationFailed, "task Script step slugs must be unique")
		}
		seenScriptIDs[step.ScriptID] = struct{}{}
		seenScriptSlugs[step.ScriptSlug] = struct{}{}
	}
	return nil
}

func cloneTaskSteps(steps []TaskStepRecord) []TaskStepRecord {
	if steps == nil {
		return nil
	}
	cloned := make([]TaskStepRecord, len(steps))
	copy(cloned, steps)
	return cloned
}
