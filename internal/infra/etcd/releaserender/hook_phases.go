package releaserender

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseHookPhases is the Release owner's ordered hook selection. Before runs
// before candidate activation, After follows candidate start, and
// Failure is selected only by the captured failure/recovery policy. It contains
// captured inputs, not generic steps, retries or implicit executable commands.
type ReleaseHookPhases struct {
	Before  []ReleaseHookRenderInput
	After   []ReleaseHookRenderInput
	Failure []ReleaseHookRenderInput
}

func OrderedReleaseHooks(operation domain.OperationKind, serviceID string,
	hooks []ReleaseHookRenderInput,
) (ReleaseHookPhases, error) {
	if err := validateReleaseHookRenderInputs(hooks, serviceID); err != nil {
		return ReleaseHookPhases{}, err
	}
	before, after := core.ScriptPreDeploy, core.ScriptPostDeploy
	switch operation {
	case domain.OperationDeploy, domain.OperationBlueprintApply:
	case domain.OperationRollback:
		before, after = core.ScriptPreRollback, core.ScriptPostRollback
	default:
		return ReleaseHookPhases{}, errs.New(errs.KindValidationFailed, "Release hook operation is invalid")
	}
	result := ReleaseHookPhases{}
	for _, hook := range cloneReleaseHookRenderInputs(hooks) {
		switch hook.When {
		case before:
			result.Before = append(result.Before, hook)
		case after:
			result.After = append(result.After, hook)
		case core.ScriptOnFailure:
			result.Failure = append(result.Failure, hook)
		default:
			result.Clear()
			return ReleaseHookPhases{}, errs.New(
				errs.KindValidationFailed,
				"captured hook trigger does not belong to this Release operation",
			)
		}
	}
	compare := func(left, right ReleaseHookRenderInput) int {
		if left.Order < right.Order {
			return -1
		}
		if left.Order > right.Order {
			return 1
		}
		return strings.Compare(left.ScriptSlug, right.ScriptSlug)
	}
	slices.SortFunc(result.Before, compare)
	slices.SortFunc(result.After, compare)
	slices.SortFunc(result.Failure, compare)
	return result, nil
}

func (phases *ReleaseHookPhases) Clear() {
	if phases == nil {
		return
	}
	for _, group := range [][]ReleaseHookRenderInput{phases.Before, phases.After, phases.Failure} {
		for _, hook := range group {
			clear(hook.RunnerSnapshot)
			clear(hook.RunnerProjection)
		}
	}
	*phases = ReleaseHookPhases{}
}
