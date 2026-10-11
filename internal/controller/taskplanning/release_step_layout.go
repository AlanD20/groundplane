package taskplanning

import (
	"fmt"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseMemberStepIDs keeps hook and recovery anchors independent of the number
// of forward steps. Task order plus the frozen render input owns these identities.
type ReleaseMemberStepIDs struct {
	Forward    []string
	Probe      string
	Compensate string
}

func releaseForwardStepCount(render releaserender.ReleaseRenderInput) (int, error) {
	switch render.Strategy {
	case domain.StrategyRecreate, domain.StrategyBlueGreen:
		if render.Rolling != nil {
			return 0, errs.New(errs.KindValidationFailed, "non-rolling Release has a rolling procedure")
		}
		return 3, nil
	case domain.StrategyRolling:
		if render.Rolling == nil {
			return 0, errs.New(errs.KindValidationFailed, "rolling Release has no captured replica plan")
		}
		changes, err := render.Rolling.Changes()
		if err != nil {
			return 0, err
		}
		count := 1 // Whole-set acknowledgement follows the last replica change.
		for _, change := range changes {
			count += len(change.Phases())
		}
		return count, nil
	default:
		return 0, errs.New(errs.KindValidationFailed, "Release step strategy is invalid")
	}
}

func AllocateReleaseMemberSteps(
	render releaserender.ReleaseRenderInput,
	allocate func(string) string,
) (ReleaseMemberStepIDs, error) {
	count, err := releaseForwardStepCount(render)
	if err != nil {
		return ReleaseMemberStepIDs{}, err
	}
	if allocate == nil {
		return ReleaseMemberStepIDs{}, errs.New(errs.KindInternal, "Release step allocator is missing")
	}
	steps := ReleaseMemberStepIDs{Forward: make([]string, count)}
	for index := range steps.Forward {
		steps.Forward[index] = allocate(fmt.Sprintf("release/%s/forward/%d", render.ServiceID, index+1))
	}
	steps.Probe = allocate("release/" + render.ServiceID + "/recovery-probe")
	steps.Compensate = allocate("release/" + render.ServiceID + "/recovery-compensate")
	return steps, steps.validate(render)
}

func (steps ReleaseMemberStepIDs) Records() []taskjournal.TaskStepRecord {
	identities := append(slices.Clone(steps.Forward), steps.Probe, steps.Compensate)
	result := make([]taskjournal.TaskStepRecord, len(identities))
	for index, identity := range identities {
		result[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: identity}
	}
	return result
}

func (steps ReleaseMemberStepIDs) validate(render releaserender.ReleaseRenderInput) error {
	count, err := releaseForwardStepCount(render)
	if err != nil {
		return err
	}
	if len(steps.Forward) != count {
		return errs.New(errs.KindValidationFailed, "Release steps do not cover the captured forward procedure")
	}
	seen := make(map[string]struct{}, count+2)
	for _, step := range steps.Records() {
		if ids.Validate(ids.KindStep, step.ID) != nil {
			return errs.New(errs.KindValidationFailed, "Release step identity is invalid")
		}
		if _, exists := seen[step.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Release step identity is repeated")
		}
		seen[step.ID] = struct{}{}
	}
	return nil
}

func ordinaryReleaseStepLayouts(
	records []taskjournal.TaskStepRecord,
	members []releaserender.ReleaseTaskRenderMember,
) ([]ReleaseMemberStepIDs, int, error) {
	layouts := make([]ReleaseMemberStepIDs, len(members))
	cursor := 0
	for index, member := range members {
		count, err := releaseForwardStepCount(member.Render)
		if err != nil {
			return nil, 0, err
		}
		if len(records)-cursor < count+2 {
			return nil, 0, errs.New(errs.KindValidationFailed, "Release Task is missing captured member steps")
		}
		layout := ReleaseMemberStepIDs{
			Forward:    make([]string, count),
			Probe:      records[cursor+count].ID,
			Compensate: records[cursor+count+1].ID,
		}
		for step := range count {
			layout.Forward[step] = records[cursor+step].ID
		}
		if err := layout.validate(member.Render); err != nil {
			return nil, 0, err
		}
		layouts[index] = layout
		cursor += count + 2
	}
	return layouts, cursor, nil
}

func releasePostHookIndex(render releaserender.ReleaseRenderInput) (int, error) {
	count, err := releaseForwardStepCount(render)
	if err != nil {
		return 0, err
	}
	switch render.Strategy {
	case domain.StrategyRecreate:
		return 1, nil
	case domain.StrategyBlueGreen:
		return 0, nil
	default:
		// A rolling post-deploy hook runs once, after the whole set serves.
		return count - 1, nil
	}
}
