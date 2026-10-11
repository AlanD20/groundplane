package taskplanning

import (
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func blueprintReleaseHookCount(members []releaserender.ReleaseTaskRenderMember) (int, error) {
	total := 0
	for _, member := range members {
		for _, hook := range member.Render.Hooks {
			if hook.When != core.ScriptPostDeploy && hook.When != core.ScriptPreDeploy {
				return 0, errs.New(errs.KindInternal, "durable Blueprint Release hook selection is invalid")
			}
			total++
			if total > taskcontract.MaximumBlueprintPostDeployHooks {
				return 0, errs.New(errs.KindInternal, "durable Blueprint Release hook count is invalid")
			}
		}
	}
	return total, nil
}

func blueprintReleaseProcedureCounts(members []releaserender.ReleaseTaskRenderMember, enabled bool) (int, int, error) {
	if !enabled {
		return 0, 0, nil
	}
	hooks, err := blueprintReleaseHookCount(members)
	if err != nil {
		return 0, 0, err
	}
	if len(members) == 0 {
		return 0, 0, errs.New(errs.KindInternal, "durable Blueprint Release procedure shape is invalid")
	}
	count := hooks
	for _, member := range members {
		forward, err := releaseForwardStepCount(member.Render)
		if err != nil {
			return 0, 0, err
		}
		if count > int(^uint(0)>>1)-forward-2 {
			return 0, 0, errs.New(
				errs.KindValidationFailed,
				"Blueprint Release procedure exceeds the supported step count",
			)
		}
		count += forward + 2
	}
	return hooks, count, nil
}

func blueprintReleaseProcedureStepIDs(
	task etcd.TaskRecord,
	captured etcd.ReleaseTaskRenderInput,
	start int,
) (BlueprintReleasePlanInput, int, error) {
	members := captured.Members
	resourceIDs, _, err := blueprintReleaseResourceStepIDs(task)
	if err != nil {
		return BlueprintReleasePlanInput{}, 0, err
	}
	if start < 0 || start > len(task.Steps) || len(resourceIDs) > len(task.Steps)-start {
		return BlueprintReleasePlanInput{}, 0, errs.New(
			errs.KindInternal,
			"durable Blueprint resource preparation steps are incomplete",
		)
	}
	for _, stepID := range resourceIDs {
		if task.Steps[start].ID != stepID || task.Steps[start].Kind != taskjournal.TaskStepOperation {
			return BlueprintReleasePlanInput{}, 0, errs.New(
				errs.KindInternal,
				"durable Blueprint resource preparation order is invalid",
			)
		}
		start++
	}
	_, count, err := blueprintReleaseProcedureCounts(members, true)
	if err != nil {
		return BlueprintReleasePlanInput{}, 0, err
	}
	if len(members) == 0 || start < 0 || start > len(task.Steps) || count > len(task.Steps)-start {
		return BlueprintReleasePlanInput{}, 0, errs.New(
			errs.KindInternal,
			"durable Blueprint Release procedure steps are invalid",
		)
	}
	result := BlueprintReleasePlanInput{
		NativePredecessors: captured.NativePredecessors,
		Members:            members,
		PreStepIDs:         make([][]string, len(members)), PostStepIDs: make([][]string, len(members)),
		MemberSteps: make([]ReleaseMemberStepIDs, len(members)),
	}
	cursor := start
	readHooks := func(when core.ScriptHook, memberIndex int, target [][]string) error {
		member := members[memberIndex]
		for _, hook := range member.Render.Hooks {
			if hook.When != when {
				continue
			}
			stepID := task.Steps[cursor].ID
			if task.Params[releaserender.ReleaseHookStepMemberParam(stepID)] != strconv.Itoa(memberIndex+1) ||
				task.Params[releaserender.ReleaseHookStepExecutionParam(stepID)] != hook.ScriptExecutionID {
				return errs.New(errs.KindInternal, "durable Blueprint Release hook step authority is invalid")
			}
			target[memberIndex] = append(target[memberIndex], stepID)
			cursor++
		}
		return nil
	}
	for index := range members {
		if err := readHooks(core.ScriptPreDeploy, index, result.PreStepIDs); err != nil {
			return BlueprintReleasePlanInput{}, 0, err
		}
	}
	for index, member := range members {
		forwardCount, err := releaseForwardStepCount(member.Render)
		if err != nil {
			return BlueprintReleasePlanInput{}, 0, err
		}
		anchor, err := releasePostHookIndex(member.Render)
		if err != nil {
			return BlueprintReleasePlanInput{}, 0, err
		}
		result.MemberSteps[index].Forward = make([]string, forwardCount)
		for forwardIndex := range forwardCount {
			result.MemberSteps[index].Forward[forwardIndex] = task.Steps[cursor].ID
			cursor++
			if forwardIndex == anchor {
				if err := readHooks(core.ScriptPostDeploy, index, result.PostStepIDs); err != nil {
					return BlueprintReleasePlanInput{}, 0, err
				}
			}
		}
	}
	for index := range members {
		result.MemberSteps[index].Probe = task.Steps[cursor].ID
		result.MemberSteps[index].Compensate = task.Steps[cursor+1].ID
		if err := result.MemberSteps[index].validate(members[index].Render); err != nil {
			return BlueprintReleasePlanInput{}, 0, err
		}
		cursor += 2
	}
	return result, cursor, nil
}
