package taskplanning

import (
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
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
	count, valid := taskcontract.BlueprintReleaseProcedureStepCount(len(members), hooks)
	if !valid {
		return 0, 0, errs.New(errs.KindInternal, "durable Blueprint Release procedure shape is invalid")
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
	hookCount, err := blueprintReleaseHookCount(members)
	if err != nil {
		return BlueprintReleasePlanInput{}, 0, err
	}
	count := len(members)*5 + hookCount
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
		ApplyStepIDs: make([]string, len(members)), HealthStepIDs: make([]string, len(members)),
		SwitchStepIDs:        make([]string, len(members)),
		RecoveryProbeStepIDs: make([]string, len(members)), RecoveryCompensateStepIDs: make([]string, len(members)),
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
		result.ApplyStepIDs[index] = task.Steps[cursor].ID
		cursor++
		if member.Render.Strategy == domain.StrategyBlueGreen {
			if err := readHooks(core.ScriptPostDeploy, index, result.PostStepIDs); err != nil {
				return BlueprintReleasePlanInput{}, 0, err
			}
		}
		result.HealthStepIDs[index] = task.Steps[cursor].ID
		cursor++
		if member.Render.Strategy == domain.StrategyRecreate {
			if err := readHooks(core.ScriptPostDeploy, index, result.PostStepIDs); err != nil {
				return BlueprintReleasePlanInput{}, 0, err
			}
		}
		result.SwitchStepIDs[index] = task.Steps[cursor].ID
		cursor++
	}
	for index := range members {
		result.RecoveryProbeStepIDs[index] = task.Steps[cursor].ID
		result.RecoveryCompensateStepIDs[index] = task.Steps[cursor+1].ID
		cursor += 2
	}
	return result, cursor, nil
}
