package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintTaskScriptIdentity struct {
	id   string
	slug string
}

func blueprintTaskStepRecords(
	steps []*agentpb.ExecutionStep,
	members []etcd.ReleaseTaskRenderMember,
) ([]taskjournal.TaskStepRecord, error) {
	byExecution := make(map[string]blueprintTaskScriptIdentity)
	for _, member := range members {
		for _, hook := range member.Render.Hooks {
			if hook.ScriptExecutionID == "" || hook.ScriptID == "" || hook.ScriptSlug == "" {
				return nil, errs.New(errs.KindValidationFailed, "blueprint Script step identity is incomplete")
			}
			if _, exists := byExecution[hook.ScriptExecutionID]; exists {
				return nil, errs.New(errs.KindValidationFailed, "blueprint Script execution identity is duplicated")
			}
			byExecution[hook.ScriptExecutionID] = blueprintTaskScriptIdentity{
				id: hook.ScriptID, slug: hook.ScriptSlug,
			}
		}
	}

	result := make([]taskjournal.TaskStepRecord, len(steps))
	for index, step := range steps {
		if step == nil {
			return nil, errs.New(errs.KindValidationFailed, "blueprint Task step is missing")
		}
		result[index].Kind = taskjournal.TaskStepOperation
		result[index].ID = step.StepId
		run := step.GetRunScript()
		if run == nil {
			continue
		}
		identity, exists := byExecution[run.ScriptExecutionId]
		if !exists || identity.id != run.ScriptId {
			return nil, errs.New(errs.KindValidationFailed, "blueprint RunScript step does not match its sealed hook")
		}
		result[index].Kind = taskjournal.TaskStepScript
		result[index].ScriptID = identity.id
		result[index].ScriptSlug = identity.slug
		delete(byExecution, run.ScriptExecutionId)
	}
	if len(byExecution) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "blueprint sealed hook is missing its RunScript step")
	}
	return result, nil
}
