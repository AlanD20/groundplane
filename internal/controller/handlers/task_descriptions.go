package handlers

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type TaskDescriptionReader interface {
	DescribeTaskSteps(context.Context, etcd.TaskRecord) (map[string]apiTypes.TaskStep, error)
}

func (s *Server) describeTask(ctx context.Context, record etcd.TaskRecord, response *apiTypes.Task) {
	var descriptions map[string]apiTypes.TaskStep
	if record.Executor == taskjournal.TaskExecutorAgent && s.taskDescriptions != nil {
		var err error
		descriptions, err = s.taskDescriptions.DescribeTaskSteps(ctx, record)
		if err != nil && s.Logger != nil {
			s.Logger.DebugContext(ctx, "Task execution descriptions unavailable", "task_id", record.ID)
		}
	}
	for index := range response.Steps {
		step := &response.Steps[index]
		step.Action = "Execution details unavailable"
		if description, ok := descriptions[step.Name]; ok {
			step.Action = description.Action
			step.TimeoutSeconds = description.TimeoutSeconds
		} else if !strings.HasPrefix(step.Name, "step_") {
			step.Action = strings.ReplaceAll(step.Name, "_", " ")
		}
		if step.Kind == apiTypes.TaskStepScript {
			step.Action = "Run script " + step.ScriptSlug
		}
	}
}
