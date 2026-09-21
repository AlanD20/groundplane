package handlers

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Trimmed history cannot reset a completed step to pending. Retained events
// supersede the compact checkpoint; Controller-native steps follow their Task.
func taskProgressStatuses(
	record etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
	status apiTypes.TaskStatus,
) (map[string]apiTypes.TaskStatus, error) {
	values := make(map[string]apiTypes.TaskStatus, len(record.Steps))
	initial := apiTypes.TaskPending
	if record.Executor == taskjournal.TaskExecutorController {
		initial = status
	}
	for _, step := range record.Steps {
		values[step.ID] = initial
	}
	set := func(id string, state taskjournal.TaskEventState) error {
		mapped, err := taskEventAPIStatus(state)
		if err != nil {
			return err
		}
		if _, exists := values[id]; !exists {
			return errs.New(errs.KindInternal, "Task event references an unknown step")
		}
		values[id] = mapped
		return nil
	}
	for _, checkpoint := range record.EventCheckpoints {
		if err := set(checkpoint.Identity.StepID, checkpoint.State); err != nil {
			return nil, err
		}
	}
	for _, event := range snapshot.Events {
		if err := set(event.Identity.StepID, event.State); err != nil {
			return nil, err
		}
	}
	return values, nil
}
