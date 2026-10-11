package handlers

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func taskAttemptHistoryResponse(
	task etcd.TaskRecord,
	history *taskjournal.AttemptHistory,
) (*apiTypes.TaskAttemptHistory, error) {
	if history == nil {
		return nil, nil
	}
	response := &apiTypes.TaskAttemptHistory{
		ThroughSequence:  history.ThroughSequence,
		DroppedSummaries: history.DroppedSummaries,
		Summaries:        make([]apiTypes.TaskAttemptSummary, len(history.Summaries)),
	}
	for index, summary := range history.Summaries {
		state, err := taskEventAPIStatus(summary.State)
		if err != nil {
			return nil, err
		}
		value := apiTypes.TaskAttemptSummary{StepID: summary.Identity.StepID, Attempt: summary.Identity.Attempt,
			Ordinal: summary.Identity.Ordinal, State: state, FirstSequence: summary.FirstSequence, LastSequence: summary.LastSequence,
			StartedReceivedAt: summary.StartedAt, FinishedReceivedAt: summary.FinishedAt, LastReceivedAt: summary.LastReceivedAt,
			Failure: taskStepFailure(
				string(state),
				summary.Identity.StepID,
				summary.Failure,
			), Action: "Action not recorded"}
		if summary.ControllerOperation != nil {
			value.Action = summary.ControllerOperation.Name()
			value.ControllerOperation = &apiTypes.TaskControllerOperation{
				Stage: summary.ControllerOperation.Stage,
				Name:  value.Action,
			}
		} else {
			for _, step := range task.Steps {
				if step.ID == summary.Identity.StepID {
					if step.Action != "" {
						value.Action = step.Action
					}
					value.Target = step.Target
					break
				}
			}
		}
		response.Summaries[index] = value
	}
	return response, nil
}
