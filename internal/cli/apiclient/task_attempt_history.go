package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskAttemptHistoryFromGenerated(value *generated.TaskAttemptHistory) (*apiTypes.TaskAttemptHistory, error) {
	if value == nil {
		return nil, nil
	}
	if value.ThroughSequence <= 0 || value.DroppedSummaries < 0 ||
		value.DroppedSummaries > value.ThroughSequence || value.Summaries == nil || len(*value.Summaries) == 0 {
		return nil, errs.New(errs.KindInternal, "apiclient: Task attempt history has an invalid archive boundary")
	}
	result := &apiTypes.TaskAttemptHistory{
		ThroughSequence: uint64(value.ThroughSequence), DroppedSummaries: uint64(value.DroppedSummaries),
		Summaries: make([]apiTypes.TaskAttemptSummary, len(*value.Summaries)),
	}
	for index, summary := range *value.Summaries {
		if summary.Attempt <= 0 || summary.Ordinal <= 0 || summary.FirstSequence <= 0 ||
			summary.LastSequence < summary.FirstSequence {
			return nil, errs.New(errs.KindInternal, "apiclient: Task attempt summary has an invalid execution identity")
		}
		result.Summaries[index] = apiTypes.TaskAttemptSummary{
			StepID: valueOrEmpty(summary.StepId), Attempt: uint32(summary.Attempt), Ordinal: uint64(summary.Ordinal),
			Action: summary.Action, Target: valueOrEmpty(summary.Target), State: apiTypes.TaskStatus(summary.State),
			FirstSequence: uint64(summary.FirstSequence), LastSequence: uint64(summary.LastSequence),
			StartedReceivedAt: summary.StartedReceivedAt, FinishedReceivedAt: summary.FinishedReceivedAt,
			LastReceivedAt: summary.LastReceivedAt, Failure: taskFailureFromGenerated(summary.Failure),
		}
		if operation := summary.ControllerOperation; operation != nil {
			result.Summaries[index].ControllerOperation = &apiTypes.TaskControllerOperation{
				Name: operation.Name, Stage: operation.Stage,
			}
		}
	}
	return result, nil
}
