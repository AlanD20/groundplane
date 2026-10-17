package workexecution

import "time"

func RequestStop(task Task, outcome Outcome, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if outcome != Cancelled && outcome != Superseded || !validTime(at) || at.Before(task.UpdatedAt) {
		return Task{}, invalid("task stop request has an invalid outcome or time")
	}
	if task.FinishedAt != nil {
		return Task{}, conflict("task has already finished")
	}
	if err := CancellationFailure(task); err != nil {
		return Task{}, err
	}
	next := CloneTask(task)
	if next.Stop != nil {
		return next, nil
	}
	next.Stop = &StopRequest{Outcome: outcome, RequestedAt: at}
	next.UpdatedAt = at
	return next, nil
}
