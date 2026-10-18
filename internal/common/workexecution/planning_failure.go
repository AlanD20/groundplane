package workexecution

import "time"

// RejectPlanning closes an action whose membership was never established. It is
// not a shortcut for settling a captured graph: even one expected or published
// item requires the normal per-item accounting and cleanup path.
func RejectPlanning(task Task, outcome Outcome, evidence Input, at, retainUntil time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if err := evidence.Validate(); err != nil {
		return Task{}, err
	}
	if outcome != Failed && (task.Stop == nil || task.Stop.Outcome != outcome || outcome != Cancelled && outcome != Superseded) {
		return Task{}, conflict("planning rejection requires failure or the recorded stop outcome")
	}
	if task.PublicationComplete || task.ExpectedItems != 0 || task.Progress.Total() != 0 ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || task.Activation != nil ||
		!validTime(at) || at.Before(task.UpdatedAt) || !validTime(retainUntil) || !retainUntil.After(at) {
		return Task{}, conflict("planning failure requires an action with no captured execution work")
	}
	next := CloneTask(task)
	next.PublicationComplete = true
	next.Outcome = outcome
	if outcome == Failed && next.FirstFailure == nil {
		failure := evidence
		next.FirstFailure = &failure
	}
	next.UpdatedAt, next.FinishedAt, next.RetainUntil = at, &at, &retainUntil
	if err := next.Validate(); err != nil {
		return Task{}, err
	}
	return next, nil
}
