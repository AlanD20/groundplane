package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Replacing an executor invalidates its execution lease, not its host effects.
// No restoration is inferred without the old executor's durable proof.
func (repository *InputRepository) PrepareRecoveryInterruption(
	ctx context.Context,
	view workqueue.ResumeView,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	attempt := view.Attempt.Attempt
	report := entryexecution.RecoveryReport{AttemptID: attempt.ID, ForwardID: captured.Source.Forward.ID,
		Failure: captured.Source.Evidence, EnvironmentID: captured.Input.EnvironmentId, Input: attempt.Input, ObservedAt: at}
	raw, _, err := entryexecution.EncodeRecoveryReport(
		attempt,
		captured.Source,
		captured.Input,
		captured.Boundary,
		report,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	return repository.prepareRecoveryResult(ctx, view, raw, at, true)
}
