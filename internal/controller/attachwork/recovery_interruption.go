package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Reports) interruptInspection(
	ctx context.Context,
	view workqueue.ResumeView,
	guards []keyvalue.Condition,
) error {
	input, sourceGuards, err := owner.inputs.Recovery(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	report := attachexecution.ProvisionInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
		Failure: input.Source.Evidence, CommandOrdinal: uint32((input.Source.Failure.Checkpoint.Sequence - 1) / 2),
		State: attachexecution.InspectionInterrupted, ObservedAt: at}
	raw, evidence, err := attachexecution.EncodeProvisionInspection(input, report)
	if err != nil {
		return err
	}
	proof, result, err := owner.inputs.repository.PrepareRecoveryResult(ctx, view, raw, evidence, at, true)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(proof.Mutations)
	completion, err := owner.queue.ReadInterruptedAttachAt(ctx, view)
	if err != nil {
		return err
	}
	completion.Result, completion.At = result, at
	terminal, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(terminal.Mutations)
	committed, err := owner.queue.Commit(
		ctx,
		workqueue.Change{Conditions: append(guards, sourceGuards...)},
		proof,
		terminal,
	)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach inspection ownership changed before interruption was recorded")
	}
	finished := view.Attempt.Attempt
	finished.FinishedAt, finished.Result, finished.Outcome = &at, &result.Evidence, result.Outcome
	owner.reportInspection(ctx, finished, report)
	return nil
}
