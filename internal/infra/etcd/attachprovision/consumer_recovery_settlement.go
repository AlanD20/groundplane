package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// A recovery observation is retained as recovery evidence, never as a successful
// forward command. Only a completely connected consumer advances applied state.
func (repository *InputRepository) prepareConsumerRecoverySettlement(ctx context.Context, view workqueue.ResumeView,
	capture blueprintattachinputs.Generation, report attachexecution.ProvisionInspection, evidence workexecution.Input,
) (workqueue.Change, error) {
	if report.State != attachexecution.ProvisionConsumerInspected || report.Consumer == nil || !report.Consumer.Connected {
		return workqueue.Change{}, nil
	}
	consumer, err := capture.Consumer()
	if err != nil {
		return workqueue.Change{}, err
	}
	if consumer == nil || capture.ConsumerRuntime == nil || !report.HelperStopped {
		return workqueue.Change{}, checkpointConflict("Attach recovery has no captured consumer runtime")
	}
	attempt := view.Attempt.Attempt
	checkpoint := workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: 1,
		StageID:    ids.DeriveAt(ids.KindStep, attempt.AssignedAt, attempt.ID, "inspect-consumer-connection"),
		RecordedAt: report.ObservedAt, Evidence: evidence}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
	if err != nil {
		return workqueue.Change{}, err
	}
	source := serviceruntimerecord.Acknowledgement{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
		Executor: attempt.Executor, Input: attempt.Input, Checkpoint: checkpoint,
		RenderGeneration: capture.ConsumerRuntime.Source.RenderGeneration, AcknowledgedAt: report.ObservedAt}
	settlement, err := repository.prepareConsumerAcknowledgement(ctx, view, capture, consumer.Candidate, &source)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, settlement.Conditions...)
	change.Mutations = append(change.Mutations, settlement.Mutations...)
	return change, nil
}
