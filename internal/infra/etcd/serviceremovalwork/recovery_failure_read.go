package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Repository) LoadRecoveryFailure(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput,
) ([]byte, removalexecution.Failure, error) {
	attempt := view.Attempt.Attempt
	if attempt.Direction != workexecution.Recovery || attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.Outcome != workexecution.Failed || attempt.TaskID != view.Work.Task.Task.ID ||
		attempt.WorkItemID != view.Work.Item.Item.ID {
		return nil, removalexecution.Failure{}, conflict("Service removal recovery has no stored failed outcome")
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, delivery)
	if err != nil {
		return nil, removalexecution.Failure{}, err
	}
	read, err := repository.read(ctx, []string{RecoveryFailureKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, removalexecution.Failure{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, removalexecution.Failure{}, conflict("Service removal recovery failure evidence is missing")
	}
	failure, err := removalexecution.DecodeFailure(attempt, delivery, saved.Capture, read.Values[0].Value, *attempt.Result)
	if err != nil {
		return nil, removalexecution.Failure{}, err
	}
	return append([]byte(nil), read.Values[0].Value...), failure, nil
}
