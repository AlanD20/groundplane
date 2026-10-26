package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Repository) LoadFailure(
	ctx context.Context,
	view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput,
) ([]byte, keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	input, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if attempt.Direction != workexecution.Forward || attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.TaskID != input.TaskID || attempt.WorkItemID != input.WorkItemID || !matchesDelivery(input, delivery) {
		return nil, keyvalue.Condition{}, conflict("Service removal failure has no matching retained result")
	}
	key := servicelifecyclework.FailureKey(attempt.ID)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, keyvalue.Condition{}, conflict("Service removal failure evidence is unavailable")
	}
	if _, err := lifecycleexecution.DecodeFailure(attempt, delivery, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, keyvalue.Condition{}, err
	}
	return append(
			[]byte(nil),
			read.Values[0].Value...), keyvalue.Condition{
			Key:         key,
			ModRevision: read.Values[0].ModRevision,
		}, nil
}
