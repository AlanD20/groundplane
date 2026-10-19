package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryKey(workID string) string { return "/v1/component-work-recovery/" + workID }

type RecoverySnapshot struct {
	Source   componentexecution.Recovery
	Revision int64
}

func (repository *InputRepository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySnapshot, error) {
	input, _, _, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	item := view.Item.Item
	if item.Operation != workexecution.Component || item.Input == nil {
		return RecoverySnapshot{}, conflict("Component recovery requires captured work")
	}
	read, err := repository.read(ctx, []string{RecoveryKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoverySnapshot{}, conflict("Component recovery source is unavailable")
	}
	source, err := componentexecution.DecodeRecovery(read.Values[0].Value, input)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input ||
		source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Forward.Number > item.AttemptNumber {
		return RecoverySnapshot{}, conflict("Component recovery source belongs to different execution")
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}
