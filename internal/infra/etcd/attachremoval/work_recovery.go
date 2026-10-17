package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RecoverySnapshot struct {
	Source   attachexecution.RemovalRecovery
	Revision int64
}

func LoadRecovery(ctx context.Context, reader Reader, view workqueue.View) (RecoverySnapshot, error) {
	item := view.Item.Item
	if !removalOperation(item.Operation) || item.Input == nil || view.ReadRevision <= 0 {
		return RecoverySnapshot{}, recordcodec.CorruptRecord()
	}
	key := WorkRecoveryKey(item.ID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return RecoverySnapshot{}, recordcodec.CorruptRecord()
	}
	source, err := attachexecution.DecodeRemovalRecovery(read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input || source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Forward.Number > item.AttemptNumber {
		return RecoverySnapshot{}, recordcodec.CorruptRecord()
	}
	if source.Forward.Number == item.AttemptNumber {
		if item.ActiveAttemptID != "" || item.Result == nil || *item.Result != source.Evidence ||
			(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
			(item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting && item.Phase != workexecution.Attention) {
			return RecoverySnapshot{}, recordcodec.CorruptRecord()
		}
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}
