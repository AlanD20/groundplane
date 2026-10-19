package hierarchydeletion

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func EnvironmentRecoveryKey(workID string) string { return "/v1/hierarchy-removal-recovery/" + workID }

type EnvironmentRecoverySnapshot struct {
	Source   hierarchyexecution.EnvironmentRecovery
	Revision int64
}

func LoadEnvironmentRecovery(
	ctx context.Context,
	reader EnvironmentReportReader,
	view workqueue.View,
) (EnvironmentRecoverySnapshot, error) {
	item := view.Item.Item
	if item.Operation != workexecution.Hierarchy || item.Input == nil || view.ReadRevision <= 0 {
		return EnvironmentRecoverySnapshot{}, CorruptHierarchyDeletion()
	}
	key := EnvironmentRecoveryKey(item.ID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return EnvironmentRecoverySnapshot{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return EnvironmentRecoverySnapshot{}, CorruptHierarchyDeletion()
	}
	source, err := hierarchyexecution.DecodeEnvironmentRecovery(read.Values[0].Value)
	if err != nil {
		return EnvironmentRecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input || source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Forward.Number > item.AttemptNumber ||
		source.Forward.Number == item.AttemptNumber && item.Phase != workexecution.Ready &&
			item.Phase != workexecution.Waiting {
		return EnvironmentRecoverySnapshot{}, CorruptHierarchyDeletion()
	}
	return EnvironmentRecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}
