package resolverwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Latest intent is not necessarily the execution holding this resource: a newer
// action can queue while an older action still owns unproven effects.
func readActiveState(ctx context.Context, store hostbindings.Reader, hostID, componentID string, raw []byte, revision int64) (workexecution.Item, *CapturedState, []keyvalue.Condition, error) {
	owner, err := workqueue.DecodeClaimOwner(raw)
	if err != nil {
		return workexecution.Item{}, nil, nil, err
	}
	key := workqueue.ItemKey(owner.WorkItemID)
	read, err := readStateKeys(ctx, store, []string{key}, revision)
	if err != nil {
		return workexecution.Item{}, nil, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workexecution.Item{}, nil, nil, invalid("DNS resource claim lost its unresolved work")
	}
	item, err := workqueue.DecodeItem(read.Values[0].Value)
	if err != nil {
		return workexecution.Item{}, nil, nil, err
	}
	claim := workexecution.Claim{HostID: hostID, ScopeID: componentID}
	if item.ID != owner.WorkItemID || item.TaskID != owner.TaskID || item.TargetID != componentID ||
		item.Phase == workexecution.Settled || !slices.Contains(item.Claims, claim) ||
		item.Operation != workexecution.Resolver && item.Operation != workexecution.DNSCheck {
		return workexecution.Item{}, nil, nil, invalid("DNS resource claim does not match its unresolved operation")
	}
	guards := []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	if item.Operation == workexecution.DNSCheck {
		return item, nil, guards, nil
	}
	if item.Placement != (workexecution.Placement{Executor: workexecution.Agent, HostID: hostID}) {
		return workexecution.Item{}, nil, nil, invalid("DNS active execution targets another host")
	}
	captured, authority, err := readCapturedState(ctx, store, item, revision)
	if err != nil {
		return workexecution.Item{}, nil, nil, err
	}
	return item, &captured, append(guards, authority...), nil
}
