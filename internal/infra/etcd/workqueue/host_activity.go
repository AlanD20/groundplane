package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type HostActivityPage struct {
	Work         []View
	More         bool
	LastID       string
	ReadRevision int64
}

// HostActivity includes running work and unresolved effects whose worker slot
// has already been released. Diagnostic retention cannot erase these entries.
func (repository *Repository) HostActivity(
	ctx context.Context,
	hostID, after string,
	limit int64,
	revision int64,
) (HostActivityPage, error) {
	if ids.Validate(ids.KindHost, hostID) != nil || after != "" && ids.Validate(ids.KindWorkItem, after) != nil ||
		limit < 1 ||
		limit > 64 ||
		revision < 0 {
		return HostActivityPage{}, invalid("host activity read is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: HostEffectsPrefix(hostID), Limit: limit, Revision: revision}
	if after != "" {
		request.StartExclusive = HostEffectKey(hostID, after)
	}
	read, err := repository.store.Range(ctx, request)
	if err != nil {
		return HostActivityPage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		int64(len(read.Values)) > limit ||
		read.More && len(read.Values) == 0 {
		return HostActivityPage{}, corrupt()
	}
	defer keyvalue.ClearRangeValues(read.Values)
	result := HostActivityPage{More: read.More, ReadRevision: read.ReadRevision}
	for _, value := range read.Values {
		id := strings.TrimPrefix(value.Key, request.Prefix)
		if value.Key != HostEffectKey(hostID, id) || ids.Validate(ids.KindWorkItem, id) != nil || id <= after ||
			result.LastID != "" && id <= result.LastID {
			return HostActivityPage{}, corrupt()
		}
		owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_host_effect")
		if err != nil || owner.WorkItemID != id || validateOwner(owner) != nil {
			return HostActivityPage{}, corrupt()
		}
		view, err := repository.ReadWork(ctx, owner.TaskID, id, read.ReadRevision)
		if err != nil {
			return HostActivityPage{}, err
		}
		item := view.Item.Item
		if item.Placement.Executor != workexecution.Agent || item.Placement.HostID != hostID ||
			item.Phase == workexecution.Settled ||
			item.AttemptNumber == 0 {
			return HostActivityPage{}, corrupt()
		}
		result.Work = append(result.Work, view)
		result.LastID = id
	}
	return result, nil
}

// HostIdle requires absence of both reservations and retained effect ownership.
// All reads use the caller's maintenance-binding revision; no process lifetime
// or timeout is interpreted as proof that effects have stopped.
func (repository *Repository) HostIdle(ctx context.Context, hostID string, revision int64) (bool, error) {
	if ids.Validate(ids.KindHost, hostID) != nil || revision <= 0 {
		return false, invalid("host idle read requires a fixed binding revision")
	}
	for _, prefix := range []string{HostEffectsPrefix(hostID), SlotsPrefix(workexecution.Identity{Executor: workexecution.Agent, HostID: hostID})} {
		read, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return false, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) > 1 {
			return false, corrupt()
		}
		busy := len(read.Values) > 0
		keyvalue.ClearRangeValues(read.Values)
		if busy {
			return false, nil
		}
	}
	return true, nil
}
