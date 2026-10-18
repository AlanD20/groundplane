package workqueue

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const root = "/v1/execution/"

func AttemptIndexPrefix(workID string) string { return root + "work-attempts/" + workID + "/" }
func AttemptIndexKey(workID string, number uint64) string {
	return AttemptIndexPrefix(workID) + fmt.Sprintf("%020d", number)
}

func TaskKey(id string) string { return root + "tasks/" + id + "/record" }

func ItemKey(id string) string                 { return root + "work/" + id }
func AttemptKey(id string) string              { return root + "attempts/" + id }
func TaskItemsPrefix(taskID string) string     { return root + "tasks/" + taskID + "/work/" }
func TaskItemKey(taskID, itemID string) string { return TaskItemsPrefix(taskID) + itemID }

func ExecutorKey(identity workexecution.Identity) string {
	return root + "executors/" + placement(identity.Executor, identity.HostID)
}

func QueuePrefix(target workexecution.Placement) string {
	return root + "ready/" + placement(target.Executor, target.HostID) + "/"
}

func QueueKey(item workexecution.Item) string { return QueuePrefix(item.Placement) + item.ID }

// Slots are host-scoped, not generation-scoped. Replacing an Agent cannot make
// an old generation's unresolved reservation disappear from capacity accounting.
func SlotKey(identity workexecution.Identity, index uint16) string {
	return SlotsPrefix(identity) + fmt.Sprintf("%03d", index)
}

func SlotsPrefix(identity workexecution.Identity) string {
	return slotsPrefix(workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID})
}

func slotsPrefix(target workexecution.Placement) string {
	return root + "slots/" + placement(target.Executor, target.HostID) + "/"
}

func HostClaimsPrefix(hostID string) string { return root + "claims/" + hostID + "/" }

// Host effects are non-exclusive membership, not a host-wide execution lock.
// They outlive a worker slot while a resource still needs reconciliation.
func HostEffectsPrefix(hostID string) string     { return root + "host-effects/" + hostID + "/" }
func HostEffectKey(hostID, workID string) string { return HostEffectsPrefix(hostID) + workID }

// HostForwardFenceKey pauses new forward work for Agent maintenance without
// blocking the recovery needed to drain that same host.
func HostForwardFenceKey(hostID string) string { return root + "host-forward-fences/" + hostID }

func ClaimKey(claim workexecution.Claim) string {
	scope := claim.HostID
	if scope == "" {
		scope = "control"
	}
	return root + "claims/" + scope + "/" + claim.ScopeID
}

func placement(executor workexecution.Executor, hostID string) string {
	if hostID == "" {
		hostID = "control"
	}
	return string(executor) + "/" + hostID
}
