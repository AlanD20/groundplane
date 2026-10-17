package attachremoval

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// CascadeStep describes a position in one frozen Backing Zone removal. The
// captured membership remains unchanged; RemovedBefore identifies earlier
// bindings whose removal must be proved by the operation cursor before this
// work can use its projected consumer transition.
type CascadeStep struct {
	ZoneID        string   `json:"zone_id"`
	Ordinal       uint32   `json:"ordinal"`
	Count         uint32   `json:"count"`
	RemovedBefore []string `json:"removed_before,omitempty"`
}

func (step *CascadeStep) Validate(snapshot DetachSnapshot) error {
	if step == nil {
		return nil
	}
	if ids.Validate(ids.KindNetwork, step.ZoneID) != nil || step.ZoneID != snapshot.Source.Attach.BackingNetworkID ||
		step.Count == 0 || uint64(step.Count) >= uint64(workexecution.MaximumTaskItems) || step.Ordinal >= step.Count ||
		uint64(len(step.RemovedBefore)) > uint64(step.Ordinal) || !slices.IsSorted(step.RemovedBefore) {
		return recordcodec.CorruptRecord()
	}
	target := snapshot.Source.Attach
	previous := ""
	for _, id := range step.RemovedBefore {
		if ids.Validate(ids.KindAttach, id) != nil || id == previous || id == target.ID {
			return recordcodec.CorruptRecord()
		}
		index, exists := slices.BinarySearchFunc(snapshot.Members, id, func(member Membership, id string) int {
			if member.AttachID < id {
				return -1
			}
			if member.AttachID > id {
				return 1
			}
			return 0
		})
		if !exists || snapshot.Members[index].Detached || snapshot.Members[index].ServiceID != target.ServiceID ||
			snapshot.Members[index].NetworkID != target.BackingNetworkID {
			return recordcodec.CorruptRecord()
		}
		previous = id
	}
	return nil
}

// KeepsConsumerNetworkAfter is a projection, not execution proof. A caller
// using a nonempty removal set must join the matching cascade cursor guard to
// every dispatch/Start and advance it only with proven resource removal.
func (snapshot DetachSnapshot) KeepsConsumerNetworkAfter(removed []string) bool {
	target := snapshot.Source.Attach
	for _, member := range snapshot.Members {
		if member.AttachID != target.ID && !member.Detached && member.ServiceID == target.ServiceID &&
			member.NetworkID == target.BackingNetworkID && !slices.Contains(removed, member.AttachID) {
			return true
		}
	}
	return false
}

func (input DetachInput) keepsConsumerNetwork() bool {
	var removed []string
	if input.Cascade != nil {
		removed = input.Cascade.RemovedBefore
	}
	return input.Snapshot.KeepsConsumerNetworkAfter(removed)
}
