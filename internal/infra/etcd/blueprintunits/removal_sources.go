package blueprintunits

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func validateServiceRemovalSources(graph CapturedGraph) error {
	sources := make(map[string]ServiceRemovalSource, len(graph.ServiceRemovals))
	previous := ""
	for _, source := range graph.ServiceRemovals {
		if ids.Validate(ids.KindWorkItem, source.WorkItemID) != nil ||
			ids.Validate(ids.KindService, source.ServiceID) != nil ||
			source.RuntimeRevision < 0 ||
			source.ClaimRevision < 0 ||
			source.WorkItemID <= previous ||
			(source.Predecessor == nil) != (source.ClaimRevision == 0) {
			return invalidRecord()
		}
		if source.RuntimeRevision == 0 && source.RuntimeSHA256 != "" ||
			source.RuntimeRevision > 0 && !recordcodec.ValidSHA256(source.RuntimeSHA256) {
			return invalidRecord()
		}
		if owner := source.Predecessor; owner != nil && (ids.Validate(ids.KindTask, owner.TaskID) != nil ||
			ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil || owner.TaskID == graph.TaskID || owner.WorkItemID == source.WorkItemID) {
			return invalidRecord()
		}
		previous = source.WorkItemID
		sources[source.WorkItemID] = source
	}
	for index, item := range graph.Items {
		source, found := sources[item.ID]
		if item.Operation != workexecution.ServiceRemoval {
			if found {
				return invalidRecord()
			}
			continue
		}
		unit := graph.Units[index]
		if !found || source.ServiceID != item.TargetID || unit.Target.Kind != ids.KindService || !unit.Removal ||
			source.Predecessor == nil &&
				(source.RuntimeRevision == 0) != (item.Placement.Executor == workexecution.Controller) {
			return invalidRecord()
		}
		delete(sources, item.ID)
	}
	if len(sources) != 0 {
		return invalidRecord()
	}
	return nil
}
