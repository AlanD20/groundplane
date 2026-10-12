package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// IsAuthoredBlueprintPreparationTask identifies the same closed child shapes
// accepted by publication and terminal receipt verification. It does not grant
// execution authority; dispatch still verifies the captured plan and assignment.
func IsAuthoredBlueprintPreparationTask(task TaskRecord) bool {
	componentID := task.Params[taskjournal.TaskBlueprintComponentUnitParam]
	if ids.Validate(ids.KindComponent, componentID) == nil {
		return blueprintComponentChildTaskMatches(task, blueprintunits.Unit{
			Target: blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: componentID},
		})
	}
	networkID := task.Params[taskjournal.TaskBlueprintNetworkUnitParam]
	volumeID := task.Params[taskjournal.TaskBlueprintVolumeUnitParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	if ids.Validate(ids.KindNetwork, networkID) == nil {
		return blueprintNetworkChildTaskMatches(task, blueprintunits.Unit{
			Target: blueprintunits.ResourceKey{Kind: ids.KindNetwork, ID: networkID},
		})
	}
	if ids.Validate(ids.KindVolume, volumeID) == nil {
		return blueprintVolumeChildTaskMatches(task, blueprintunits.Unit{
			Target: blueprintunits.ResourceKey{Kind: ids.KindVolume, ID: volumeID},
		})
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return false
	}
	return blueprintAttachChildTaskMatches(task, blueprintunits.Unit{
		Target: blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID},
	})
}
