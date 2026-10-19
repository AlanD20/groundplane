package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
)

// Release only the initiating marker; all other fields come from the current
// CAS-protected resource. No historical resource definition is restored.
func releaseHierarchyPlanningRoot(
	kind hierarchydeletion.HierarchyDeletionTargetKind,
	id, taskID string,
	raw []byte,
) ([]byte, error) {
	switch kind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		value, err := hierarchy.DecodeTenant(raw)
		if err != nil || value.ID != id || value.DeletionTaskID != taskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		value.DeletionTaskID = ""
		return hierarchy.EncodeTenant(value)
	case hierarchydeletion.HierarchyDeletionTargetProject, hierarchydeletion.HierarchyDeletionTargetBacking:
		value, err := hierarchy.DecodeProject(raw)
		if err != nil || value.ID != id || value.DeletionTaskID != taskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		value.DeletionTaskID = ""
		return hierarchy.EncodeProject(value)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		value, err := hierarchy.DecodeEnvironment(raw)
		if err != nil || value.ID != id || value.DeletionTaskID != taskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		value.DeletionTaskID = ""
		return hierarchy.EncodeEnvironment(value)
	default:
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
}
