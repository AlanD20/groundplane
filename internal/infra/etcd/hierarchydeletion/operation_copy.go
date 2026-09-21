package hierarchydeletion

import ()

func CloneHierarchyDeletionOperation(operation HierarchyDeletionOperation) HierarchyDeletionOperation {
	cloned := operation
	cloned.Tombstone.PlanCount = cloneInt64Pointer(operation.Tombstone.PlanCount)
	cloned.Tombstone.PlanDigest = cloneStringPointer(operation.Tombstone.PlanDigest)
	cloned.Tombstone.Terminal = cloneHierarchyDeletionTerminal(operation.Tombstone.Terminal)
	cloned.Fence.PlanDigest = cloneStringPointer(operation.Fence.PlanDigest)
	cloned.Fence.ActiveActionOrdinal = cloneInt64Pointer(operation.Fence.ActiveActionOrdinal)
	cloned.Intent = operation.Intent
	return cloned
}

func ClearHierarchyDeletionOperation(operation HierarchyDeletionOperation) {
	operation.Tombstone.PlanDigest = nil
	operation.Fence.PlanDigest = nil
	operation.Intent.RootSlug = ""
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneHierarchyDeletionTerminal(value *HierarchyDeletionTerminal) *HierarchyDeletionTerminal {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
