package hierarchydeletion

func hierarchyDeletionOperationCollectionPrefix(collection, operationID string) (string, error) {
	operation, err := hierarchyDeletionDynamicSegment(operationID)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + collection + "/" + operation + "/", nil
}

func HierarchyDeletionReceiptPrefix(operationID string) (string, error) {
	return hierarchyDeletionOperationCollectionPrefix("deletion-receipts", operationID)
}

func HierarchyDeletionProgressPrefix(operationID string) (string, error) {
	return hierarchyDeletionOperationCollectionPrefix("deletion-progress", operationID)
}

func HierarchyDeletionSuccessorPrefix(operationID string) (string, error) {
	return hierarchyDeletionOperationCollectionPrefix("deletion-successors", operationID)
}

func hierarchyDeletionChildAttemptKey(
	collection string,
	parentOperationID string,
	childOperationID string,
	attemptID string,
	includeAttempts bool,
) (string, error) {
	parent, err := hierarchyDeletionDynamicSegment(parentOperationID)
	if err != nil {
		return "", err
	}
	child, err := hierarchyDeletionDynamicSegment(childOperationID)
	if err != nil {
		return "", err
	}
	if !validHierarchyDeletionRawStableID(attemptID) {
		return "", CorruptHierarchyDeletion()
	}
	key := hierarchyDeletionRuntimeRoot + collection + "/" + parent + "/" + child + "/"
	if includeAttempts {
		key += "attempts/"
	}
	return key + attemptID, nil
}

func HierarchyDeletionSuccessorKey(parentOperationID, childOperationID, attemptID string) (string, error) {
	return hierarchyDeletionChildAttemptKey(
		"deletion-successors",
		parentOperationID,
		childOperationID,
		attemptID,
		false,
	)
}

func HierarchyDeletionReceiptKey(parentOperationID, childOperationID, attemptID string) (string, error) {
	return hierarchyDeletionChildAttemptKey("deletion-receipts", parentOperationID, childOperationID, attemptID, true)
}

func HierarchyDeletionProgressKey(parentOperationID, childOperationID, attemptID string) (string, error) {
	return hierarchyDeletionChildAttemptKey("deletion-progress", parentOperationID, childOperationID, attemptID, true)
}

func hierarchyDeletionChildCurrentKey(collection, parentOperationID, childOperationID string) (string, error) {
	parent, err := hierarchyDeletionDynamicSegment(parentOperationID)
	if err != nil {
		return "", err
	}
	child, err := hierarchyDeletionDynamicSegment(childOperationID)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + collection + "/" + parent + "/" + child + "/current", nil
}

func HierarchyDeletionReceiptCurrentKey(parentOperationID, childOperationID string) (string, error) {
	return hierarchyDeletionChildCurrentKey("deletion-receipts", parentOperationID, childOperationID)
}
