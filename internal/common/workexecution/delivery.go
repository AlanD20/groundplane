package workexecution

// ValidateDelivery checks persisted assignment ownership. It grants no new
// effects: a running delivery is reconciliation, while an assigned delivery
// still needs Start. Expired attempts remain readable to account for effects.
func ValidateDelivery(item Item, attempt Attempt) error {
	if err := matchActive(item, attempt); err != nil {
		return err
	}
	if item.Phase != Assigned && item.Phase != Running ||
		attempt.Deadline.After(item.Deadline) || attempt.AssignedAt.Before(item.CreatedAt) ||
		item.Phase == Assigned && attempt.StartedAt != nil || item.Phase == Running && attempt.StartedAt == nil {
		return conflict("delivery does not match the saved execution phase or deadline")
	}
	return nil
}

// SameAssignment compares immutable attempt authority. Progress and terminal
// receipts may differ during replay; neither can change the originally assigned
// executor, input, direction, generation or deadline.
func SameAssignment(left, right Attempt) bool {
	return left.ID == right.ID && left.TaskID == right.TaskID && left.WorkItemID == right.WorkItemID &&
		left.Number == right.Number && left.Executor == right.Executor && left.Input == right.Input &&
		left.Direction == right.Direction && left.AssignedAt.Equal(right.AssignedAt) && left.Deadline.Equal(right.Deadline)
}
