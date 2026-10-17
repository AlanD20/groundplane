package workexecution

// ValidateGraph checks the complete captured plan before the publication barrier
// opens. A dependency cannot point to a missing item or another Task's execution.
func ValidateGraph(task Task, items []Item) error {
	if err := task.Validate(); err != nil {
		return err
	}
	if uint64(len(items)) != uint64(task.ExpectedItems) {
		return invalid("task work graph is incomplete")
	}
	byID := make(map[string]Item, len(items))
	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
		if item.TaskID != task.ID || item.CreatedAt.Before(task.CreatedAt) || item.Deadline.After(task.Deadline) {
			return invalid("work graph item does not belong to the captured task")
		}
		if _, exists := byID[item.ID]; exists {
			return invalid("work graph contains a duplicate item")
		}
		byID[item.ID] = item
	}
	indegree := make(map[string]int, len(items))
	dependents := make(map[string][]string, len(items))
	ready := make([]string, 0, len(items))
	for _, item := range items {
		indegree[item.ID] = len(item.Dependencies)
		if len(item.Dependencies) == 0 {
			ready = append(ready, item.ID)
		}
		for _, dependency := range item.Dependencies {
			if _, exists := byID[dependency]; !exists {
				return invalid("work graph refers to an unpublished dependency")
			}
			dependents[dependency] = append(dependents[dependency], item.ID)
		}
	}
	for index := 0; index < len(ready); index++ {
		for _, dependent := range dependents[ready[index]] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if len(ready) != len(items) {
		return invalid("work graph contains a dependency cycle")
	}
	return nil
}
