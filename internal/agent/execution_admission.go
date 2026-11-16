package agent

func (workers *executionWorkers) capacity() int {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed {
		return 0
	}
	return workers.limit - len(workers.active)
}

// canAdmit is called under mu before an execution acquires host ownership.
func (workers *executionWorkers) canAdmit(taskID string) bool {
	if len(workers.active) >= workers.limit {
		return false
	}
	counts := make(map[string]int)
	for _, active := range workers.active {
		counts[active.attempt.TaskID]++
	}
	return counts[taskID] < int(workers.limits.OperationsPerTask) &&
		(counts[taskID] > 0 || len(counts) < int(workers.limits.Tasks))
}
