package runnerchecks

func RecordKey(taskID string) string   { return "/v1/runtime/runner-checks/" + taskID }
func ActiveKey(runnerID string) string { return "/v1/indexes/runner-checks/active/" + runnerID }
func LatestKey(runnerID string) string { return "/v1/indexes/runner-checks/latest/" + runnerID }
func OperationKey(operationID string) string {
	return "/v1/indexes/runner-checks/by-operation/" + operationID
}
