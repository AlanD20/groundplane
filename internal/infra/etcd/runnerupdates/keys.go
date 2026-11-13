package runnerupdates

func RecordKey(taskID string) string   { return "/v1/runtime/runner-updates/" + taskID }
func ActiveKey(runnerID string) string { return "/v1/indexes/runner-runtime/active/" + runnerID }
func LatestKey(runnerID string) string { return "/v1/indexes/runner-updates/latest/" + runnerID }
func OperationKey(operationID string) string {
	return "/v1/indexes/runner-updates/by-operation/" + operationID
}
