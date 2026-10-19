package taskjournal

// TaskMaterializationWriterKey is still compared by metadata-only Blueprint
// omission admission. Native file-effect grants belong to runtimeconfiguration.
func TaskMaterializationWriterKey(environmentID string) string {
	return "/v1/runtime/task-materialization-writers/" + environmentID
}
