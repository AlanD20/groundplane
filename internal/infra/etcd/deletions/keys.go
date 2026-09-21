package deletions

const deletionTombstoneRootPrefix = "/v1/runtime/deletions/"

func TombstoneKey(targetKind string, targetID string) string {
	return deletionTombstoneRootPrefix + targetKind + "/" + targetID
}
