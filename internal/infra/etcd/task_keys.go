package etcd

const deletionTombstoneRootPrefix = "/v1/runtime/deletions/"

func deletionTombstoneKey(targetKind string, targetID string) string {
	return deletionTombstoneRootPrefix + targetKind + "/" + targetID
}
