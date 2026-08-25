package etcd

const environmentCoordinationPrefix = "/v1/runtime/environment-coordination/"

func environmentCoordinationKey(environmentID string) string {
	return environmentCoordinationPrefix + environmentID
}
