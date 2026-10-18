package etcd

import "github.com/AlanD20/groundplane/proto/agentpb"

func composeServiceReleaseID(service *agentpb.ComposeService) string {
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == "com.groundplane.release-id" {
			return label.GetValue()
		}
	}
	return ""
}
