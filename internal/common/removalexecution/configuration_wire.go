package removalexecution

import "github.com/AlanD20/groundplane/proto/agentpb"

func ClearConfigurationDelivery(delivery *agentpb.ServiceRemovalConfigurationFileDelivery) {
	if delivery == nil {
		return
	}
	clear(delivery.Content)
	delivery.Content = nil
}
