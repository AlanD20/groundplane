package backingprovision

import "github.com/AlanD20/groundplane/proto/agentpb"

func ClearPrivateDelivery(message *agentpb.ControllerMessage) {
	if message == nil {
		return
	}
	if delivery := message.GetBackingProvisionFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	if delivery := message.GetBackingProvisionHookDelivery(); delivery != nil && delivery.Hook != nil {
		for _, value := range delivery.Hook.Inputs {
			if value != nil {
				clear(value.Value)
				value.Value = nil
			}
		}
	}
}
