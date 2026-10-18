package releaseexecution

import "github.com/AlanD20/groundplane/proto/agentpb"

// ClearHookDelivery erases transport-owned plaintext after send or consumption.
func ClearHookDelivery(delivery *agentpb.ReleaseHookDelivery) {
	if delivery == nil {
		return
	}
	if delivery.Body != nil {
		clear(delivery.Body.Body)
		delivery.Body.Body = nil
	}
	for _, entry := range delivery.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
