package agentchannel

import "github.com/AlanD20/groundplane/proto/agentpb"

// Each compiled operation owns authentication and typed decoding. The stream
// loop owns sending and clears transient file/hook bytes even when Send fails.
func (server *Server) nativeOperationReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	for _, route := range []func(*Session, uint64, *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error){
		server.environmentProvisionReport, server.entryReport, server.componentReport,
		server.networkReport, server.volumeReport, server.removalRecoveryReport,
		server.backupWorkReport, server.lifecycleReport, server.attachReport,
	} {
		if handled, reply, err := route(session, generation, message); handled {
			return true, reply, err
		}
	}
	return false, nil, nil
}

func clearNativeOperationReply(reply *agentpb.ControllerMessage) {
	if delivery := reply.GetEntryConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	if delivery := reply.GetComponentConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	if delivery := reply.GetServiceRemovalConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	clearLifecycleHookReply(reply)
}
