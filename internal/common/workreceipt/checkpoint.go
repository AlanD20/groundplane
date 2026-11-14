package workreceipt

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// DescribeCheckpoint identifies reports that may be refused within their own
// live exchange. They never enter the terminal-result journal or startup replay.
func DescribeCheckpoint(message *agentpb.AgentMessage) (*agentpb.WorkAttemptIdentity, string, bool, error) {
	if message == nil {
		return nil, "", false, nil
	}
	var identity *agentpb.WorkAttemptIdentity
	kind := ""
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_VolumeWorkCheckpointRequest:
		identity, kind = payload.VolumeWorkCheckpointRequest.GetAttempt(), "volume_checkpoint"
	case *agentpb.AgentMessage_NetworkWorkCheckpointRequest:
		identity, kind = payload.NetworkWorkCheckpointRequest.GetAttempt(), "network_checkpoint"
	case *agentpb.AgentMessage_ResolverWorkCheckpointRequest:
		identity, kind = payload.ResolverWorkCheckpointRequest.GetAttempt(), "resolver_checkpoint"
	case *agentpb.AgentMessage_BackingProvisionCheckpointRequest:
		identity, kind = payload.BackingProvisionCheckpointRequest.GetAttempt(), "backing_checkpoint"
	case *agentpb.AgentMessage_EntryConfigurationWorkCheckpointRequest:
		identity, kind = payload.EntryConfigurationWorkCheckpointRequest.GetAttempt(), "entry_checkpoint"
	case *agentpb.AgentMessage_ComponentWorkCheckpointRequest:
		identity, kind = payload.ComponentWorkCheckpointRequest.GetAttempt(), "component_checkpoint"
	case *agentpb.AgentMessage_ServiceLifecycleCheckpointRequest:
		identity, kind = payload.ServiceLifecycleCheckpointRequest.GetAttempt(), "lifecycle_checkpoint"
	case *agentpb.AgentMessage_ServiceRemovalRecoveryCheckpointRequest:
		identity, kind = payload.ServiceRemovalRecoveryCheckpointRequest.GetAttempt(), "removal_recovery_checkpoint"
	case *agentpb.AgentMessage_AttachRemovalCheckpointRequest:
		identity, kind = payload.AttachRemovalCheckpointRequest.GetAttempt(), "attach_removal_checkpoint"
	case *agentpb.AgentMessage_BackupWorkCheckpointRequest:
		identity, kind = payload.BackupWorkCheckpointRequest.GetAttempt(), "backup_checkpoint"
	default:
		return nil, "", false, nil
	}
	if identity == nil || executionplan.RejectUnknown(message) != nil || proto.Size(message) > MaximumBytes {
		return nil, "", true, invalid()
	}
	executor := workexecution.Identity{Executor: workexecution.Agent, HostID: identity.HostId,
		AgentID: identity.AgentId, Generation: identity.ExecutorGeneration}
	if _, err := workexecution.DecodeAgentAttempt(identity, executor); err != nil {
		return nil, "", true, err
	}
	return identity, kind, true, nil
}

func describeRejection(message *agentpb.AgentMessage) (*agentpb.WorkAttemptIdentity, string, bool, error) {
	identity, kind, supported, err := Describe(message)
	if supported || err != nil {
		return identity, kind, supported, err
	}
	return DescribeCheckpoint(message)
}
