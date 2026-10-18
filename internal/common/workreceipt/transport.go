// Package workreceipt defines the closed, replay-only terminal report boundary.
// It grants no execution, input delivery, checkpoint or hook permission.
package workreceipt

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const MaximumBytes = 36 * 1024

// Describe returns false for ordinary execution messages. Only these compiled
// final reports may be retained and replayed before an executor becomes Ready.
func Describe(message *agentpb.AgentMessage) (*agentpb.WorkAttemptIdentity, string, bool, error) {
	if message == nil {
		return nil, "", false, nil
	}
	var identity *agentpb.WorkAttemptIdentity
	kind := ""
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ReleaseWorkCompletionRequest:
		identity, kind = payload.ReleaseWorkCompletionRequest.GetAttempt(), "release_completion"
	case *agentpb.AgentMessage_ReleaseWorkFailureRequest:
		identity, kind = payload.ReleaseWorkFailureRequest.GetAttempt(), "release_failure"
	case *agentpb.AgentMessage_ScriptWorkCompletionRequest:
		identity, kind = payload.ScriptWorkCompletionRequest.GetAttempt(), "script_completion"
	case *agentpb.AgentMessage_ScriptRecoveryCompletionRequest:
		identity, kind = payload.ScriptRecoveryCompletionRequest.GetAttempt(), "script_recovery_completion"
	case *agentpb.AgentMessage_ScriptWorkFailureRequest:
		identity, kind = payload.ScriptWorkFailureRequest.GetAttempt(), "script_failure"
	case *agentpb.AgentMessage_EnvironmentRemovalCompletionRequest:
		identity, kind = payload.EnvironmentRemovalCompletionRequest.GetAttempt(), "environment_removal_completion"
	case *agentpb.AgentMessage_EnvironmentRemovalFailureRequest:
		identity, kind = payload.EnvironmentRemovalFailureRequest.GetAttempt(), "environment_removal_failure"
	case *agentpb.AgentMessage_AttachRemovalCompletionRequest:
		identity, kind = payload.AttachRemovalCompletionRequest.GetAttempt(), "attach_removal_completion"
	case *agentpb.AgentMessage_AttachRemovalFailureRequest:
		identity, kind = payload.AttachRemovalFailureRequest.GetAttempt(), "attach_removal_failure"
	case *agentpb.AgentMessage_AttachRecoveryCompletionRequest:
		identity, kind = payload.AttachRecoveryCompletionRequest.GetAttempt(), "attach_recovery_completion"
	case *agentpb.AgentMessage_VolumeWorkCompletionRequest:
		identity, kind = payload.VolumeWorkCompletionRequest.GetAttempt(), "volume_completion"
	case *agentpb.AgentMessage_VolumeWorkFailureRequest:
		identity, kind = payload.VolumeWorkFailureRequest.GetAttempt(), "volume_failure"
	case *agentpb.AgentMessage_VolumeWorkRecoveryReportRequest:
		identity, kind = payload.VolumeWorkRecoveryReportRequest.GetAttempt(), "volume_recovery"
	case *agentpb.AgentMessage_NetworkWorkCompletionRequest:
		identity, kind = payload.NetworkWorkCompletionRequest.GetAttempt(), "network_completion"
	case *agentpb.AgentMessage_NetworkWorkFailureRequest:
		identity, kind = payload.NetworkWorkFailureRequest.GetAttempt(), "network_failure"
	case *agentpb.AgentMessage_NetworkWorkRecoveryReportRequest:
		identity, kind = payload.NetworkWorkRecoveryReportRequest.GetAttempt(), "network_recovery"
	case *agentpb.AgentMessage_ComponentWorkCompletionRequest:
		identity, kind = payload.ComponentWorkCompletionRequest.GetAttempt(), "component_completion"
	case *agentpb.AgentMessage_ComponentWorkFailureRequest:
		identity, kind = payload.ComponentWorkFailureRequest.GetAttempt(), "component_failure"
	case *agentpb.AgentMessage_ComponentWorkRecoveryReportRequest:
		identity, kind = payload.ComponentWorkRecoveryReportRequest.GetAttempt(), "component_recovery"
	case *agentpb.AgentMessage_EntryConfigurationWorkCompletionRequest:
		identity, kind = payload.EntryConfigurationWorkCompletionRequest.GetAttempt(), "entry_completion"
	case *agentpb.AgentMessage_EntryConfigurationWorkFailureRequest:
		identity, kind = payload.EntryConfigurationWorkFailureRequest.GetAttempt(), "entry_failure"
	case *agentpb.AgentMessage_EntryConfigurationWorkRecoveryReportRequest:
		identity, kind = payload.EntryConfigurationWorkRecoveryReportRequest.GetAttempt(), "entry_recovery"
	case *agentpb.AgentMessage_AttachProvisionCompletionRequest:
		identity, kind = payload.AttachProvisionCompletionRequest.GetAttempt(), "attach_provision_completion"
	case *agentpb.AgentMessage_AttachProvisionFailureRequest:
		identity, kind = payload.AttachProvisionFailureRequest.GetAttempt(), "attach_provision_failure"
	case *agentpb.AgentMessage_AttachProvisionRecoveryReportRequest:
		identity, kind = payload.AttachProvisionRecoveryReportRequest.GetAttempt(), "attach_provision_recovery"
	case *agentpb.AgentMessage_ServiceLifecycleCompletionRequest:
		identity, kind = payload.ServiceLifecycleCompletionRequest.GetAttempt(), "lifecycle_completion"
	case *agentpb.AgentMessage_ServiceLifecycleFailureRequest:
		identity, kind = payload.ServiceLifecycleFailureRequest.GetAttempt(), "lifecycle_failure"
	case *agentpb.AgentMessage_ServiceLifecycleRecoveryReportRequest:
		identity, kind = payload.ServiceLifecycleRecoveryReportRequest.GetAttempt(), "lifecycle_recovery"
	case *agentpb.AgentMessage_ServiceRemovalRecoveryCompletionRequest:
		identity, kind = payload.ServiceRemovalRecoveryCompletionRequest.GetAttempt(), "removal_recovery_completion"
	case *agentpb.AgentMessage_ServiceRemovalRecoveryFailureRequest:
		identity, kind = payload.ServiceRemovalRecoveryFailureRequest.GetAttempt(), "removal_recovery_failure"
	case *agentpb.AgentMessage_BackupWorkCompletionRequest:
		identity, kind = payload.BackupWorkCompletionRequest.GetAttempt(), "backup_completion"
	case *agentpb.AgentMessage_BackupWorkFailureRequest:
		identity, kind = payload.BackupWorkFailureRequest.GetAttempt(), "backup_failure"
	case *agentpb.AgentMessage_BackupRecoveryRequest:
		identity, kind = payload.BackupRecoveryRequest.GetAttempt(), "backup_recovery"
	case *agentpb.AgentMessage_EnvironmentProvisionResultRequest:
		identity, kind = payload.EnvironmentProvisionResultRequest.GetAttempt(), "environment_provision_result"
	case *agentpb.AgentMessage_BackingProvisionCompletionRequest:
		identity, kind = payload.BackingProvisionCompletionRequest.GetAttempt(), "backing_completion"
	case *agentpb.AgentMessage_BackingProvisionFailureRequest:
		identity, kind = payload.BackingProvisionFailureRequest.GetAttempt(), "backing_failure"
	case *agentpb.AgentMessage_BackingProvisionRecoveryRequest:
		identity, kind = payload.BackingProvisionRecoveryRequest.GetAttempt(), "backing_recovery"
	case *agentpb.AgentMessage_ResolverWorkCompletionRequest:
		identity, kind = payload.ResolverWorkCompletionRequest.GetAttempt(), "resolver_completion"
	case *agentpb.AgentMessage_ResolverWorkFailureRequest:
		identity, kind = payload.ResolverWorkFailureRequest.GetAttempt(), "resolver_failure"
	case *agentpb.AgentMessage_ResolverWorkRecoveryRequest:
		identity, kind = payload.ResolverWorkRecoveryRequest.GetAttempt(), "resolver_recovery"
	default:
		return nil, "", false, nil
	}
	if identity == nil || executionplan.RejectUnknown(message) != nil || proto.Size(message) > MaximumBytes {
		return nil, "", true, invalid()
	}
	executor := workexecution.Identity{Executor: workexecution.Agent, HostID: identity.HostId, AgentID: identity.AgentId, Generation: identity.ExecutorGeneration}
	if _, err := workexecution.DecodeAgentAttempt(identity, executor); err != nil {
		return nil, "", true, err
	}
	return identity, kind, true, nil
}

// Match checks the exact report family and assignment, not just an Attempt id.
// The operation remains responsible for its result and physical-effect proof.
func Match(request *agentpb.AgentMessage, reply *agentpb.ControllerMessage) error {
	identity, kind, supported, err := Describe(request)
	if err != nil {
		return err
	}
	if !supported || reply == nil || executionplan.RejectUnknown(reply) != nil || proto.Size(reply) > MaximumBytes {
		return invalid()
	}
	var acknowledged *agentpb.WorkAttemptIdentity
	switch kind {
	case "release_completion":
		acknowledged = reply.GetReleaseWorkCompletionAck().GetAttempt()
	case "release_failure":
		acknowledged = reply.GetReleaseWorkFailureAck().GetAttempt()
	case "script_completion":
		acknowledged = reply.GetScriptWorkCompletionAck().GetAttempt()
	case "script_recovery_completion":
		acknowledged = reply.GetScriptRecoveryCompletionAck().GetAttempt()
	case "script_failure":
		acknowledged = reply.GetScriptWorkFailureAck().GetAttempt()
	case "environment_removal_completion":
		acknowledged = reply.GetEnvironmentRemovalCompletionAck().GetAttempt()
	case "environment_removal_failure":
		acknowledged = reply.GetEnvironmentRemovalFailureAck().GetAttempt()
	case "attach_removal_completion":
		acknowledged = reply.GetAttachRemovalCompletionAck().GetAttempt()
	case "attach_removal_failure":
		acknowledged = reply.GetAttachRemovalFailureAck().GetAttempt()
	case "attach_recovery_completion":
		acknowledged = reply.GetAttachRecoveryCompletionAck().GetAttempt()
	case "volume_completion":
		acknowledged = reply.GetVolumeWorkCompletionAck().GetAttempt()
	case "volume_failure":
		acknowledged = reply.GetVolumeWorkFailureAck().GetAttempt()
	case "volume_recovery":
		acknowledged = reply.GetVolumeWorkRecoveryReportAck().GetAttempt()
	case "network_completion":
		acknowledged = reply.GetNetworkWorkCompletionAck().GetAttempt()
	case "network_failure":
		acknowledged = reply.GetNetworkWorkFailureAck().GetAttempt()
	case "network_recovery":
		acknowledged = reply.GetNetworkWorkRecoveryReportAck().GetAttempt()
	case "component_completion":
		acknowledged = reply.GetComponentWorkCompletionAck().GetAttempt()
	case "component_failure":
		acknowledged = reply.GetComponentWorkFailureAck().GetAttempt()
	case "component_recovery":
		acknowledged = reply.GetComponentWorkRecoveryReportAck().GetAttempt()
	case "entry_completion":
		acknowledged = reply.GetEntryConfigurationWorkCompletionAck().GetAttempt()
	case "entry_failure":
		acknowledged = reply.GetEntryConfigurationWorkFailureAck().GetAttempt()
	case "entry_recovery":
		acknowledged = reply.GetEntryConfigurationWorkRecoveryReportAck().GetAttempt()
	case "attach_provision_completion":
		acknowledged = reply.GetAttachProvisionCompletionAck().GetAttempt()
	case "attach_provision_failure":
		acknowledged = reply.GetAttachProvisionFailureAck().GetAttempt()
	case "attach_provision_recovery":
		acknowledged = reply.GetAttachProvisionRecoveryReportAck().GetAttempt()
	case "lifecycle_completion":
		acknowledged = reply.GetServiceLifecycleCompletionAck().GetAttempt()
	case "lifecycle_failure":
		acknowledged = reply.GetServiceLifecycleFailureAck().GetAttempt()
	case "lifecycle_recovery":
		acknowledged = reply.GetServiceLifecycleRecoveryReportAck().GetAttempt()
	case "removal_recovery_completion":
		acknowledged = reply.GetServiceRemovalRecoveryCompletionAck().GetAttempt()
	case "removal_recovery_failure":
		acknowledged = reply.GetServiceRemovalRecoveryFailureAck().GetAttempt()
	case "backup_completion":
		acknowledged = reply.GetBackupWorkCompletionAck().GetAttempt()
	case "backup_failure":
		acknowledged = reply.GetBackupWorkFailureAck().GetAttempt()
	case "backup_recovery":
		acknowledged = reply.GetBackupRecoveryAck().GetAttempt()
	case "environment_provision_result":
		acknowledged = reply.GetEnvironmentProvisionResultAck().GetAttempt()
	case "backing_completion":
		acknowledged = reply.GetBackingProvisionCompletionAck().GetAttempt()
	case "backing_failure":
		acknowledged = reply.GetBackingProvisionFailureAck().GetAttempt()
	case "backing_recovery":
		acknowledged = reply.GetBackingProvisionRecoveryAck().GetAttempt()
	case "resolver_completion":
		acknowledged = reply.GetResolverWorkResultAck().GetAttempt()
		if reply.GetResolverWorkResultAck().GetKind() != "completion" {
			return invalid()
		}
	case "resolver_failure":
		acknowledged = reply.GetResolverWorkResultAck().GetAttempt()
		if reply.GetResolverWorkResultAck().GetKind() != "failure" {
			return invalid()
		}
	case "resolver_recovery":
		acknowledged = reply.GetResolverWorkResultAck().GetAttempt()
		if reply.GetResolverWorkResultAck().GetKind() != "recovery" {
			return invalid()
		}
	}
	if acknowledged == nil || !proto.Equal(identity, acknowledged) {
		return invalid()
	}
	return nil
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "terminal report replay differs from its captured assignment or result family")
}
