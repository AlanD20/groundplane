package workreceipt

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Terminal decodes the authenticated transport receipt using the original local
// Start receipt. Operation journals still validate their own retained proof when
// saving this result; this decoder cannot prove an external effect by itself.
func Terminal(
	request *agentpb.AgentMessage,
	reply *agentpb.ControllerMessage,
	original workexecution.Attempt,
) (workexecution.Attempt, []byte, error) {
	if err := Match(request, reply); err != nil {
		return workexecution.Attempt{}, nil, err
	}
	identity, kind, _, err := Describe(request)
	if err != nil {
		return workexecution.Attempt{}, nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, original.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, original) {
		return workexecution.Attempt{}, nil, invalid()
	}
	var body interface{ GetFinishedAt() *timestamppb.Timestamp }
	switch payload := reply.Payload.(type) {
	case *agentpb.ControllerMessage_ReleaseWorkCompletionAck:
		body = payload.ReleaseWorkCompletionAck
	case *agentpb.ControllerMessage_ReleaseWorkFailureAck:
		body = payload.ReleaseWorkFailureAck
	case *agentpb.ControllerMessage_ScriptWorkCompletionAck:
		body = payload.ScriptWorkCompletionAck
	case *agentpb.ControllerMessage_ScriptRecoveryCompletionAck:
		body = payload.ScriptRecoveryCompletionAck
	case *agentpb.ControllerMessage_ScriptWorkFailureAck:
		body = payload.ScriptWorkFailureAck
	case *agentpb.ControllerMessage_EnvironmentRemovalCompletionAck:
		body = payload.EnvironmentRemovalCompletionAck
	case *agentpb.ControllerMessage_EnvironmentRemovalFailureAck:
		body = payload.EnvironmentRemovalFailureAck
	case *agentpb.ControllerMessage_AttachRemovalCompletionAck:
		body = payload.AttachRemovalCompletionAck
	case *agentpb.ControllerMessage_AttachRemovalFailureAck:
		body = payload.AttachRemovalFailureAck
	case *agentpb.ControllerMessage_AttachRecoveryCompletionAck:
		body = payload.AttachRecoveryCompletionAck
	case *agentpb.ControllerMessage_VolumeWorkCompletionAck:
		body = payload.VolumeWorkCompletionAck
	case *agentpb.ControllerMessage_VolumeWorkFailureAck:
		body = payload.VolumeWorkFailureAck
	case *agentpb.ControllerMessage_VolumeWorkRecoveryReportAck:
		body = payload.VolumeWorkRecoveryReportAck
	case *agentpb.ControllerMessage_NetworkWorkCompletionAck:
		body = payload.NetworkWorkCompletionAck
	case *agentpb.ControllerMessage_NetworkWorkFailureAck:
		body = payload.NetworkWorkFailureAck
	case *agentpb.ControllerMessage_NetworkWorkRecoveryReportAck:
		body = payload.NetworkWorkRecoveryReportAck
	case *agentpb.ControllerMessage_ComponentWorkCompletionAck:
		body = payload.ComponentWorkCompletionAck
	case *agentpb.ControllerMessage_ComponentWorkFailureAck:
		body = payload.ComponentWorkFailureAck
	case *agentpb.ControllerMessage_ComponentWorkRecoveryReportAck:
		body = payload.ComponentWorkRecoveryReportAck
	case *agentpb.ControllerMessage_EntryConfigurationWorkCompletionAck:
		body = payload.EntryConfigurationWorkCompletionAck
	case *agentpb.ControllerMessage_EntryConfigurationWorkFailureAck:
		body = payload.EntryConfigurationWorkFailureAck
	case *agentpb.ControllerMessage_EntryConfigurationWorkRecoveryReportAck:
		body = payload.EntryConfigurationWorkRecoveryReportAck
	case *agentpb.ControllerMessage_AttachProvisionCompletionAck:
		body = payload.AttachProvisionCompletionAck
	case *agentpb.ControllerMessage_AttachProvisionFailureAck:
		body = payload.AttachProvisionFailureAck
	case *agentpb.ControllerMessage_AttachProvisionRecoveryReportAck:
		body = payload.AttachProvisionRecoveryReportAck
	case *agentpb.ControllerMessage_ServiceLifecycleCompletionAck:
		body = payload.ServiceLifecycleCompletionAck
		if payload.ServiceLifecycleCompletionAck.GetStopped() != nil {
			body = payload.ServiceLifecycleCompletionAck.Stopped
		}
	case *agentpb.ControllerMessage_ServiceLifecycleFailureAck:
		body = payload.ServiceLifecycleFailureAck
	case *agentpb.ControllerMessage_ServiceLifecycleRecoveryReportAck:
		body = payload.ServiceLifecycleRecoveryReportAck
	case *agentpb.ControllerMessage_ServiceRemovalRecoveryCompletionAck:
		body = payload.ServiceRemovalRecoveryCompletionAck
	case *agentpb.ControllerMessage_ServiceRemovalRecoveryFailureAck:
		body = payload.ServiceRemovalRecoveryFailureAck
	case *agentpb.ControllerMessage_BackupWorkCompletionAck:
		body = payload.BackupWorkCompletionAck
	case *agentpb.ControllerMessage_BackupWorkFailureAck:
		body = payload.BackupWorkFailureAck
	case *agentpb.ControllerMessage_BackupRecoveryAck:
		body = payload.BackupRecoveryAck
	case *agentpb.ControllerMessage_EnvironmentProvisionResultAck:
		body = payload.EnvironmentProvisionResultAck
	case *agentpb.ControllerMessage_BackingProvisionCompletionAck:
		body = payload.BackingProvisionCompletionAck
	case *agentpb.ControllerMessage_BackingProvisionFailureAck:
		body = payload.BackingProvisionFailureAck
	case *agentpb.ControllerMessage_BackingProvisionRecoveryAck:
		body = payload.BackingProvisionRecoveryAck
	case *agentpb.ControllerMessage_ResolverWorkResultAck:
		body = payload.ResolverWorkResultAck
	default:
		return workexecution.Attempt{}, nil, invalid()
	}
	finished := body.GetFinishedAt()
	if finished == nil || finished.CheckValid() != nil {
		return workexecution.Attempt{}, nil, invalid()
	}
	terminal := original
	at := finished.AsTime()
	terminal.FinishedAt = &at
	terminal.Outcome = workexecution.Succeeded
	if outcome, ok := body.(interface{ GetOutcome() string }); ok {
		terminal.Outcome = workexecution.Outcome(outcome.GetOutcome())
	}
	if kind == "removal_recovery_failure" {
		terminal.Outcome = workexecution.Failed
	}
	if boundary, ok := body.(interface{ GetBoundaryCompleted() bool }); ok && !boundary.GetBoundaryCompleted() {
		terminal.Outcome = workexecution.Failed
	}
	if restored, ok := body.(interface{ GetPredecessorRestored() bool }); ok && !restored.GetPredecessorRestored() {
		terminal.Outcome = workexecution.Failed
	}
	if started, ok := body.(interface{ GetStartedAt() *timestamppb.Timestamp }); ok {
		if timestamp := started.GetStartedAt(); timestamp != nil {
			if timestamp.CheckValid() != nil {
				return workexecution.Attempt{}, nil, invalid()
			}
			at := timestamp.AsTime()
			if original.StartedAt != nil && !original.StartedAt.Equal(at) {
				return workexecution.Attempt{}, nil, invalid()
			}
			terminal.StartedAt = &at
		}
	}
	var reference *agentpb.WorkInputReference
	if binding, ok := body.(interface {
		GetBindingResult() *agentpb.WorkInputReference
	}); ok {
		reference = binding.GetBindingResult()
	} else if evidence, ok := body.(interface {
		GetEvidence() *agentpb.WorkInputReference
	}); ok {
		reference = evidence.GetEvidence()
	} else if checkpoint, ok := body.(interface {
		GetCheckpointEvidence() *agentpb.WorkInputReference
	}); ok {
		reference = checkpoint.GetCheckpointEvidence()
	}
	if reference == nil {
		return workexecution.Attempt{}, nil, invalid()
	}
	terminal.Result = &workexecution.Input{ID: reference.InputId, SHA256: hex.EncodeToString(reference.Sha256)}
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, nil, err
	}
	var failure []byte
	if evidence, ok := body.(interface{ GetFailure() []byte }); ok {
		failure = evidence.GetFailure()
	}
	return terminal, failure, nil
}
