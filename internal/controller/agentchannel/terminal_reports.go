package agentchannel

import (
	"context"
	"errors"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) terminalOperationReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	identity, _, supported, err := workreceipt.Describe(message)
	if !supported {
		return false, nil, err
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	if _, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity()); err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply, err := server.terminalReport(session.ctx, authorization, message)
	if err != nil {
		slog.WarnContext(session.ctx, "Agent result requires reconciliation", "task_id", identity.TaskId,
			"work_id", identity.WorkItemId, "attempt_id", identity.AttemptId, "error", err)
		problem := errs.New(errs.KindInternal, "").ToProblem()
		var productError *errs.Error
		if errors.As(err, &productError) {
			problem = productError.ToProblem()
		}
		if len(problem.Detail) > 4096 {
			problem.Detail = "terminal report could not be settled; inspect Controller logs"
		}
		rejection, rejectErr := workreceipt.Reject(message, string(problem.Code), problem.Detail)
		return true, rejection, rejectErr
	}
	if err := workreceipt.Match(message, reply); err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}

// terminalReport is shared by live delivery and startup replay. This
// switch is deliberately closed: no grant, hook, input or checkpoint route.
func (server *Server) terminalReport(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	message *agentpb.AgentMessage,
) (*agentpb.ControllerMessage, error) {
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ReleaseWorkCompletionRequest:
		if server.releaseCheckpoints == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.releaseCheckpoints.CompleteRelease(ctx, authorization, payload.ReleaseWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseWorkCompletionAck{ReleaseWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ReleaseWorkFailureRequest:
		if server.releaseCheckpoints == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.releaseCheckpoints.FailRelease(ctx, authorization, payload.ReleaseWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseWorkFailureAck{ReleaseWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_ScriptWorkCompletionRequest:
		if server.scriptCompletion == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.scriptCompletion.CompleteScript(ctx, authorization, payload.ScriptWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkCompletionAck{ScriptWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ScriptRecoveryCompletionRequest:
		if server.scriptCompletion == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.scriptCompletion.CompleteScriptRecovery(ctx, authorization, payload.ScriptRecoveryCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptRecoveryCompletionAck{ScriptRecoveryCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ScriptWorkFailureRequest:
		if server.scriptCompletion == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.scriptCompletion.FailScript(ctx, authorization, payload.ScriptWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkFailureAck{ScriptWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_EnvironmentRemovalCompletionRequest:
		if server.hierarchyReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.hierarchyReports.CompleteEnvironmentRemoval(ctx, authorization, payload.EnvironmentRemovalCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentRemovalCompletionAck{EnvironmentRemovalCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_EnvironmentRemovalFailureRequest:
		if server.hierarchyReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.hierarchyReports.FailEnvironmentRemoval(ctx, authorization, payload.EnvironmentRemovalFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentRemovalFailureAck{EnvironmentRemovalFailureAck: ack}}, nil
	case *agentpb.AgentMessage_AttachRemovalCompletionRequest:
		if server.attachReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachReports.CompleteAttachRemoval(ctx, authorization, payload.AttachRemovalCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachRemovalCompletionAck{AttachRemovalCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_AttachRemovalFailureRequest:
		if server.attachReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachReports.FailAttachRemoval(ctx, authorization, payload.AttachRemovalFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachRemovalFailureAck{AttachRemovalFailureAck: ack}}, nil
	case *agentpb.AgentMessage_AttachRecoveryCompletionRequest:
		if server.attachReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachReports.CompleteAttachRecovery(ctx, authorization, payload.AttachRecoveryCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachRecoveryCompletionAck{AttachRecoveryCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_VolumeWorkCompletionRequest:
		if server.volumeReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.volumeReports.CompleteVolume(ctx, authorization, payload.VolumeWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_VolumeWorkCompletionAck{VolumeWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_VolumeWorkFailureRequest:
		if server.volumeReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.volumeReports.FailVolume(ctx, authorization, payload.VolumeWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_VolumeWorkFailureAck{VolumeWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_VolumeWorkRecoveryReportRequest:
		if server.volumeReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.volumeReports.RecoverVolume(ctx, authorization, payload.VolumeWorkRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_VolumeWorkRecoveryReportAck{VolumeWorkRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_NetworkWorkCompletionRequest:
		if server.networkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.networkReports.CompleteNetwork(ctx, authorization, payload.NetworkWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_NetworkWorkCompletionAck{NetworkWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_NetworkWorkFailureRequest:
		if server.networkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.networkReports.FailNetwork(ctx, authorization, payload.NetworkWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_NetworkWorkFailureAck{NetworkWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_NetworkWorkRecoveryReportRequest:
		if server.networkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.networkReports.RecoverNetwork(ctx, authorization, payload.NetworkWorkRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_NetworkWorkRecoveryReportAck{NetworkWorkRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_ComponentWorkCompletionRequest:
		if server.componentReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.componentReports.CompleteComponent(ctx, authorization, payload.ComponentWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ComponentWorkCompletionAck{ComponentWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ComponentWorkFailureRequest:
		if server.componentReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.componentReports.FailComponent(ctx, authorization, payload.ComponentWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ComponentWorkFailureAck{ComponentWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_ComponentWorkRecoveryReportRequest:
		if server.componentReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.componentReports.RecoverComponent(ctx, authorization, payload.ComponentWorkRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ComponentWorkRecoveryReportAck{ComponentWorkRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_EntryConfigurationWorkCompletionRequest:
		if server.entryReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.entryReports.CompleteEntry(ctx, authorization, payload.EntryConfigurationWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EntryConfigurationWorkCompletionAck{EntryConfigurationWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_EntryConfigurationWorkFailureRequest:
		if server.entryReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.entryReports.FailEntry(ctx, authorization, payload.EntryConfigurationWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EntryConfigurationWorkFailureAck{EntryConfigurationWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_EntryConfigurationWorkRecoveryReportRequest:
		if server.entryReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.entryReports.RecoverEntry(ctx, authorization, payload.EntryConfigurationWorkRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EntryConfigurationWorkRecoveryReportAck{EntryConfigurationWorkRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_AttachProvisionCompletionRequest:
		if server.attachProvisionReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachProvisionReports.CompleteAttachProvision(ctx, authorization, payload.AttachProvisionCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachProvisionCompletionAck{AttachProvisionCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_AttachProvisionFailureRequest:
		if server.attachProvisionReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachProvisionReports.FailAttachProvision(ctx, authorization, payload.AttachProvisionFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachProvisionFailureAck{AttachProvisionFailureAck: ack}}, nil
	case *agentpb.AgentMessage_AttachProvisionRecoveryReportRequest:
		if server.attachProvisionReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.attachProvisionReports.ReportProvisionRecovery(ctx, authorization, payload.AttachProvisionRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachProvisionRecoveryReportAck{AttachProvisionRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_ServiceLifecycleCompletionRequest:
		if server.lifecycleReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.lifecycleReports.CompleteLifecycle(ctx, authorization, payload.ServiceLifecycleCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ServiceLifecycleCompletionAck{ServiceLifecycleCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ServiceLifecycleFailureRequest:
		if server.lifecycleReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.lifecycleReports.FailLifecycle(ctx, authorization, payload.ServiceLifecycleFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ServiceLifecycleFailureAck{ServiceLifecycleFailureAck: ack}}, nil
	case *agentpb.AgentMessage_ServiceLifecycleRecoveryReportRequest:
		if server.lifecycleReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.lifecycleReports.RecoverLifecycle(ctx, authorization, payload.ServiceLifecycleRecoveryReportRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ServiceLifecycleRecoveryReportAck{ServiceLifecycleRecoveryReportAck: ack}}, nil
	case *agentpb.AgentMessage_ServiceRemovalRecoveryCompletionRequest:
		if server.removalRecoveryReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.removalRecoveryReports.CompleteRemovalRecovery(ctx, authorization, payload.ServiceRemovalRecoveryCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ServiceRemovalRecoveryCompletionAck{ServiceRemovalRecoveryCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_ServiceRemovalRecoveryFailureRequest:
		if server.removalRecoveryReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.removalRecoveryReports.FailRemovalRecovery(ctx, authorization, payload.ServiceRemovalRecoveryFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ServiceRemovalRecoveryFailureAck{ServiceRemovalRecoveryFailureAck: ack}}, nil
	case *agentpb.AgentMessage_BackupWorkCompletionRequest:
		if server.backupWorkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backupWorkReports.Complete(ctx, authorization, payload.BackupWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		if server.secrets != nil {
			if err := server.secrets.ReleaseRestoreIdentity(ctx, ack.GetAttempt().GetTaskId()); err != nil {
				return nil, err
			}
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupWorkCompletionAck{BackupWorkCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_BackupWorkFailureRequest:
		if server.backupWorkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backupWorkReports.Fail(ctx, authorization, payload.BackupWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupWorkFailureAck{BackupWorkFailureAck: ack}}, nil
	case *agentpb.AgentMessage_BackupRecoveryRequest:
		if server.backupWorkReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backupWorkReports.Recover(ctx, authorization, payload.BackupRecoveryRequest)
		if err != nil {
			return nil, err
		}
		if server.secrets != nil {
			if err := server.secrets.ReleaseRestoreIdentity(ctx, ack.GetAttempt().GetTaskId()); err != nil {
				return nil, err
			}
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupRecoveryAck{BackupRecoveryAck: ack}}, nil
	case *agentpb.AgentMessage_EnvironmentProvisionResultRequest:
		if server.environmentProvision == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.environmentProvision.ReportEnvironmentProvision(ctx, authorization, payload.EnvironmentProvisionResultRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentProvisionResultAck{EnvironmentProvisionResultAck: ack}}, nil
	case *agentpb.AgentMessage_BackingProvisionCompletionRequest:
		if server.backingReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backingReports.CompleteBackingProvision(ctx, authorization, payload.BackingProvisionCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackingProvisionCompletionAck{BackingProvisionCompletionAck: ack}}, nil
	case *agentpb.AgentMessage_BackingProvisionFailureRequest:
		if server.backingReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backingReports.FailBackingProvision(ctx, authorization, payload.BackingProvisionFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackingProvisionFailureAck{BackingProvisionFailureAck: ack}}, nil
	case *agentpb.AgentMessage_BackingProvisionRecoveryRequest:
		if server.backingReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.backingReports.RecoverBackingProvision(ctx, authorization, payload.BackingProvisionRecoveryRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackingProvisionRecoveryAck{BackingProvisionRecoveryAck: ack}}, nil
	case *agentpb.AgentMessage_ResolverWorkCompletionRequest:
		if server.resolverReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.resolverReports.CompleteResolver(ctx, authorization, payload.ResolverWorkCompletionRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}}, nil
	case *agentpb.AgentMessage_ResolverWorkFailureRequest:
		if server.resolverReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.resolverReports.FailResolver(ctx, authorization, payload.ResolverWorkFailureRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}}, nil
	case *agentpb.AgentMessage_ResolverWorkRecoveryRequest:
		if server.resolverReports == nil {
			return nil, errs.New(errs.KindInternal, "terminal result owner is not configured")
		}
		ack, err := server.resolverReports.RecoverResolver(ctx, authorization, payload.ResolverWorkRecoveryRequest)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}}, nil
	default:
		return nil, errs.New(errs.KindValidationFailed, "startup replay accepts only terminal result reports")
	}
}
