package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.EnvironmentProvisionAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type BackingDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.BackingProvisionAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type ResolverDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.ResolverWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}
type ReleaseDeliveryOwner interface {
	LoadRecoveryInput(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, error)
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.ReleaseWorkAssignment, error)
	RejectInterruptedBeforeEffects(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
	QueueInterruptedRecovery(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
	SettleInterruptedRecovery(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type ReleaseRestorationDeliveryOwner interface {
	LoadRecoveryInput(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type ScriptDeliveryOwner interface {
	LoadRecoveryInput(context.Context, workqueue.ResumeView) (scriptexecution.RecoveryInput, error)
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.ScriptWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type HierarchyDeliveryOwner interface {
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
	HierarchyAssignment(context.Context, workqueue.ResumeView) (*agentpb.ControllerMessage, error)
}

type DetachDeliveryOwner interface {
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
	DetachAssignment(context.Context, workqueue.ResumeView) (*agentpb.ControllerMessage, error)
}

type AttachDeliveryOwner interface {
	LoadRecoveryAssignment(context.Context, workqueue.ResumeView) (*agentpb.AttachProvisionRecoveryAssignment, error)
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.AttachProvisionAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type NetworkDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.NetworkWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type ComponentDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.ComponentWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type EntryDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.EntryConfigurationWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type VolumeDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.VolumeWorkAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type LifecycleDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.ServiceLifecycleAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

type ServiceRemovalDeliveryOwner interface {
	LifecycleDeliveryOwner
	LoadRemovalRecoveryAssignment(
		context.Context,
		workqueue.ResumeView,
	) (*agentpb.ServiceRemovalRecoveryAssignment, error)
}

type BackupDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.BackupWorkAssignment, error)
	LoadRecoveryAssignment(context.Context, workqueue.ResumeView) (*agentpb.BackupRecoveryAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}
