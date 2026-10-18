package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Disabled selection has a Controller operation, not an Agent Task or invented
// execution receipt. Its future complete projection uses the same inert chunk
// owner; atomic metadata settlement must subsequently fence that reference.
func (repository *EnvironmentBlueprintRepository) StageBlueprintDisabledProxyCandidateInput(ctx context.Context,
	work workqueue.View, candidate componentrecord.Record,
	projection projectionrecord.EnvironmentComposeProjection, desiredRootRevision int64,
) (keyvalue.Versioned[componentplanning.ProxyCandidateInputReference], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	parent, item := work.Task.Task, work.Item.Item
	operationID, err := componentplanning.ControllerProxyOperationID(parent, item)
	if err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	if repository == nil || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		desiredRootRevision <= 0 || work.ReadRevision < desiredRootRevision ||
		parent.Stop != nil || parent.FinishedAt != nil || parent.CollectionStartedAt != nil ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		item.TargetID != candidate.Desired.ID ||
		componentrecord.ValidateRecord(candidate) != nil ||
		!candidate.Desired.Kind.IsEnvironmentProxy() ||
		candidate.Desired.Enabled ||
		candidate.Desired.OwnerID != parent.Scope.EnvironmentID ||
		len(candidate.Runtime.GeneratedServices) != 0 ||
		candidate.Runtime.PinnedIPv4 != "" ||
		candidate.Runtime.Healthy ||
		projection.EnvironmentID != parent.Scope.EnvironmentID ||
		projection.RevisionID != parent.ID {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindValidationFailed,
			"disabled proxy candidate input is invalid",
		)
	}
	if err := repository.validateProxyCandidateDesired(ctx, parent, projection, desiredRootRevision, work.ReadRevision); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	return repository.stageBlueprintProxyCandidateInput(ctx, componentplanning.ProxyCandidateInputReference{
		EnvironmentID: parent.Scope.EnvironmentID, ParentTaskID: parent.ID, ComponentID: candidate.Desired.ID,
		SourceOwner: componentplanning.ProxyInputControllerSettlement, SourceID: operationID,
	}, projection, desiredRootRevision)
}
