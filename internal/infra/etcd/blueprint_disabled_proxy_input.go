package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Disabled selection has a Controller operation, not an Agent Task or invented
// execution receipt. Its future complete projection uses the same inert chunk
// owner; atomic metadata settlement must subsequently fence that reference.
func (repository *EnvironmentBlueprintRepository) StageBlueprintDisabledProxyCandidateInput(ctx context.Context,
	parent TaskRecord, operationID string, candidate componentrecord.Record,
	projection projectionrecord.EnvironmentComposeProjection, desiredRootRevision int64,
) (keyvalue.Versioned[componentplanning.ProxyCandidateInputReference], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	if repository == nil || validateBlueprintParentClaimTask(parent) != nil ||
		parent.Status != taskjournal.TaskStatusRunning ||
		desiredRootRevision <= 0 ||
		componentrecord.ValidateRecord(candidate) != nil ||
		!candidate.Desired.Kind.IsEnvironmentProxy() ||
		candidate.Desired.Enabled ||
		candidate.Desired.OwnerID != parent.Owner.EnvironmentID ||
		len(candidate.Runtime.GeneratedServices) != 0 ||
		candidate.Runtime.PinnedIPv4 != "" ||
		candidate.Runtime.Healthy ||
		projection.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.RevisionID != parent.ID ||
		projection.RenderGeneration != uint64(parent.RenderGeneration) {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindValidationFailed,
			"disabled proxy candidate input is invalid",
		)
	}
	return repository.stageBlueprintProxyCandidateInput(ctx, componentplanning.ProxyCandidateInputReference{
		EnvironmentID: parent.Owner.EnvironmentID, ParentTaskID: parent.ID, ComponentID: candidate.Desired.ID,
		SourceOwner: componentplanning.ProxyInputControllerSettlement, SourceID: operationID,
	}, projection, desiredRootRevision)
}
