package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRuntimeProjection joins acknowledged Attach/Entry/Component inputs
// into one immutable render. A newly sealed projection is usable on a fresh
// work snapshot, never retroactively at the snapshot that selected its inputs.
func (service *Service) PrepareRuntimeProjection(ctx context.Context,
	parent workexecution.Task, snapshot blueprintunits.Snapshot,
) (bool, error) {
	if err := validateAuthoredRuntimeProjectionParent(parent, snapshot); err != nil {
		return false, err
	}
	_, found, err := service.repository.ReadBlueprintRuntimeProjection(
		ctx, parent.Scope.EnvironmentID, parent.ID, snapshot.ReadRevision,
	)
	if err != nil || found {
		return found, err
	}
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, parent)
	if err != nil {
		return false, err
	}
	if err := requireAuthoredProjectionInputReceipts(source.desired.Record, source.identities.Record, snapshot, ""); err != nil {
		if kind, _ := errs.KindOf(err); kind == errs.KindResourceInUse {
			return false, nil
		}
		return false, err
	}
	components, err := service.loadAuthoredComponentSelections(ctx, parent, source, snapshot, nil)
	if err != nil {
		return false, err
	}
	attaches, err := service.loadAuthoredRuntimeAttaches(
		ctx,
		parent.Scope.EnvironmentID,
		source.identities.Record,
		snapshot,
	)
	if err != nil {
		return false, err
	}
	requirements, err := service.loadAuthoredRuntimeRequirements(ctx, source.desired, snapshot.HeadRevision)
	if err != nil {
		return false, err
	}
	projection, err := service.compileAuthoredEnvironmentInput(
		ctx,
		parent,
		source,
		attaches,
		authoredComponentCompileInput{
			records:   components.records,
			projected: components.projected,
			sources:   components.sources,
		},
		requirements,
	)
	if err != nil {
		return false, err
	}
	backup, err := service.prepareAuthoredRuntimeBackup(ctx, parent, source, snapshot, projection, attaches)
	if err != nil {
		return false, err
	}
	defer backup.Clear()
	projection.Backup = backup.Projection()
	err = service.repository.SealEnvironmentBlueprintRuntimeProjection(ctx, etcd.BlueprintRuntimeProjectionSeal{
		Parent: parent, SnapshotReadRevision: snapshot.ReadRevision,
		ExpectedHeadRevision: snapshot.HeadRevision, ExpectedEpochRevision: snapshot.EpochRevision,
		DesiredRootRevision: source.desired.Revision, OwnedIdentitiesRevision: source.identities.Revision,
		Tenant: source.tenant, Project: source.project, Environment: source.environment,
		Projection: projection, Attaches: attaches, Components: components.authorities,
		ComponentReceipts: components.receipts, BackupPreparation: backup,
	})
	return false, err
}
