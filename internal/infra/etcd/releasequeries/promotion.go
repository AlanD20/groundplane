package releasequeries

import (
	"context"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SelectPromotion captures the successful source at the destination's planning revision.
// Compare the source's retention and owner alongside native Release publication.
func (ledger *Reader) SelectPromotion(ctx context.Context, scope ReleasePlanningScope,
	releaseID string,
) (domain.Intent, []keyvalue.Condition, error) {
	view, err := ledger.getAt(ctx, releaseID, scope.ReadRevision)
	if err != nil {
		return domain.Intent{}, nil, err
	}
	if !promotionWorkspaceMatches(view.Intent, scope) {
		return domain.Intent{}, nil, errs.New(errs.KindReleaseNotFound,
			"promotion source must be a Release in a sibling Environment of this Project")
	}
	if err := verifyPromotionMaterial(view); err != nil {
		return domain.Intent{}, nil, err
	}
	keys := []string{hierarchy.EnvironmentKey(view.Intent.EnvironmentID),
		deletions.TombstoneKey("environment", view.Intent.EnvironmentID),
		releases.ReleaseRetentionKey(releaseID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: scope.ReadRevision})
	if err != nil {
		return domain.Intent{}, nil, err
	}
	if read == nil || read.ReadRevision != scope.ReadRevision || len(read.Values) != len(keys) ||
		read.Values[2] == nil {
		return domain.Intent{}, nil, releases.CorruptReleaseRecord()
	}
	if read.Values[0] == nil || read.Values[1] != nil {
		return domain.Intent{}, nil, errs.New(errs.KindReleaseNotFound, "promotion source Environment is unavailable")
	}
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil || environment.ID != view.Intent.EnvironmentID || environment.ProjectID != scope.Project.Record.ID {
		return domain.Intent{}, nil, releases.CorruptReleaseRecord()
	}
	if environment.ProvisioningState != hierarchy.EnvironmentProvisioningReady || environment.DeletionTaskID != "" {
		return domain.Intent{}, nil, errs.New(errs.KindReleaseNotFound, "promotion source Environment is unavailable")
	}
	return view.Intent, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1]},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision},
	}, nil
}

func promotionWorkspaceMatches(source domain.Intent, scope ReleasePlanningScope) bool {
	return source.Workspace.Kind == domain.WorkspaceTenant && source.Workspace.TenantID == scope.Tenant.Record.ID &&
		source.Workspace.ProjectID == scope.Project.Record.ID && source.EnvironmentID != scope.Environment.Record.ID
}

func verifyPromotionMaterial(view ReleaseView) error {
	if view.Checkpoint.State != domain.StateCompleted || view.Terminal == nil ||
		view.Terminal.Outcome != domain.StateCompleted || view.Terminal.FinalServingReleaseID != view.Intent.ID {
		return errs.New(errs.KindStateConflict, "promotion source did not complete and serve successfully")
	}
	if view.Retention == nil || view.Retention.Status != domain.RetentionAvailable {
		return errs.New(errs.KindStateConflict, "promotion source material is no longer retained")
	}
	if !slices.Contains(view.Retention.References, view.Intent.CandidateWorkload.LocalImageID) {
		return releases.CorruptReleaseRecord()
	}
	return nil
}
