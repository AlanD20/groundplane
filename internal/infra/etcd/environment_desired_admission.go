package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentDesiredPublication(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim blueprints.EnvironmentBlueprintStageClaim,
	revision blueprints.EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Revision <= 0 ||
		environment.Revision <= 0 || project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		expectedHeadRevision < 0 {
		return errs.New(
			errs.KindStateConflict, "Environment is not ready for desired-state publication",
		)
	}
	taskEnvironment, ownsDesired, taskEnvironmentErr := desiredRevisionTaskEnvironment(task)
	if recordcodec.ValidateID(ids.KindEnvironment, revision.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, revision.RevisionID) != nil ||
		revision.EnvironmentID != environment.Record.ID ||
		claim.EnvironmentID != revision.EnvironmentID || claim.RevisionID != revision.RevisionID ||
		claim.TaskID != task.ID || projection.EnvironmentID != revision.EnvironmentID ||
		projection.RevisionID != revision.RevisionID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != revision.RevisionID ||
		taskEnvironmentErr != nil || !ownsDesired || taskEnvironment != revision.EnvironmentID ||
		task.Status != taskjournal.TaskStatusPending {
		return errs.New(
			errs.KindValidationFailed, "Environment desired revision publication identity is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return errs.New(
			errs.KindValidationFailed, "Environment desired revision marker does not match its Task",
		)
	}
	return nil
}
