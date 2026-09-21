package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcdstore.Versioned[attachrender.AttachTaskRenderInput], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachrender.AttachTaskRenderInput]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindPlan, planID); err != nil {
		return etcdstore.Versioned[attachrender.AttachTaskRenderInput]{}, err
	}
	return recordquery.Get(
		ctx,
		repository.store,
		attachrender.AttachTaskRenderInputKey(planID),
		planID,
		errs.KindTaskNotFound,
		attachrender.DecodeAttachTaskRenderInput,
		func(record attachrender.AttachTaskRenderInput) string { return record.PlanID },
	)
}

func validateAttachTaskRenderInputScope(
	scope AttachCreateScope,
	record attachrecord.Record,
	task TaskRecord,
	input attachrender.AttachTaskRenderInput,
) error {
	if err := validateAttachRuntimePreparation(input, task); err != nil {
		return err
	}
	if scope.Tenant.Revision <= 0 || scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach render scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.DesiredHead.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.RevisionID != scope.DesiredHead.Record.RevisionID {
		return errs.New(errs.KindScopeUnauthorized, "Attach render hierarchy is invalid")
	}
	if input.PlanID != task.PlanID || input.AttachID != record.ID || input.AttachName != record.Name ||
		input.TenantID != scope.Tenant.Record.ID || input.TenantSlug != scope.Tenant.Record.Slug ||
		input.ProjectID != scope.Project.Record.ID || input.ProjectSlug != scope.Project.Record.Slug ||
		input.EnvironmentID != scope.Environment.Record.ID || input.EnvironmentName != scope.Environment.Record.Name ||
		input.AuthorizedVolumeDir != scope.Environment.Record.VolumeDir ||
		input.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		input.BackingProjectID != record.BackingProjectID ||
		input.AdapterKey != scope.BackingService.Record.Desired.Adapter ||
		input.Authentication != scope.BackingService.Record.Desired.Authentication ||
		!backinghook.EqualConfiguration(input.HookConfiguration, scope.BackingService.Record.Desired.Hooks) ||
		input.DesiredRevisionID != scope.DesiredHead.Record.RevisionID ||
		input.RenderGeneration != scope.ComposeProjection.Record.RenderGeneration ||
		input.RenderGeneration != uint64(task.RenderGeneration) ||
		input.RuntimeProjection.EnvironmentID != scope.ComposeProjection.Record.EnvironmentID ||
		input.RuntimeProjection.RevisionID != scope.ComposeProjection.Record.RevisionID ||
		input.RuntimeProjection.RenderGeneration != scope.ComposeProjection.Record.RenderGeneration ||
		!slices.Equal(input.Services, attachrender.AttachTaskServiceSnapshots(scope.ComposeProjection.Record.DesiredServices)) ||
		!slices.Equal(input.Networks, attachrender.AttachTaskOwnedNetworkSnapshots(scope.ComposeProjection.Record.DesiredZones)) ||
		!slices.Equal(input.Volumes, scope.ComposeProjection.Record.Volumes) ||
		!slices.Equal(input.VolumeMounts, scope.ComposeProjection.Record.VolumeMounts) ||
		!attachrender.EqualServiceDependencyPlans(
			input.ServiceDependencyPlans,
			scope.ComposeProjection.Record.ServiceDependencyPlans,
		) ||
		!slices.Equal(input.ConsumerServiceIDs, []string{record.ServiceID}) ||
		!slices.Equal(input.GrantAttachIDs, record.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "Attach Task render input does not match its pinned desired state")
	}
	if task.Type == taskjournal.TaskAttach {
		coveredByBackingNetwork := make(map[string]struct{})
		for _, join := range input.NetworkJoins {
			if join.NetworkID != record.BackingNetworkID {
				continue
			}
			for _, serviceID := range join.ServiceIDs {
				coveredByBackingNetwork[serviceID] = struct{}{}
			}
		}
		if _, exists := coveredByBackingNetwork[record.ServiceID]; !exists {
			return errs.New(
				errs.KindValidationFailed,
				"Attach Task render input omits a consumer Service binding to the backing network",
			)
		}
	}
	return attachrender.ValidateAttachTaskRenderInput(input)
}
