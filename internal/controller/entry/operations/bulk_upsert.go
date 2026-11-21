package operations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryBulkUpsertRoute = "/entries/bulk"

type entryBulkUpsertService struct {
	desired  *entryDesiredMutationService
	accepted *acceptedaction.Submitter
}

func NewBulkUpsertService(
	desired *entryDesiredMutationService,
	accepted *acceptedaction.Submitter,
) (*entryBulkUpsertService, error) {
	if desired == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Entry bulk upsert service is not configured")
	}
	return &entryBulkUpsertService{desired: desired, accepted: accepted}, nil
}

func (service *entryBulkUpsertService) prepareAcceptedBulk(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request acceptedEntryBulkUpsert,
) (taskdependencies.Prepared, error) {
	defer clear(request.Intent.Ciphertext)
	defer func() {
		for _, entry := range request.Entries {
			clear(entry.Value)
		}
	}()
	input, err := request.input()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if accepted.Task.TargetID != input.environmentID || accepted.Task.Scope.EnvironmentID != input.environmentID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Entry Environment changed after acceptance",
		)
	}
	exposure, err := service.desired.work.ResolveEntryMutationExposure(ctx, input.environmentID, request.Bindings)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input.exposure, err = normalizeEntryExposure(exposure)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	locator := request.Locator
	environment, err := service.desired.repository.GetEnvironment(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.desired.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.ID != accepted.Task.Scope.ProjectID || project.Record.TenantID != accepted.Task.Scope.TenantID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Entry Environment ownership changed after acceptance",
		)
	}
	if _, err := service.desired.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if err := service.desired.validateExposure(ctx, input.environmentID, input.exposure); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := service.desired.repository.InitializeEnvironmentDesiredState(ctx, input.environmentID, locator, request.Intent, at); err != nil {
		return taskdependencies.Prepared{}, err
	}
	head, _, err := service.desired.repository.GetEnvironmentBlueprintHead(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	source, err := service.desired.repository.ReadEntryMutationSource(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if source.Desired.Revision != head.Revision || source.Desired.Record.RevisionID != head.Record.RevisionID {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Entry desired source changed")
	}
	return service.bulkEntryDefinitions(ctx, input, locator, request.Intent, source, project, environment, accepted, at)
}
