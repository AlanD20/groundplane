package backingservices

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedCreation struct {
	BoundHooks    *apiTypes.BackingHookConfiguration      `json:"bound_hooks,omitempty"`
	Request       apiTypes.BackingServiceCreate           `json:"request"`
	ProjectID     string                                  `json:"project_id"`
	EnvironmentID string                                  `json:"environment_id"`
	ServiceID     string                                  `json:"service_id"`
	Locator       idempotencyrecord.IdempotencyLocator    `json:"locator"`
	RequestSHA256 string                                  `json:"request_sha256"`
	Intent        idempotencyrecord.ProtectedIntentRecord `json:"intent"`
}

func (service *CreationService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(service.accepted, "backing-create", workexecution.ActionCreate,
			ids.KindService, service.prepareAcceptedCreation),
	}
}

func (service *CreationService) prepareAcceptedCreation(ctx context.Context, snapshot workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedCreation,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	if input.ServiceID != snapshot.Task.TargetID || input.ProjectID != snapshot.Task.Scope.ProjectID ||
		input.EnvironmentID != snapshot.Task.Scope.EnvironmentID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Backing creation accepted identity changed",
		)
	}
	adapter, ok := adapters.Get(input.Request.Adapter)
	if !ok {
		return taskdependencies.Prepared{}, errs.New(errs.KindValidationFailed, "Backing adapter is unavailable")
	}
	var spec adapters.CreationSpec
	var err error
	if adapter.Custom() {
		spec, err = adapters.CustomBackingCreationSpec(input.Request.Slug, input.Request.Image)
	} else {
		spec, err = adapters.BackingCreationSpec(input.Request.Adapter, input.Request.AdapterVersion, core.BackingAuthentication(input.Request.Authentication))
		if input.Request.Image != "" {
			spec.Image = input.Request.Image
		}
	}
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err = configureCreation(&spec, input.Request, adapter.Custom()); err != nil {
		return taskdependencies.Prepared{}, err
	}
	bytes, err := json.Marshal(input.Request)
	if err != nil {
		return taskdependencies.Prepared{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(bytes)
	bundle := core.BlueprintBundle{
		RootPath:       "backing-service.yaml",
		ComposeSources: []string{"backing-service.yaml"},
		Files:          []core.BlueprintFile{{Path: "backing-service.yaml", Content: bytes}},
	}
	seedValue, err := randomBackingCredential()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(seedValue)
	seed, err := sealBackingSecret(
		ctx,
		service.protector,
		ids.DeriveAt(ids.KindSecret, snapshot.Task.CreatedAt, snapshot.Task.ID, "backing-bootstrap-seed"),
		seedValue,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(seed.Ciphertext)
	stage, err := service.repository.ClaimBackingServiceCreationStage(ctx, etcd.BackingServiceCreationStage{
		BootstrapSeed: seed, Locator: input.Locator, RequestSHA256: input.RequestSHA256, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID,
		TaskID: snapshot.Task.ID, CreatedAt: snapshot.Task.CreatedAt}, snapshot.Revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(stage.Record.BootstrapSeed.Ciphertext)
	capturedRequest := input.Request
	capturedRequest.Hooks = input.BoundHooks
	return service.createBackingServiceFromStage(
		ctx,
		capturedRequest,
		bundle,
		stage,
		adapter,
		spec,
		input.Intent,
		snapshot,
		input.ServiceID,
		at,
	)
}
