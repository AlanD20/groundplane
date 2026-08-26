package etcd

import (
	"context"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumAttachTaskRenderInputBytes = 256 << 10

type AttachTaskNetworkJoin struct {
	NetworkID  string   `json:"network_id"`
	ServiceIDs []string `json:"service_ids"`
}

// AttachTaskRenderInput is the immutable, non-secret desired-state projection
// needed to reproduce an Attach plan after restart. Retries retain PlanID and
// therefore consume the exact same input without reading mutable topology.
type AttachTaskRenderInput struct {
	PlanID              string                       `json:"plan_id"`
	AttachID            string                       `json:"attach_id"`
	AttachName          string                       `json:"attach_name"`
	TenantID            string                       `json:"tenant_id"`
	TenantSlug          string                       `json:"tenant_slug"`
	ProjectID           string                       `json:"project_id"`
	ProjectSlug         string                       `json:"project_slug"`
	EnvironmentID       string                       `json:"environment_id"`
	EnvironmentName     string                       `json:"environment_name"`
	AuthorizedVolumeDir string                       `json:"authorized_volume_dir"`
	BackingServiceID    string                       `json:"backing_service_id"`
	BackingProjectID    string                       `json:"backing_project_id"`
	AdapterKey          string                       `json:"adapter_key"`
	BlueprintRevisionID string                       `json:"blueprint_revision_id"`
	ArtifactID          string                       `json:"artifact_id"`
	RenderGeneration    uint64                       `json:"render_generation"`
	Services            []EnvironmentComposeIdentity `json:"services"`
	Networks            []EnvironmentComposeIdentity `json:"networks,omitempty"`
	Volumes             []EnvironmentVolumeIdentity  `json:"volumes,omitempty"`
	NetworkJoins        []AttachTaskNetworkJoin      `json:"network_joins"`
	ConsumerServiceIDs  []string                     `json:"consumer_service_ids"`
	GrantAttachIDs      []string                     `json:"grant_attach_ids,omitempty"`
	core.ServiceDependencyPlans
}

func (repository *AttachRepository) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (Versioned[AttachTaskRenderInput], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[AttachTaskRenderInput]{}, err
	}
	if err := validateID(ids.KindPlan, planID); err != nil {
		return Versioned[AttachTaskRenderInput]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		attachTaskRenderInputKey(planID),
		planID,
		errs.KindTaskNotFound,
		decodeAttachTaskRenderInput,
		func(record AttachTaskRenderInput) string { return record.PlanID },
	)
}

func attachTaskRenderInputKey(planID string) string {
	return "/v1/records/attach-task-render-inputs/" + planID
}

func encodeAttachTaskRenderInput(input AttachTaskRenderInput) ([]byte, error) {
	if err := validateAttachTaskRenderInput(input); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope("attach-task-render-input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumAttachTaskRenderInputBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "Attach Task render input exceeds the durable size limit")
	}
	return value, nil
}

func decodeAttachTaskRenderInput(value []byte) (AttachTaskRenderInput, error) {
	input, err := decodeEnvelope[AttachTaskRenderInput](value, "attach-task-render-input")
	if err != nil {
		return AttachTaskRenderInput{}, err
	}
	if err := validateAttachTaskRenderInput(input); err != nil {
		return AttachTaskRenderInput{}, corruptAttachTaskRenderInput()
	}
	return input, nil
}

func validateAttachTaskRenderInput(input AttachTaskRenderInput) error {
	if validateStableID(ids.KindPlan, input.PlanID) != nil ||
		validateStableID(ids.KindAttach, input.AttachID) != nil ||
		validateLabel("Attach name", input.AttachName) != nil ||
		validateStableID(ids.KindTenant, input.TenantID) != nil ||
		validateStableID(ids.KindProject, input.ProjectID) != nil ||
		validateStableID(ids.KindEnvironment, input.EnvironmentID) != nil ||
		validateStableID(ids.KindService, input.BackingServiceID) != nil ||
		validateStableID(ids.KindProject, input.BackingProjectID) != nil ||
		validateStableID(ids.KindTask, input.BlueprintRevisionID) != nil ||
		validateStableID(ids.KindConfig, input.ArtifactID) != nil || input.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Attach Task render input identity is invalid")
	}
	if !validAttachTaskRenderLabel(input.TenantSlug) || !validAttachTaskRenderLabel(input.ProjectSlug) ||
		!validAttachTaskRenderLabel(input.EnvironmentName) ||
		!validAttachTaskRenderLabel(input.AdapterKey) ||
		!strings.HasPrefix(input.AuthorizedVolumeDir, "/") ||
		strings.IndexByte(input.AuthorizedVolumeDir, 0) >= 0 {
		return errs.New(errs.KindValidationFailed, "Attach Task render input hierarchy is invalid")
	}
	if err := validateEnvironmentComposeIdentities(ids.KindService, input.Services); err != nil {
		return err
	}
	if len(input.Services) == 0 {
		return errs.New(errs.KindValidationFailed, "Attach Task render input has no consumer Services")
	}
	names := make([]string, len(input.Services))
	for index, service := range input.Services {
		names[index] = service.Name
	}
	if err := input.ServiceDependencyPlans.Validate(names); err != nil {
		return err
	}
	if len(input.ConsumerServiceIDs) == 0 ||
		validateSortedStableIDs(input.ConsumerServiceIDs, ids.KindService, "Attach consumer service_ids") != nil ||
		validateSortedStableIDs(input.GrantAttachIDs, ids.KindAttach, "Attach grant_attach_ids") != nil {
		return errs.New(errs.KindValidationFailed, "attach task removal evidence is invalid or unsorted")
	}
	serviceIDs := make(map[string]struct{}, len(input.Services))
	for _, service := range input.Services {
		serviceIDs[service.ID] = struct{}{}
	}
	for _, serviceID := range input.ConsumerServiceIDs {
		if _, exists := serviceIDs[serviceID]; !exists {
			return errs.New(errs.KindValidationFailed, "attach task removal evidence references an unknown service")
		}
	}
	if err := validateEnvironmentComposeIdentities(ids.KindNetwork, input.Networks); err != nil {
		return err
	}
	if err := validateEnvironmentVolumeIdentities(input.Volumes); err != nil {
		return err
	}
	ownedNetworkIDs := make(map[string]struct{}, len(input.Networks))
	for _, network := range input.Networks {
		ownedNetworkIDs[network.ID] = struct{}{}
	}
	previousNetworkID := ""
	for _, join := range input.NetworkJoins {
		if validateStableID(ids.KindNetwork, join.NetworkID) != nil || join.NetworkID <= previousNetworkID ||
			len(join.ServiceIDs) == 0 {
			return errs.New(errs.KindValidationFailed, "Attach Task network joins are invalid or unsorted")
		}
		if _, owned := ownedNetworkIDs[join.NetworkID]; owned {
			return errs.New(
				errs.KindValidationFailed,
				"Attach Task external network is owned by the consumer Environment",
			)
		}
		previousServiceID := ""
		for _, serviceID := range join.ServiceIDs {
			if validateStableID(ids.KindService, serviceID) != nil || serviceID <= previousServiceID {
				return errs.New(errs.KindValidationFailed, "Attach Task network Service ids are invalid or unsorted")
			}
			if _, exists := serviceIDs[serviceID]; !exists {
				return errs.New(errs.KindValidationFailed, "Attach Task network references an unknown consumer Service")
			}
			previousServiceID = serviceID
		}
		previousNetworkID = join.NetworkID
	}
	return nil
}

func validateAttachTaskRenderInputScope(
	scope AttachCreateScope,
	record AttachRecord,
	task TaskRecord,
	input AttachTaskRenderInput,
) error {
	if scope.Tenant.Revision <= 0 || scope.BlueprintRevision.Revision <= 0 || scope.ComposeProjection.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach render scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.BlueprintRevision.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.RevisionID != scope.BlueprintRevision.Record.RevisionID {
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
		input.BlueprintRevisionID != scope.BlueprintRevision.Record.RevisionID ||
		input.RenderGeneration != scope.ComposeProjection.Record.RenderGeneration ||
		input.RenderGeneration != uint64(task.RenderGeneration) ||
		!slices.Equal(input.Services, scope.ComposeProjection.Record.Services) ||
		!slices.Equal(input.Networks, scope.ComposeProjection.Record.Networks) ||
		!slices.Equal(input.Volumes, scope.ComposeProjection.Record.Volumes) ||
		!equalServiceDependencyPlans(
			input.ServiceDependencyPlans,
			scope.ComposeProjection.Record.ServiceDependencyPlans,
		) ||
		!slices.Equal(input.ConsumerServiceIDs, record.ServiceIDs) ||
		!slices.Equal(input.GrantAttachIDs, record.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "Attach Task render input does not match its pinned desired state")
	}
	if task.Type == TaskAttach {
		coveredByBackingNetwork := make(map[string]struct{})
		for _, join := range input.NetworkJoins {
			if join.NetworkID != record.BackingNetworkID {
				continue
			}
			for _, serviceID := range join.ServiceIDs {
				coveredByBackingNetwork[serviceID] = struct{}{}
			}
		}
		for _, serviceID := range record.ServiceIDs {
			if _, exists := coveredByBackingNetwork[serviceID]; !exists {
				return errs.New(
					errs.KindValidationFailed,
					"Attach Task render input omits a consumer Service binding to the backing network",
				)
			}
		}
	}
	return validateAttachTaskRenderInput(input)
}

func equalServiceDependencyPlans(left, right core.ServiceDependencyPlans) bool {
	return equalServiceDependencyPhasePlan(left.DeployDependencyPlan, right.DeployDependencyPlan) &&
		equalServiceDependencyPhasePlan(left.RollbackDependencyPlan, right.RollbackDependencyPlan)
}

func equalServiceDependencyPhasePlan(left, right core.ServiceDependencyPhasePlan) bool {
	return left.Phase == right.Phase &&
		slices.Equal(left.OrderedServices, right.OrderedServices) &&
		slices.Equal(left.Edges, right.Edges)
}

func validAttachTaskRenderLabel(value string) bool {
	return value != "" && len(value) <= 255 && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func corruptAttachTaskRenderInput() error {
	return errs.New(errs.KindInternal, "Attach Task render input is corrupt")
}
