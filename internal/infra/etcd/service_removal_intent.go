package etcd

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceRemovalIntentPrefix = "/v1/records/service-removal-intents/"

// ServiceRemovalIntent owns one sealed desired candidate while the active
// Service and desired head remain public until Agent cleanup succeeds.
type ServiceRemovalIntent struct {
	TaskID                    string                         `json:"task_id"`
	EnvironmentID             string                         `json:"environment_id"`
	ServiceID                 string                         `json:"service_id"`
	ServiceName               string                         `json:"service_name"`
	ServiceRevision           int64                          `json:"service_revision"`
	RuntimeRevision           int64                          `json:"runtime_revision"`
	CurrentProjectionRevision int64                          `json:"current_projection_revision"`
	ExpectedHeadRevision      int64                          `json:"expected_head_revision"`
	Claim                     EnvironmentBlueprintStageClaim `json:"claim"`
	CurrentProjection         EnvironmentComposeProjection   `json:"current_projection"`
	CandidateProjection       EnvironmentComposeProjection   `json:"candidate_projection"`
	Status                    TaskStatus                     `json:"status"`
	CreatedAt                 time.Time                      `json:"created_at"`
	TerminalAt                *time.Time                     `json:"terminal_at,omitempty"`
}

func NewServiceRemovalIntent(
	taskID string,
	service Versioned[ServiceRecord],
	projection Versioned[EnvironmentComposeProjection],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	candidate EnvironmentComposeProjection,
	createdAt time.Time,
) (ServiceRemovalIntent, error) {
	intent := ServiceRemovalIntent{
		TaskID: taskID, EnvironmentID: service.Record.EnvironmentID,
		ServiceID: service.Record.Desired.ID, ServiceName: service.Record.Desired.Name,
		ServiceRevision: service.Revision, RuntimeRevision: serviceRuntimeRevision(service),
		CurrentProjectionRevision: projection.Revision,
		ExpectedHeadRevision:      expectedHeadRevision, Claim: claim,
		CurrentProjection:   cloneEnvironmentComposeProjection(projection.Record),
		CandidateProjection: cloneEnvironmentComposeProjection(candidate),
		Status:              TaskStatusPending, CreatedAt: createdAt,
	}
	if err := validateServiceRemovalIntent(intent); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return cloneServiceRemovalIntent(intent), nil
}

func serviceRemovalIntentKey(taskID string) string { return serviceRemovalIntentPrefix + taskID }

func (repository *ServiceRepository) GetServiceRemovalIntent(
	ctx context.Context,
	taskID string,
) (Versioned[ServiceRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ServiceRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return Versioned[ServiceRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service removal Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, serviceRemovalIntentKey(taskID))
	if err != nil {
		return Versioned[ServiceRemovalIntent]{}, false, err
	}
	if result == nil {
		return Versioned[ServiceRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Service removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[ServiceRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeServiceRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return Versioned[ServiceRemovalIntent]{}, false, corruptServiceRemovalIntent()
	}
	return Versioned[ServiceRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func terminalServiceRemovalIntent(
	intent ServiceRemovalIntent,
	status TaskStatus,
	at time.Time,
) (ServiceRemovalIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return ServiceRemovalIntent{}, errs.New(errs.KindStateConflict, "Service removal intent is not pending")
	}
	terminal := cloneServiceRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(at)
	if err := validateServiceRemovalIntent(terminal); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return terminal, nil
}

func validateServiceRemovalIntent(intent ServiceRemovalIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, intent.ServiceID) != nil ||
		intent.ServiceName == "" ||
		intent.ServiceRevision <= 0 ||
		intent.RuntimeRevision < 0 ||
		intent.CurrentProjectionRevision <= 0 ||
		intent.ExpectedHeadRevision <= 0 ||
		validateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
		intent.Claim.EnvironmentID != intent.EnvironmentID ||
		intent.Claim.RevisionID != intent.Claim.TaskID ||
		intent.Claim.BaselineHeadRevision != intent.ExpectedHeadRevision ||
		intent.Claim.SourceKind != EnvironmentBlueprintSourceMutation ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CurrentProjection.RevisionID == intent.CandidateProjection.RevisionID ||
		intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
		intent.CurrentProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
		intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration ||
		validateEnvironmentComposeProjection(intent.CurrentProjection) != nil ||
		validateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		validateTimestamp("Service removal created_at", intent.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal intent identity is invalid")
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Service removal intent has terminal time")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) ||
		validateTimestamp("Service removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal terminal state is invalid")
	}
	expected := cloneEnvironmentComposeProjection(intent.CurrentProjection)
	expected.RevisionID = intent.CandidateProjection.RevisionID
	expected.RenderGeneration = intent.CandidateProjection.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), intent.CandidateProjection.ComposeArtifact...)
	expected.NormalizedCompose = append([]byte(nil), intent.CandidateProjection.NormalizedCompose...)
	expected.DesiredServices = nil
	removedDesired := false
	for _, desired := range intent.CurrentProjection.DesiredServices {
		if desired.Desired.ID == intent.ServiceID && desired.Desired.Name == intent.ServiceName {
			removedDesired = true
			continue
		}
		expected.DesiredServices = append(expected.DesiredServices, desired)
	}
	expected.VolumeMounts = expected.VolumeMounts[:0]
	for _, mount := range intent.CurrentProjection.VolumeMounts {
		if mount.ServiceID != intent.ServiceID {
			expected.VolumeMounts = append(expected.VolumeMounts, mount)
		}
	}
	expected.ServiceDependencyPlans = expected.ServiceDependencyPlans.WithoutService(intent.ServiceName)
	delete(expected.ServiceExtensions, intent.ServiceName)
	if !removedDesired || !sameServiceRemovalProjection(expected, intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Service removal candidate projection changed")
	}
	return nil
}

func sameServiceRemovalProjection(left, right EnvironmentComposeProjection) bool {
	return left.EnvironmentID == right.EnvironmentID && left.RevisionID == right.RevisionID &&
		left.RenderGeneration == right.RenderGeneration && sameServiceRemovalBytes(left.ComposeArtifact, right.ComposeArtifact) &&
		sameServiceRemovalBytes(left.NormalizedCompose, right.NormalizedCompose) &&
		sameServiceRemovalBlueprintFiles(left.RuntimeFiles, right.RuntimeFiles) &&
		sameServiceRemovalServiceExtensions(left.ServiceExtensions, right.ServiceExtensions) &&
		sameServiceRemovalDesiredZones(left.DesiredZones, right.DesiredZones) &&
		sameServiceRemovalDesiredServices(left.DesiredServices, right.DesiredServices) &&
		sameServiceRemovalDesiredRoutes(left.DesiredRoutes, right.DesiredRoutes) &&
		sameServiceRemovalComparableSlices(left.Volumes, right.Volumes) &&
		sameServiceRemovalComparableSlices(left.VolumeMounts, right.VolumeMounts) &&
		sameServiceRemovalComponents(left.Components, right.Components) &&
		sameServiceRemovalEntries(left.Entries, right.Entries) &&
		sameServiceRemovalDependencyPlans(left.ServiceDependencyPlans, right.ServiceDependencyPlans)
}

func sameServiceRemovalDesiredZones(left, right []EnvironmentZoneProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID || left[index].Desired != right[index].Desired {
			return false
		}
	}
	return true
}

func sameServiceRemovalDesiredServices(left, right []EnvironmentServiceProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID ||
			left[index].BackingNetworkID != right[index].BackingNetworkID ||
			!sameServiceRemovalDesired(left[index].Desired, right[index].Desired) {
			return false
		}
	}
	return true
}

func sameServiceRemovalDesiredRoutes(left, right []EnvironmentRouteProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID ||
			left[index].Desired != right[index].Desired ||
			left[index].DesiredGeneration != right[index].DesiredGeneration {
			return false
		}
	}
	return true
}

func sameServiceRemovalDesired(left, right core.Service) bool {
	if left.ID != right.ID || left.Name != right.Name || left.Image != right.Image ||
		left.Strategy != right.Strategy || left.OnFailure != right.OnFailure ||
		left.Healthcheck != right.Healthcheck || left.Resources != right.Resources ||
		left.Restart != right.Restart || left.Logging != right.Logging || left.Replicas != right.Replicas ||
		left.Adapter != right.Adapter || left.FactsPrefix != right.FactsPrefix || left.Label != right.Label ||
		!sameServiceRemovalComparableSlices(left.Zones, right.Zones) ||
		!sameServiceRemovalComparableSlices(left.Command, right.Command) ||
		!sameServiceRemovalComparableSlices(left.Mounts, right.Mounts) ||
		!sameServiceRemovalComparableSlices(left.Expose, right.Expose) ||
		!sameServiceRemovalEnvEntries(left.Environment, right.Environment) ||
		!sameServiceRemovalStringSlices(left.Aliases, right.Aliases) ||
		!sameServiceRemovalDependencies(left.DependsOn, right.DependsOn) {
		return false
	}
	return true
}

func sameServiceRemovalEnvEntries(left, right []core.EnvEntry) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalEnvEntry(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sameServiceRemovalStringSlices(left, right map[string][]string) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for key, leftValues := range left {
		rightValues, exists := right[key]
		if !exists || !sameServiceRemovalComparableSlices(leftValues, rightValues) {
			return false
		}
	}
	return true
}

func sameServiceRemovalDependencies(left, right map[string]core.ServiceDependency) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for name, leftDependency := range left {
		rightDependency, exists := right[name]
		if !exists || leftDependency.Condition != rightDependency.Condition ||
			!sameServiceRemovalComparableSlices(leftDependency.Phases, rightDependency.Phases) {
			return false
		}
	}
	return true
}

func sameServiceRemovalBlueprintFiles(left, right []core.BlueprintFile) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Path != right[index].Path ||
			!sameServiceRemovalBytes(left[index].Content, right[index].Content) {
			return false
		}
	}
	return true
}

func sameServiceRemovalServiceExtensions(
	left,
	right map[string]core.ServiceExtensionSpec,
) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for name, leftExtension := range left {
		rightExtension, exists := right[name]
		if !exists || !sameServiceRemovalServiceExtension(leftExtension, rightExtension) {
			return false
		}
	}
	return true
}

func sameServiceRemovalServiceExtension(left, right core.ServiceExtensionSpec) bool {
	if left.Release == nil || right.Release == nil {
		if left.Release != nil || right.Release != nil {
			return false
		}
	} else if *left.Release != *right.Release {
		return false
	}
	if (left.DependsOn == nil) != (right.DependsOn == nil) || len(left.DependsOn) != len(right.DependsOn) {
		return false
	}
	for name, leftDependency := range left.DependsOn {
		rightDependency, exists := right.DependsOn[name]
		if !exists || leftDependency.Condition != rightDependency.Condition ||
			!sameServiceRemovalComparableSlices(leftDependency.Phases, rightDependency.Phases) {
			return false
		}
	}
	return true
}

func sameServiceRemovalBytes(left, right []byte) bool {
	return (left == nil) == (right == nil) && bytes.Equal(left, right)
}

func sameServiceRemovalComparableSlices[E comparable](left, right []E) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}

func sameServiceRemovalComponents(left, right []ComponentRecord) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalComponentRecord(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sameServiceRemovalEntries(left, right []EntryRecord) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalEntryRecord(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sameServiceRemovalDependencyPlans(left, right core.ServiceDependencyPlans) bool {
	return sameServiceRemovalDependencyPlan(left.DeployDependencyPlan, right.DeployDependencyPlan) &&
		sameServiceRemovalDependencyPlan(left.RollbackDependencyPlan, right.RollbackDependencyPlan)
}

func sameServiceRemovalDependencyPlan(left, right core.ServiceDependencyPhasePlan) bool {
	return left.Phase == right.Phase &&
		sameServiceRemovalComparableSlices(left.OrderedServices, right.OrderedServices) &&
		sameServiceRemovalComparableSlices(left.Edges, right.Edges)
}

func sameServiceRemovalComponentRecord(left, right ComponentRecord) bool {
	return left.Desired.ID == right.Desired.ID && left.Desired.Owner == right.Desired.Owner &&
		left.Desired.OwnerID == right.Desired.OwnerID && left.Desired.Kind == right.Desired.Kind &&
		left.Desired.Enabled == right.Desired.Enabled &&
		sameServiceRemovalComponentConfig(left.Desired.Config, right.Desired.Config) &&
		sameServiceRemovalComparableSlices(left.Runtime.GeneratedServices, right.Runtime.GeneratedServices) &&
		left.Runtime.PinnedIPv4 == right.Runtime.PinnedIPv4 && left.Runtime.Healthy == right.Runtime.Healthy
}

func sameServiceRemovalComponentConfig(left, right core.ComponentConfig) bool {
	if (left.Caddy == nil) != (right.Caddy == nil) ||
		(left.CloudflareTunnel == nil) != (right.CloudflareTunnel == nil) ||
		(left.CoreDNS == nil) != (right.CoreDNS == nil) {
		return false
	}
	if left.Caddy != nil && (left.Caddy.CaddyfileTemplate != right.Caddy.CaddyfileTemplate ||
		!slices.Equal(left.Caddy.ZoneIDs, right.Caddy.ZoneIDs)) {
		return false
	}
	if left.CloudflareTunnel != nil && (left.CloudflareTunnel.SecretID != right.CloudflareTunnel.SecretID ||
		!slices.Equal(left.CloudflareTunnel.ZoneIDs, right.CloudflareTunnel.ZoneIDs)) {
		return false
	}
	if left.CoreDNS == nil {
		return true
	}
	if left.CoreDNS.UpstreamAuto != right.CoreDNS.UpstreamAuto ||
		left.CoreDNS.TailnetDelegation != right.CoreDNS.TailnetDelegation ||
		!sameServiceRemovalComparableSlices(left.CoreDNS.UpstreamResolvers, right.CoreDNS.UpstreamResolvers) ||
		(left.CoreDNS.Forwarders == nil) != (right.CoreDNS.Forwarders == nil) ||
		len(left.CoreDNS.Forwarders) != len(right.CoreDNS.Forwarders) {
		return false
	}
	for index := range left.CoreDNS.Forwarders {
		leftForwarder := left.CoreDNS.Forwarders[index]
		rightForwarder := right.CoreDNS.Forwarders[index]
		if leftForwarder.Domain != rightForwarder.Domain ||
			!sameServiceRemovalComparableSlices(leftForwarder.Resolvers, rightForwarder.Resolvers) {
			return false
		}
	}
	return true
}

func sameServiceRemovalEntryRecord(left, right EntryRecord) bool {
	return left.EnvironmentID == right.EnvironmentID && left.BlueprintKey == right.BlueprintKey &&
		left.CurrentValueGenerationID == right.CurrentValueGenerationID &&
		sameServiceRemovalEnvEntry(left.Entry, right.Entry)
}

func sameServiceRemovalEnvEntry(left, right core.EnvEntry) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.Key == right.Key && left.Path == right.Path &&
		sameServiceRemovalOptionalUint32(left.UID, right.UID) &&
		sameServiceRemovalOptionalUint32(left.GID, right.GID) && left.Secret == right.Secret &&
		sameServiceRemovalEntrySource(left.Source, right.Source) &&
		sameServiceRemovalComparableSlices(left.Exposure, right.Exposure)
}

func sameServiceRemovalOptionalUint32(left, right *uint32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameServiceRemovalEntrySource(left, right core.EntrySource) bool {
	if left.Kind != right.Kind || left.Literal != right.Literal || left.SecretRef != right.SecretRef {
		return false
	}
	if left.Fact == nil || right.Fact == nil {
		return left.Fact == nil && right.Fact == nil
	}
	return *left.Fact == *right.Fact
}

func validateServiceRemovalTaskOwner(task TaskRecord, intent ServiceRemovalIntent) error {
	if task.ID != intent.TaskID || task.Executor != TaskExecutorAgent || task.Type != TaskRemove ||
		task.Target != intent.ServiceID || !task.CreatedAt.Equal(intent.CreatedAt) || len(task.Params) != 4 ||
		task.Params[TaskResourceKindParam] != TaskResourceService ||
		task.Params[TaskServiceEnvironmentParam] != intent.EnvironmentID ||
		task.Params[EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		task.Params[TaskComposeArtifactParam] == "" {
		return errs.New(errs.KindStateConflict, "Service removal intent does not belong to its Task")
	}
	return nil
}

func encodeServiceRemovalIntent(intent ServiceRemovalIntent) ([]byte, error) {
	if err := validateServiceRemovalIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("service_removal_intent", intent)
}

func decodeServiceRemovalIntent(value []byte) (ServiceRemovalIntent, error) {
	intent, err := decodeEnvelope[ServiceRemovalIntent](value, "service_removal_intent")
	if err != nil || validateServiceRemovalIntent(intent) != nil {
		return ServiceRemovalIntent{}, corruptServiceRemovalIntent()
	}
	return intent, nil
}

func cloneServiceRemovalIntent(intent ServiceRemovalIntent) ServiceRemovalIntent {
	intent.Claim.Intent.Ciphertext = append([]byte(nil), intent.Claim.Intent.Ciphertext...)
	intent.CurrentProjection = cloneEnvironmentComposeProjection(intent.CurrentProjection)
	intent.CandidateProjection = cloneEnvironmentComposeProjection(intent.CandidateProjection)
	intent.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return intent
}

func corruptServiceRemovalIntent() error {
	return errs.New(errs.KindInternal, "Service removal intent is corrupt")
}
