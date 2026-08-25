// Package executionplan owns validation and deterministic hashing for the
// protobuf procedure shared by the Controller and Agent.
package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	SchemaVersion                        = 1
	MaximumArtifacts                     = 16
	MaximumArtifactYAMLBytes             = 1024 * 1024
	MaximumPlanBytes                     = 4 * 1024 * 1024
	MaximumAdapterKeyBytes               = 64
	MaximumAdapterSecretBytes            = 256
	MaximumBackupSources                 = 12
	MaximumBackupPrunePoints             = 11
	MaximumBackupPruneStepTimeoutSeconds = 30 * 60
	maximumComposeNameBytes              = 255
	maximumAdapterIdentityBytes          = 63
	maximumBackupObjectKeyBytes          = 1024
)

const (
	labelEnvironmentID = "com.groundplane.environment-id"
	labelKind          = "com.groundplane.kind"
	labelManaged       = "com.groundplane.managed"
	labelPlanID        = "com.groundplane.plan-id"
	labelProjectID     = "com.groundplane.project-id"
	labelReleaseID     = "com.groundplane.release-id"
	labelRenderGen     = "com.groundplane.render-generation"
	labelServiceID     = "com.groundplane.service-id"
	labelSlot          = "com.groundplane.slot"
	labelTenantID      = "com.groundplane.tenant-id"
)

// Seal validates an unhashed plan, computes its canonical digest, and returns
// an owned plan. The caller's message is never mutated.
func Seal(plan *agentpb.ExecutionPlan) (*agentpb.ExecutionPlan, error) {
	if plan == nil || len(plan.PlanHash) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "execution plan must be unhashed before sealing")
	}
	owned := proto.Clone(plan).(*agentpb.ExecutionPlan)
	if err := validateShape(owned); err != nil {
		return nil, err
	}
	encoded, err := marshalHashInput(owned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	owned.PlanHash = append([]byte(nil), digest[:]...)
	return owned, nil
}

// Validate verifies structure, bounds, unknown-field exclusion, and the
// deterministic digest, then returns an owned plan.
func Validate(plan *agentpb.ExecutionPlan) (*agentpb.ExecutionPlan, error) {
	if plan == nil || len(plan.PlanHash) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "execution plan hash must contain 32 bytes")
	}
	owned := proto.Clone(plan).(*agentpb.ExecutionPlan)
	if err := validateShape(owned); err != nil {
		return nil, err
	}
	encoded, err := marshalHashInput(owned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	if subtle.ConstantTimeCompare(owned.PlanHash, digest[:]) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "execution plan hash does not match its contents")
	}
	return owned, nil
}

// AuthorizeVolumeDirectories applies daemon-owned host policy after generic
// plan validation. The trusted root never enters the sealed plan or its hash.
func AuthorizeVolumeDirectories(plan *agentpb.ExecutionPlan, volumeRoot string) error {
	if plan == nil {
		return errs.New(errs.KindValidationFailed, "execution plan is required")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return err
	}
	for _, artifact := range plan.Artifacts {
		if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
			continue
		}
		scope, err := environmentpath.Parse(volumeRoot, artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != artifact.OwnerId {
			return errs.New(
				errs.KindValidationFailed,
				"authorized environment volume directory is outside its trusted owner policy",
			)
		}
	}
	for _, step := range plan.Steps {
		create := step.GetEnvironmentDirectoryCreate()
		if create != nil {
			scope, err := environmentpath.Parse(volumeRoot, create.ExpectedVolumeDir)
			if err != nil || scope.EnvironmentID != create.EnvironmentId || create.EnvironmentId != plan.TargetId {
				return errs.New(
					errs.KindValidationFailed,
					"environment directory is outside its trusted owner policy",
				)
			}
		}
		remove := step.GetEnvironmentDirectoryRemove()
		if remove != nil {
			scope, err := environmentpath.Parse(volumeRoot, remove.ExpectedVolumeDir)
			if err != nil || scope.EnvironmentID != remove.EnvironmentId || remove.EnvironmentId != plan.TargetId {
				return errs.New(
					errs.KindValidationFailed,
					"environment directory removal is outside its trusted owner policy",
				)
			}
		}
	}
	return nil
}

// RejectUnknown rejects unknown fields and map values recursively in a
// versioned execution message before its semantic decoder uses it.
func RejectUnknown(message proto.Message) error {
	if message == nil {
		return errs.New(errs.KindValidationFailed, "execution message is required")
	}
	return rejectUnknown(message.ProtoReflect())
}

func marshalHashInput(plan *agentpb.ExecutionPlan) ([]byte, error) {
	hashInput := proto.Clone(plan).(*agentpb.ExecutionPlan)
	hashInput.PlanHash = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(hashInput)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) > MaximumPlanBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"execution plan exceeds the %d-byte machine-message limit",
			MaximumPlanBytes,
		)
	}
	return encoded, nil
}

func validateShape(plan *agentpb.ExecutionPlan) error {
	if err := rejectUnknown(plan.ProtoReflect()); err != nil {
		return err
	}
	if plan.Schema != SchemaVersion {
		return errs.New(errs.KindValidationFailed, "execution plan schema is unsupported")
	}
	if err := validateID(ids.KindPlan, plan.PlanId); err != nil {
		return err
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP ||
		plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
		if plan.RenderGeneration != 0 {
			return errs.New(errs.KindValidationFailed, "backup execution plan cannot carry a render generation")
		}
	} else if plan.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "execution plan render generation must be positive")
	}
	if !validOperation(plan.Operation) {
		return errs.New(errs.KindValidationFailed, "execution plan operation is unsupported")
	}
	if err := validateAnyStableID(plan.TargetId); err != nil {
		return errs.New(errs.KindValidationFailed, "execution plan target id is invalid")
	}
	if (plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH ||
		plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH) &&
		validateID(ids.KindAttach, plan.TargetId) != nil {
		return errs.New(errs.KindValidationFailed, "adapter execution plan target must be an Attach")
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE {
		return validateEnvironmentCreatePlan(plan)
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP {
		return validateBackupPlan(plan)
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
		return validateBackupPrunePlan(plan)
	}
	if len(plan.Artifacts) == 0 && (plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH ||
		plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH) {
		return validateArtifactFreeAdapterPlan(plan)
	}
	if len(plan.Artifacts) == 0 && validateID(ids.KindNetwork, plan.TargetId) == nil {
		return validateArtifactFreeManagedNetworkRemovePlan(plan)
	}
	if len(plan.Artifacts) == 0 {
		return validateArtifactFreeEnvironmentRemovePlan(plan)
	}
	if len(plan.Artifacts) > MaximumArtifacts {
		return errs.Newf(
			errs.KindValidationFailed,
			"execution plan must contain between 1 and %d Compose artifacts",
			MaximumArtifacts,
		)
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		if err := validateArtifact(plan, artifact); err != nil {
			return err
		}
		if _, duplicate := artifacts[artifact.ArtifactId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan artifact ids must be unique")
		}
		artifacts[artifact.ArtifactId] = artifact
	}
	if len(plan.Steps) == 0 {
		return errs.New(errs.KindValidationFailed, "execution plan must contain at least one step")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	materializationIDs := make(map[string]struct{})
	materializationDestinations := make(map[string]struct{})
	for _, step := range plan.Steps {
		if err := validateStep(plan.Operation, plan.RenderGeneration, step, artifacts); err != nil {
			return err
		}
		if procedure := step.GetAdapterProcedure(); procedure != nil && procedure.AttachId != plan.TargetId {
			return errs.New(errs.KindValidationFailed, "adapter procedure does not identify the plan target")
		}
		if remove := step.GetEnvironmentDirectoryRemove(); remove != nil && remove.EnvironmentId != plan.TargetId {
			return errs.New(
				errs.KindValidationFailed,
				"environment directory removal does not identify the plan target",
			)
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
		if materialization := step.GetMaterializeFile(); materialization != nil {
			if _, duplicate := materializationIDs[materialization.MaterializationId]; duplicate {
				return errs.New(errs.KindValidationFailed, "materialization ids must be unique within a plan")
			}
			materializationIDs[materialization.MaterializationId] = struct{}{}
			destinationKey := materialization.ArtifactId + "\x00" + materialization.Destination
			if _, duplicate := materializationDestinations[destinationKey]; duplicate {
				return errs.New(
					errs.KindValidationFailed,
					"materialization destinations must be unique within an artifact",
				)
			}
			materializationDestinations[destinationKey] = struct{}{}
		}
	}
	return nil
}

func validateBackupPlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Artifacts) != 0 ||
		len(plan.Steps) == 0 || len(plan.Steps) > MaximumBackupSources {
		return errs.New(errs.KindValidationFailed, "backup execution plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	sourceIDs := make(map[string]struct{}, len(plan.Steps))
	pointIDs := make(map[string]struct{}, len(plan.Steps))
	var fixed *agentpb.BackupSourceCapture
	for _, step := range plan.Steps {
		if step == nil || validateID(ids.KindStep, step.StepId) != nil || step.TimeoutSeconds == 0 {
			return errs.New(errs.KindValidationFailed, "backup execution step identity or timeout is invalid")
		}
		capture := step.GetBackupSourceCapture()
		if err := validateBackupSourceCapture(plan.TargetId, capture); err != nil {
			return err
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		if _, duplicate := sourceIDs[capture.SourceId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup source ids must be unique")
		}
		if _, duplicate := pointIDs[capture.PointId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup point ids must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
		sourceIDs[capture.SourceId] = struct{}{}
		pointIDs[capture.PointId] = struct{}{}
		if fixed == nil {
			fixed = capture
			continue
		}
		if capture.ConnectorId != fixed.ConnectorId || capture.ConnectorRevision != fixed.ConnectorRevision ||
			capture.Encryption != fixed.Encryption || capture.KeyEra != fixed.KeyEra ||
			capture.AgeRecipient != fixed.AgeRecipient {
			return errs.New(errs.KindValidationFailed, "backup plan policy controls must be identical across sources")
		}
	}
	return nil
}

func validateBackupPrunePlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Artifacts) != 0 ||
		len(plan.Steps) == 0 || len(plan.Steps) > MaximumBackupPrunePoints {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	pointIDs := make(map[string]struct{}, len(plan.Steps))
	objectKeys := make(map[string]struct{}, len(plan.Steps))
	pruneOperationID := ""
	for index, step := range plan.Steps {
		if step == nil || validateID(ids.KindStep, step.StepId) != nil ||
			step.TimeoutSeconds != MaximumBackupPruneStepTimeoutSeconds {
			return errs.New(errs.KindValidationFailed, "backup prune step identity or timeout is invalid")
		}
		prune := step.GetBackupArtifactPrune()
		if err := validateBackupArtifactPrune(plan.TargetId, uint32(index+1), prune); err != nil {
			return err
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune step ids must be unique")
		}
		if _, duplicate := pointIDs[prune.PointId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune point ids must be unique")
		}
		if _, duplicate := objectKeys[prune.ProtectedObjectKey]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune object keys must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
		pointIDs[prune.PointId] = struct{}{}
		objectKeys[prune.ProtectedObjectKey] = struct{}{}
		if pruneOperationID == "" {
			pruneOperationID = prune.PruneOperationId
		} else if prune.PruneOperationId != pruneOperationID {
			return errs.New(errs.KindValidationFailed, "backup prune operation must be identical across points")
		}
	}
	return nil
}

func validateArtifactFreeAdapterPlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindAttach, plan.TargetId) != nil || len(plan.Steps) == 0 {
		return errs.New(errs.KindValidationFailed, "artifact-free adapter plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	for _, step := range plan.Steps {
		if err := validateStep(plan.Operation, plan.RenderGeneration, step, nil); err != nil {
			return err
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil || procedure.AttachId != plan.TargetId {
			return errs.New(errs.KindValidationFailed, "artifact-free adapter plan target is invalid")
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
	}
	return nil
}

func validateArtifactFreeEnvironmentRemovePlan(plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "artifact-free Environment remove plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan.Operation, plan.RenderGeneration, step, nil); err != nil {
		return err
	}
	remove := step.GetEnvironmentDirectoryRemove()
	if remove == nil || remove.EnvironmentId != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "artifact-free Environment remove plan target is invalid")
	}
	return nil
}

func validateArtifactFreeManagedNetworkRemovePlan(plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		validateID(ids.KindNetwork, plan.TargetId) != nil || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "artifact-free managed network remove plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan.Operation, plan.RenderGeneration, step, nil); err != nil {
		return err
	}
	remove := step.GetManagedNetworkRemove()
	if remove == nil || remove.NetworkId != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "managed network removal does not identify the plan target")
	}
	return nil
}

func validateEnvironmentCreatePlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil {
		return errs.New(errs.KindValidationFailed, "environment create target id is invalid")
	}
	if len(plan.Artifacts) != 0 || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "environment create plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan.Operation, plan.RenderGeneration, step, nil); err != nil {
		return err
	}
	create := step.GetEnvironmentDirectoryCreate()
	if create.GetEnvironmentId() != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "environment directory does not identify the plan target")
	}
	return validateVolumeDirectory(create.GetExpectedVolumeDir(), create.GetEnvironmentId())
}

func validateArtifact(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	if artifact == nil {
		return errs.New(errs.KindValidationFailed, "execution plan contains an empty artifact")
	}
	if err := validateID(ids.KindConfig, artifact.ArtifactId); err != nil {
		return err
	}
	switch artifact.OwnerKind {
	case agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT:
		if err := validateID(ids.KindEnvironment, artifact.OwnerId); err != nil {
			return err
		}
		if artifact.ProjectName != "gp-"+strings.ToLower(artifact.OwnerId) {
			return errs.New(errs.KindValidationFailed, "environment Compose project name is not id-derived")
		}
		if err := validateVolumeDirectory(artifact.AuthorizedVolumeDir, artifact.OwnerId); err != nil {
			return err
		}
	case agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM:
		if artifact.OwnerId != "" || artifact.ProjectName != "groundplane-infra" || artifact.AuthorizedVolumeDir != "" {
			return errs.New(errs.KindValidationFailed, "platform Compose artifact identity is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Compose artifact owner kind is unsupported")
	}
	if len(artifact.CanonicalYaml) == 0 || len(artifact.CanonicalYaml) > MaximumArtifactYAMLBytes ||
		!utf8.Valid(artifact.CanonicalYaml) {
		return errs.New(errs.KindValidationFailed, "Compose artifact YAML is empty, oversized, or invalid UTF-8")
	}
	if len(artifact.YamlSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Compose artifact digest must contain 32 bytes")
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if subtle.ConstantTimeCompare(artifact.YamlSha256, digest[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "Compose artifact digest does not match its YAML")
	}
	if strings.Contains(string(artifact.CanonicalYaml), "${") {
		return errs.New(errs.KindValidationFailed, "Compose artifact contains unresolved interpolation")
	}
	if err := validateServices(plan, artifact); err != nil {
		return err
	}
	if err := validateNetworks(plan, artifact); err != nil {
		return err
	}
	return validateVolumes(plan, artifact)
}

func validateServices(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	for _, service := range artifact.Services {
		if service == nil || (validateID(ids.KindService, service.ServiceId) != nil &&
			validateID(ids.KindComponent, service.ServiceId) != nil) {
			return errs.New(errs.KindValidationFailed, "Compose artifact service id is invalid")
		}
		if previous >= service.ServiceId {
			return errs.New(errs.KindValidationFailed, "Compose artifact services must be uniquely sorted by id")
		}
		previous = service.ServiceId
		if err := validateComposeName(service.ComposeName); err != nil {
			return err
		}
		if service.ExpectedReplicas == 0 {
			return errs.New(errs.KindValidationFailed, "Compose service expected replicas must be positive")
		}
		if err := validateLabels(plan, artifact, "service", service.ServiceId, service.ExpectedLabels); err != nil {
			return err
		}
	}
	return nil
}

func validateNetworks(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	for _, network := range artifact.Networks {
		if network == nil || validateID(ids.KindNetwork, network.NetworkId) != nil {
			return errs.New(errs.KindValidationFailed, "Compose artifact network id is invalid")
		}
		if previous >= network.NetworkId {
			return errs.New(errs.KindValidationFailed, "Compose artifact networks must be uniquely sorted by id")
		}
		previous = network.NetworkId
		if err := validateComposeName(network.ComposeName); err != nil {
			return err
		}
		if err := validateComposeName(network.DockerName); err != nil {
			return err
		}
		if err := validateLabels(plan, artifact, "network", network.NetworkId, network.ExpectedLabels); err != nil {
			return err
		}
	}
	return nil
}

func validateVolumes(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	for _, volume := range artifact.Volumes {
		if volume == nil || validateID(ids.KindVolume, volume.VolumeId) != nil {
			return errs.New(errs.KindValidationFailed, "Compose artifact volume id is invalid")
		}
		if previous >= volume.VolumeId {
			return errs.New(errs.KindValidationFailed, "Compose artifact volumes must be uniquely sorted by id")
		}
		previous = volume.VolumeId
		if err := validateComposeName(volume.ComposeName); err != nil {
			return err
		}
		if err := validateComposeName(volume.DockerName); err != nil {
			return err
		}
		if err := validateLabels(plan, artifact, "volume", volume.VolumeId, volume.ExpectedLabels); err != nil {
			return err
		}
	}
	return nil
}

func validateLabels(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	resourceKind string,
	resourceID string,
	pairs []*agentpb.LabelPair,
) error {
	values := make(map[string]string, len(pairs))
	previous := ""
	for _, pair := range pairs {
		if pair == nil || pair.Key <= previous || !validLabelKey(pair.Key) || !utf8.ValidString(pair.Value) {
			return errs.New(errs.KindValidationFailed, "expected ownership labels are invalid or unsorted")
		}
		previous = pair.Key
		values[pair.Key] = pair.Value
	}
	if values[labelManaged] != "true" || values[labelKind] != resourceKind ||
		values[labelPlanID] != plan.PlanId ||
		values[labelRenderGen] != strconv.FormatUint(plan.RenderGeneration, 10) {
		return errs.New(errs.KindValidationFailed, "expected ownership labels do not identify the current plan")
	}
	if resourceKind == "service" && values[labelServiceID] != resourceID {
		return errs.New(errs.KindValidationFailed, "expected service labels do not identify the service")
	}
	if artifact.OwnerKind == agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT &&
		values[labelEnvironmentID] != artifact.OwnerId {
		return errs.New(errs.KindValidationFailed, "expected ownership labels do not identify the environment")
	}
	for key, value := range values {
		var kind ids.Kind
		switch key {
		case labelTenantID:
			kind = ids.KindTenant
		case labelProjectID:
			kind = ids.KindProject
		case labelEnvironmentID:
			kind = ids.KindEnvironment
		case labelServiceID:
			if validateID(ids.KindService, value) == nil || validateID(ids.KindComponent, value) == nil {
				continue
			}
			return errs.New(errs.KindValidationFailed, "expected service label is invalid")
		case labelReleaseID:
			kind = ids.KindDeployment
		case labelSlot:
			if value == "blue" || value == "green" {
				continue
			}
			return errs.New(errs.KindValidationFailed, "expected slot label is invalid")
		default:
			continue
		}
		if err := validateID(kind, value); err != nil {
			return err
		}
	}
	return nil
}

func validateStep(
	operation agentpb.PlanOperation,
	renderGeneration uint64,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if step == nil || validateID(ids.KindStep, step.StepId) != nil || step.TimeoutSeconds == 0 {
		return errs.New(errs.KindValidationFailed, "execution step identity or timeout is invalid")
	}
	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		if payload.ComposeApply == nil {
			return errs.New(errs.KindValidationFailed, "Compose apply payload is empty")
		}
		selected := payload.ComposeApply.ServiceIds
		if payload.ComposeApply.FullReconcile == (len(selected) != 0) {
			return errs.New(errs.KindValidationFailed, "Compose apply selection is inconsistent")
		}
		return validateSelection(payload.ComposeApply.ArtifactId, selected, artifacts, false)
	case *agentpb.ExecutionStep_ComposeStop:
		if payload.ComposeStop == nil || payload.ComposeStop.GraceSeconds == 0 ||
			payload.ComposeStop.GraceSeconds > 300 {
			return errs.New(errs.KindValidationFailed, "Compose stop payload is invalid")
		}
		return validateSelection(payload.ComposeStop.ArtifactId, payload.ComposeStop.ServiceIds, artifacts, true)
	case *agentpb.ExecutionStep_ComposeRemove:
		if payload.ComposeRemove == nil || payload.ComposeRemove.WholeProject ==
			(len(payload.ComposeRemove.ServiceIds) != 0) {
			return errs.New(errs.KindValidationFailed, "Compose remove selection is inconsistent")
		}
		if payload.ComposeRemove.WholeProject && operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE {
			return errs.New(errs.KindValidationFailed, "whole-project removal requires a remove operation")
		}
		return validateSelection(
			payload.ComposeRemove.ArtifactId,
			payload.ComposeRemove.ServiceIds,
			artifacts,
			!payload.ComposeRemove.WholeProject,
		)
	case *agentpb.ExecutionStep_WaitHealthy:
		if payload.WaitHealthy == nil {
			return errs.New(errs.KindValidationFailed, "health wait payload is empty")
		}
		return validateSelection(payload.WaitHealthy.ArtifactId, payload.WaitHealthy.ServiceIds, artifacts, true)
	case *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
			payload.EnvironmentDirectoryCreate == nil ||
			validateID(ids.KindEnvironment, payload.EnvironmentDirectoryCreate.EnvironmentId) != nil {
			return errs.New(errs.KindValidationFailed, "environment directory create payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_EnvironmentDirectoryRemove:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
			payload.EnvironmentDirectoryRemove == nil ||
			validateID(ids.KindEnvironment, payload.EnvironmentDirectoryRemove.EnvironmentId) != nil {
			return errs.New(errs.KindValidationFailed, "environment directory remove payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure:
		if payload.ManagedVolumeDirectoriesEnsure == nil || !operationCreatesManagedVolumes(operation) {
			return errs.New(errs.KindValidationFailed, "managed volume directory ensure payload is invalid")
		}
		artifact := artifacts[payload.ManagedVolumeDirectoriesEnsure.ArtifactId]
		if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			len(artifact.Volumes) == 0 {
			return errs.New(errs.KindValidationFailed, "managed volume directory artifact is invalid")
		}
		composeNames := make(map[string]struct{}, len(artifact.Volumes))
		for _, volume := range artifact.Volumes {
			if _, duplicate := composeNames[volume.ComposeName]; duplicate {
				return errs.New(errs.KindValidationFailed, "managed volume directory names must be unique")
			}
			composeNames[volume.ComposeName] = struct{}{}
		}
		return nil
	case *agentpb.ExecutionStep_MaterializeFile:
		return validateMaterializeFile(renderGeneration, payload.MaterializeFile, artifacts)
	case *agentpb.ExecutionStep_AdapterProcedure:
		return validateAdapterProcedure(operation, payload.AdapterProcedure)
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		remove := payload.ManagedNetworkRemove
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || remove == nil ||
			validateID(ids.KindNetwork, remove.NetworkId) != nil ||
			validateID(ids.KindEnvironment, remove.EnvironmentId) != nil ||
			remove.DockerName != "gp_net_"+strings.ToLower(remove.NetworkId) {
			return errs.New(errs.KindValidationFailed, "managed network remove payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_CaddyConfigApply:
		apply := payload.CaddyConfigApply
		if apply == nil ||
			(operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
				operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
				operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE) ||
			validateID(ids.KindService, apply.ServiceId) != nil ||
			len(apply.CaddyfileSha256) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "Caddy config apply payload is invalid")
		}
		artifact := artifacts[apply.ArtifactId]
		if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.AuthorizedVolumeDir == "" {
			return errs.New(errs.KindValidationFailed, "Caddy config apply artifact is invalid")
		}
		for _, service := range artifact.Services {
			if service.ServiceId == apply.ServiceId && service.ComposeName == "caddy" {
				return nil
			}
		}
		return errs.New(errs.KindValidationFailed, "Caddy config apply Service is invalid")
	case *agentpb.ExecutionStep_BackupArtifactPrune:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
			return errs.New(errs.KindValidationFailed, "backup artifact prune requires a backup prune operation")
		}
		return validateBackupArtifactPrune("", payload.BackupArtifactPrune.GetOrdinal(), payload.BackupArtifactPrune)
	default:
		return errs.New(errs.KindValidationFailed, "execution step payload is unsupported")
	}
}

func validateBackupArtifactPrune(
	environmentID string,
	expectedOrdinal uint32,
	prune *agentpb.BackupArtifactPrune,
) error {
	if prune == nil || prune.Ordinal != expectedOrdinal ||
		validateID(ids.KindOperation, prune.PruneOperationId) != nil || prune.PruneRevision == 0 ||
		validateID(ids.KindRecoveryPoint, prune.PointId) != nil || prune.PointRevision == 0 ||
		validateID(ids.KindBackupSource, prune.SourceId) != nil || prune.SourceRevision == 0 ||
		validateID(ids.KindEnvironment, prune.EnvironmentId) != nil || prune.EnvironmentRevision == 0 ||
		(environmentID != "" && prune.EnvironmentId != environmentID) ||
		validateID(ids.KindConnector, prune.ConnectorId) != nil || prune.ConnectorRevision == 0 ||
		!validBackupConnectorEndpoint(prune.ConnectorEndpoint) ||
		!validBackupConnectorBucket(prune.ConnectorBucket) ||
		!validBackupConnectorPrefix(prune.ConnectorPrefix) ||
		!validBackupConnectorRegion(prune.ConnectorRegion) ||
		!validBackupS3Addressing(prune.ConnectorAddressing) ||
		!validBackupPruneObjectKey(prune) || prune.StoredSizeBytes == 0 ||
		len(prune.StoredSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "backup artifact prune control data is invalid")
	}
	return nil
}

func validBackupConnectorEndpoint(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > MaximumPlanBytes {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" &&
		(parsed.Path == "" || parsed.Path == "/") && !strings.HasSuffix(value, "/")
}

func validBackupConnectorBucket(value string) bool {
	if len(value) < 3 || len(value) > 63 || net.ParseIP(value) != nil ||
		!backupConnectorBucketAlphaNumeric(value[0]) || !backupConnectorBucketAlphaNumeric(value[len(value)-1]) ||
		strings.Contains(value, "..") || strings.Contains(value, ".-") || strings.Contains(value, "-.") {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if !backupConnectorBucketAlphaNumeric(character) && character != '-' && character != '.' {
			return false
		}
	}
	return true
}

func backupConnectorBucketAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func validBackupConnectorPrefix(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > maximumBackupObjectKeyBytes || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') ||
		strings.Contains(value, `\`) || strings.HasPrefix(value, "/") || !strings.HasSuffix(value, "/") {
		return false
	}
	withoutSlash := strings.TrimSuffix(value, "/")
	if withoutSlash == "" || path.Clean(withoutSlash) != withoutSlash {
		return false
	}
	for _, component := range strings.Split(withoutSlash, "/") {
		if component == "." || component == ".." {
			return false
		}
	}
	return true
}

func validBackupConnectorRegion(value string) bool {
	if value == "" || len(value) > MaximumPlanBytes {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validBackupS3Addressing(value agentpb.BackupS3Addressing) bool {
	return value == agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE ||
		value == agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE
}

func validBackupPruneObjectKey(prune *agentpb.BackupArtifactPrune) bool {
	if prune.ProtectedObjectKey == "" || len(prune.ProtectedObjectKey) > maximumBackupObjectKeyBytes ||
		!utf8.ValidString(prune.ProtectedObjectKey) || strings.ContainsRune(prune.ProtectedObjectKey, '\x00') ||
		strings.Contains(prune.ProtectedObjectKey, `\`) || strings.HasPrefix(prune.ProtectedObjectKey, "/") {
		return false
	}
	expected := prune.ConnectorPrefix + prune.EnvironmentId + "/" + prune.SourceId + "/" + prune.PointId +
		"/artifact.bin"
	return prune.ProtectedObjectKey == expected
}

func validateBackupSourceCapture(environmentID string, capture *agentpb.BackupSourceCapture) error {
	if capture == nil || validateID(ids.KindBackupSource, capture.SourceId) != nil ||
		capture.SourceRevision == 0 || capture.TargetRevision == 0 ||
		validateID(ids.KindRecoveryPoint, capture.PointId) != nil ||
		validateID(ids.KindConnector, capture.ConnectorId) != nil || capture.ConnectorRevision == 0 {
		return errs.New(errs.KindValidationFailed, "backup source identity or revision is invalid")
	}
	switch capture.Encryption {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if capture.KeyEra != 0 || capture.AgeRecipient != "" {
			return errs.New(errs.KindValidationFailed, "unencrypted backup source carries age control data")
		}
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		recipient, err := age.ParseX25519Recipient(capture.AgeRecipient)
		if capture.KeyEra == 0 || err != nil || recipient.String() != capture.AgeRecipient {
			return errs.New(errs.KindValidationFailed, "age-encrypted backup source control data is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source encryption is unsupported")
	}
	switch source := capture.Source.(type) {
	case *agentpb.BackupSourceCapture_Attach:
		if source.Attach == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1 ||
			validateID(ids.KindAttach, capture.TargetId) != nil ||
			validateID(ids.KindService, source.Attach.BackingServiceId) != nil ||
			source.Attach.BackingServiceRevision == 0 || !validAdapterIdentity(source.Attach.Database, true) ||
			!validAdapterIdentity(source.Attach.Role, true) {
			return errs.New(errs.KindValidationFailed, "postgresql backup source control data is invalid")
		}
	case *agentpb.BackupSourceCapture_Config:
		if source.Config == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1 ||
			capture.TargetId != environmentID || validateID(ids.KindEnvironment, capture.TargetId) != nil ||
			source.Config.SnapshotRevision == 0 ||
			capture.Encryption != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return errs.New(errs.KindValidationFailed, "config backup source control data is invalid")
		}
	case *agentpb.BackupSourceCapture_Volume:
		if source.Volume == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1 ||
			validateID(ids.KindVolume, capture.TargetId) != nil {
			return errs.New(errs.KindValidationFailed, "volume backup source control data is invalid")
		}
		previousServiceID := ""
		for _, service := range source.Volume.Services {
			if service == nil || validateID(ids.KindService, service.ServiceId) != nil ||
				service.ServiceId <= previousServiceID || service.ServiceRevision == 0 ||
				!validBackupServiceRuntimeIntent(service.PriorIntent) {
				return errs.New(errs.KindValidationFailed, "volume backup service control data is invalid")
			}
			previousServiceID = service.ServiceId
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source kind is unsupported")
	}
	return nil
}

func validBackupServiceRuntimeIntent(intent agentpb.BackupServiceRuntimeIntent) bool {
	switch intent {
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED,
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT:
		return true
	default:
		return false
	}
}

func validateAdapterProcedure(operation agentpb.PlanOperation, procedure *agentpb.AdapterProcedure) error {
	if procedure == nil || !validAdapterKey(procedure.AdapterKey) ||
		validateID(ids.KindAttach, procedure.AttachId) != nil ||
		(validateID(ids.KindService, procedure.BackingServiceId) != nil &&
			validateID(ids.KindComponent, procedure.BackingServiceId) != nil) ||
		!validAdapterIdentity(procedure.Role, true) ||
		!validAdapterSecret(procedure.Password) {
		return errs.New(errs.KindValidationFailed, "adapter procedure identity is invalid")
	}
	switch procedure.Phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH || len(procedure.Password) == 0 ||
			!validAdapterIdentity(procedure.Database, false) || procedure.GrantOn != "" {
			return errs.New(errs.KindValidationFailed, "adapter provision procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH || len(procedure.Password) != 0 ||
			procedure.Database != "" || !validAdapterIdentity(procedure.GrantOn, true) {
			return errs.New(errs.KindValidationFailed, "adapter grant procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_DETACH || len(procedure.Password) != 0 ||
			procedure.Database != "" || !validAdapterIdentity(procedure.GrantOn, true) {
			return errs.New(errs.KindValidationFailed, "adapter revoke procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_DETACH || len(procedure.Password) != 0 ||
			!validAdapterIdentity(procedure.Database, false) || procedure.GrantOn != "" {
			return errs.New(errs.KindValidationFailed, "adapter detach procedure is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "adapter procedure phase is unsupported")
	}
	return nil
}

func validAdapterKey(value string) bool {
	if len(value) == 0 || len(value) > MaximumAdapterKeyBytes {
		return false
	}
	for index, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
			(index > 0 && (character == '-' || character == '_' || character == '.' || character == ':')) {
			continue
		}
		return false
	}
	return true
}

func validAdapterSecret(value []byte) bool {
	if len(value) > MaximumAdapterSecretBytes {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validAdapterIdentity(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > maximumAdapterIdentityBytes || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '_' {
			continue
		}
		return false
	}
	return true
}

func operationCreatesManagedVolumes(operation agentpb.PlanOperation) bool {
	switch operation {
	case agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
		agentpb.PlanOperation_PLAN_OPERATION_START:
		return true
	default:
		return false
	}
}

func validateMaterializeFile(
	renderGeneration uint64,
	materialization *agentpb.MaterializeFile,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if materialization == nil || validateID(ids.KindConfig, materialization.MaterializationId) != nil ||
		validateID(ids.KindEnvironment, materialization.EnvironmentId) != nil ||
		len(materialization.Sha256) != sha256.Size ||
		materialization.Length > entrymaterialization.MaximumContentBytes ||
		uint64(len(materialization.Destination)) > uint64(entrymaterialization.MaximumDestinationBytes) {
		return errs.New(errs.KindValidationFailed, "materialization metadata is invalid")
	}
	artifact := artifacts[materialization.ArtifactId]
	if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != materialization.EnvironmentId {
		return errs.New(errs.KindValidationFailed, "materialization artifact ownership is invalid")
	}
	serviceName := ""
	if materialization.ServiceId != "" {
		for _, service := range artifact.Services {
			if service.ServiceId == materialization.ServiceId {
				serviceName = service.ComposeName
				break
			}
		}
		if serviceName == "" || materialization.ServiceName != serviceName {
			return errs.New(errs.KindValidationFailed, "materialization service identity is invalid")
		}
	} else if materialization.ServiceName != "" {
		return errs.New(errs.KindValidationFailed, "materialization service identity is invalid")
	}
	outputKind, err := materializationOutputKind(materialization.OutputKind)
	if err != nil {
		return err
	}
	if err := entrymaterialization.ValidateMetadata(entrymaterialization.MetadataSpec{
		EnvironmentID: materialization.EnvironmentId,
		Generation:    renderGeneration,
		Destination:   materialization.Destination,
		ServiceID:     materialization.ServiceId,
		ServiceName:   materialization.ServiceName,
		OutputKind:    outputKind,
		UID:           materialization.Uid,
		GID:           materialization.Gid,
		Mode:          entrymaterialization.Mode(materialization.Mode),
	}); err != nil {
		return errs.New(errs.KindValidationFailed, "materialization output policy is invalid")
	}
	return nil
}

func materializationOutputKind(value agentpb.MaterializationOutputKind) (entrymaterialization.OutputKind, error) {
	switch value {
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV:
		return entrymaterialization.OutputGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE:
		return entrymaterialization.OutputPlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE:
		return entrymaterialization.OutputSecretFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV:
		return entrymaterialization.OutputRemoveGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE:
		return entrymaterialization.OutputRemovePlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE:
		return entrymaterialization.OutputRemoveSecretFile, nil
	default:
		return 0, errs.New(errs.KindValidationFailed, "materialization output kind is invalid")
	}
}

func validateSelection(
	artifactID string,
	serviceIDs []string,
	artifacts map[string]*agentpb.ComposeArtifact,
	requireServices bool,
) error {
	artifact := artifacts[artifactID]
	if artifact == nil || (requireServices && len(serviceIDs) == 0) {
		return errs.New(errs.KindValidationFailed, "execution step artifact or service selection is invalid")
	}
	available := make(map[string]struct{}, len(artifact.Services))
	for _, service := range artifact.Services {
		available[service.ServiceId] = struct{}{}
	}
	selected := make(map[string]struct{}, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		if _, exists := available[serviceID]; !exists {
			return errs.New(errs.KindValidationFailed, "execution step selects an unknown service")
		}
		if _, duplicate := selected[serviceID]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution step selects a service more than once")
		}
		selected[serviceID] = struct{}{}
	}
	return nil
}

func rejectUnknown(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return errs.New(errs.KindValidationFailed, "execution plan contains unknown protobuf fields")
	}
	var result error
	message.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if descriptor.IsMap() {
			result = errs.New(errs.KindValidationFailed, "execution plan contains a protobuf map")
			return false
		}
		if descriptor.Kind() != protoreflect.MessageKind && descriptor.Kind() != protoreflect.GroupKind {
			return true
		}
		if descriptor.IsList() {
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if result = rejectUnknown(list.Get(index).Message()); result != nil {
					return false
				}
			}
			return true
		}
		result = rejectUnknown(value.Message())
		return result == nil
	})
	return result
}

func validateVolumeDirectory(value, environmentID string) error {
	if value == "" || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 ||
		!strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return errs.New(errs.KindValidationFailed, "authorized environment volume directory is invalid")
	}
	if path.Base(value) != environmentID {
		return errs.New(errs.KindValidationFailed, "authorized environment volume directory is outside its owner")
	}
	return nil
}

func validateComposeName(value string) error {
	if value == "" || len(value) > maximumComposeNameBytes || !utf8.ValidString(value) {
		return errs.New(errs.KindValidationFailed, "Compose resource name is invalid")
	}
	for _, character := range value {
		if character <= ' ' || character == '/' || character == '\\' {
			return errs.New(errs.KindValidationFailed, "Compose resource name contains an unsafe character")
		}
	}
	return nil
}

func validateAnyStableID(value string) error {
	separator := strings.IndexByte(value, '_')
	if separator <= 0 {
		return fmt.Errorf("invalid stable id")
	}
	kind := ids.Kind(value[:separator])
	switch kind {
	case ids.KindTenant, ids.KindProject, ids.KindEnvironment, ids.KindService,
		ids.KindDeployment, ids.KindEnvEntry, ids.KindVolume, ids.KindAttach,
		ids.KindRoute, ids.KindSecret, ids.KindConnector, ids.KindRunner,
		ids.KindScript, ids.KindBackupSource, ids.KindRecoveryPoint, ids.KindTask,
		ids.KindOperation, ids.KindPlan, ids.KindStep, ids.KindConfig, ids.KindAgent,
		ids.KindNetwork, ids.KindBackingService, ids.KindReleaseGroup, ids.KindComponent:
		return ids.Validate(kind, value)
	default:
		return fmt.Errorf("invalid stable id kind")
	}
}

func validateID(kind ids.Kind, value string) error {
	if err := ids.Validate(kind, value); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func validOperation(operation agentpb.PlanOperation) bool {
	switch operation {
	case agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
		agentpb.PlanOperation_PLAN_OPERATION_START,
		agentpb.PlanOperation_PLAN_OPERATION_STOP,
		agentpb.PlanOperation_PLAN_OPERATION_DESTROY,
		agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		agentpb.PlanOperation_PLAN_OPERATION_ATTACH,
		agentpb.PlanOperation_PLAN_OPERATION_DETACH,
		agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE:
		return true
	default:
		return false
	}
}

func validLabelKey(key string) bool {
	switch key {
	case labelEnvironmentID, labelKind, labelManaged, labelPlanID, labelProjectID,
		labelReleaseID, labelRenderGen, labelServiceID, labelSlot, labelTenantID:
		return true
	default:
		return false
	}
}
