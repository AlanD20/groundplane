// Package executionplan owns validation and deterministic hashing for the
// protobuf procedure shared by the Controller and Agent.
package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"path"
	"strings"
	"unicode/utf8"
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
	labelEnvironmentID     = "com.groundplane.environment-id"
	labelComponentID       = "com.groundplane.component-id"
	labelKind              = "com.groundplane.kind"
	labelManaged           = "com.groundplane.managed"
	labelPlanID            = "com.groundplane.plan-id"
	labelProjectID         = "com.groundplane.project-id"
	labelReleaseID         = "com.groundplane.release-id"
	labelRenderGen         = "com.groundplane.render-generation"
	labelRuntimeRole       = "com.groundplane.runtime-role"
	labelServiceID         = "com.groundplane.service-id"
	labelSlot              = "com.groundplane.slot"
	labelTenantID          = "com.groundplane.tenant-id"
	labelImageChildDigest  = "com.groundplane.image-child-digest"
	labelImageConfigDigest = "com.groundplane.image-config-digest"
	labelImageIndexDigest  = "com.groundplane.image-index-digest"
	labelImagePlatform     = "com.groundplane.image-platform"
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
	if err := validateRuntimeMutationProcedures(plan); err != nil {
		return err
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_SCRIPT {
		return validateManualScriptPlan(plan)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
		plan.GetComponentRollbackObservation() != nil {
		return errs.New(errs.KindValidationFailed, "non-Component plan carries a rollback observation")
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
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE && len(plan.Artifacts) == 0 {
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
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY && len(plan.Artifacts) == 0 {
		return validateComponentApplyPlan(plan, nil)
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
		validationPlan, lifecycle, identityErr := serviceLifecycleArtifactValidationPlan(plan, artifact)
		if identityErr != nil {
			return identityErr
		}
		if !lifecycle {
			validationPlan, identityErr = managedComponentArtifactValidationPlan(plan, artifact)
			if identityErr == nil {
				validationPlan, identityErr = componentArtifactValidationPlan(validationPlan, artifact)
			}
			if identityErr != nil {
				return identityErr
			}
		}
		if err := validateArtifact(validationPlan, artifact); err != nil {
			return err
		}
		if _, duplicate := artifacts[artifact.ArtifactId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan artifact ids must be unique")
		}
		artifacts[artifact.ArtifactId] = artifact
	}
	if err := validateManagedComponentProcedure(plan, artifacts); err != nil {
		return err
	}
	if err := validateCandidateReleasePlan(plan, artifacts); err != nil {
		return err
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if err := validateComponentApplyPlan(plan, artifacts); err != nil {
			return err
		}
	}
	if len(plan.Steps) == 0 {
		return errs.New(errs.KindValidationFailed, "execution plan must contain at least one step")
	}
	if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE {
		if err := validateEnvironmentDirectoryCreateTarget(plan, plan.Steps[0]); err != nil {
			return err
		}
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	materializationIDs := make(map[string]struct{})
	materializationDestinations := make(map[string]struct{})
	for _, step := range plan.Steps {
		if err := validateStep(plan, step, artifacts, plan.Steps); err != nil {
			return err
		}
		if procedure := step.GetAdapterProcedure(); procedure != nil &&
			plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
			plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
			procedure.AttachId != plan.TargetId {
			return errs.New(errs.KindValidationFailed, "adapter procedure does not identify the plan target")
		}
		if remove := step.GetEnvironmentDirectoryRemove(); remove != nil && remove.EnvironmentId != plan.TargetId {
			return errs.New(
				errs.KindValidationFailed,
				"environment directory removal does not identify the plan target",
			)
		}
		if remove := step.GetManagedVolumeRemove(); remove != nil && remove.VolumeId != plan.TargetId {
			return errs.New(errs.KindValidationFailed, "managed Volume removal does not identify the plan target")
		}
		if remove := step.GetManagedVolumeDirectoryRemove(); remove != nil && remove.VolumeId != plan.TargetId {
			return errs.New(
				errs.KindValidationFailed,
				"managed Volume directory removal does not identify the plan target",
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
			destinationKey := materializationDestinationKey(step)
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
		agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
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
	case labelComponentID, labelEnvironmentID, labelImageChildDigest, labelImageConfigDigest, labelImageIndexDigest,
		labelImagePlatform,
		labelKind, labelManaged, labelPlanID, labelProjectID,
		labelReleaseID, labelRenderGen, labelRuntimeRole, labelServiceID, labelSlot, labelTenantID:
		return true
	default:
		return false
	}
}
