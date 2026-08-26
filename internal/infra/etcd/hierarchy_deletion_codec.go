package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hierarchyDeletionSmallRecordBytes      = 4 << 10
	hierarchyDeletionActionRecordBytes     = 16 << 10
	hierarchyDeletionCompletionRecordBytes = 8 << 10
	hierarchyDeletionLargeRecordBytes      = 64 << 10
	hierarchyDeletionTransactionBytes      = 900 << 10
)

var hierarchyDeletionPrivateIDPattern = regexp.MustCompile(`^(del|act)_[0-9a-f]{32}$`)
var hierarchyDeletionRawStableIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*_[0-9A-HJKMNP-TV-Z]{26}$`)

func encodeHierarchyDeletionRecord(value any, maximum int) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) == 0 || len(encoded) > maximum {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion record exceeds its size limit")
	}
	return encoded, nil
}

func decodeHierarchyDeletionRecord(value []byte, maximum int, target any) error {
	if len(value) == 0 || len(value) > maximum || rejectDuplicateJSONFields(value) != nil {
		return corruptHierarchyDeletion()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil || requireJSONEOF(decoder) != nil {
		return corruptHierarchyDeletion()
	}
	return nil
}

func hierarchyDeletionDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func validHierarchyDeletionDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func validHierarchyDeletionPrivateID(value string, prefix string) bool {
	return hierarchyDeletionPrivateIDPattern.MatchString(value) && strings.HasPrefix(value, prefix+"_")
}

func validHierarchyDeletionRawStableID(value string) bool {
	return hierarchyDeletionRawStableIDPattern.MatchString(value)
}

func enforceHierarchyDeletionTransaction(conditions []Condition, mutations []Mutation) error {
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion transaction exceeds its operation limit")
	}
	size := 0
	for _, condition := range conditions {
		size += len(condition.Key) + 32
	}
	for _, mutation := range mutations {
		size += len(mutation.Key) + len(mutation.Value) + 32
	}
	if size > hierarchyDeletionTransactionBytes {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion transaction exceeds its byte limit")
	}
	return nil
}

func validHierarchyDeletionTarget(kind HierarchyDeletionTargetKind, id string) bool {
	var expected ids.Kind
	switch kind {
	case HierarchyDeletionTargetTenant:
		expected = ids.KindTenant
	case HierarchyDeletionTargetProject:
		expected = ids.KindProject
	case HierarchyDeletionTargetBacking:
		expected = ids.KindProject
	case HierarchyDeletionTargetEnvironment:
		expected = ids.KindEnvironment
	default:
		return false
	}
	return ids.Validate(expected, id) == nil
}

func validHierarchyDeletionOperation(kind HierarchyDeletionOperationKind, target HierarchyDeletionTargetKind) bool {
	switch kind {
	case HierarchyDeletionOperationTenant:
		return target == HierarchyDeletionTargetTenant
	case HierarchyDeletionOperationProject:
		return target == HierarchyDeletionTargetProject
	case HierarchyDeletionOperationBacking:
		return target == HierarchyDeletionTargetBacking
	case HierarchyDeletionOperationEnvironment:
		return target == HierarchyDeletionTargetEnvironment
	default:
		return false
	}
}

func validHierarchyDeletionPhase(value HierarchyDeletionPhase) bool {
	switch value {
	case HierarchyDeletionPlanning, HierarchyDeletionExecuting, HierarchyDeletionSummarizing,
		HierarchyDeletionFinalizing, HierarchyDeletionRetained:
		return true
	default:
		return false
	}
}

func validHierarchyDeletionTimestamp(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Equal(value.UTC())
}

func validateHierarchyCoordination(record HierarchyCoordinationRecord) error {
	if record.Schema != 1 || !validHierarchyDeletionTarget(record.TargetKind, record.TargetID) ||
		record.MutationEpoch <= 0 {
		return errs.New(errs.KindValidationFailed, "hierarchy coordination record is invalid")
	}
	return nil
}

func encodeHierarchyCoordination(record HierarchyCoordinationRecord) ([]byte, error) {
	if err := validateHierarchyCoordination(record); err != nil {
		return nil, err
	}
	return encodeHierarchyDeletionRecord(record, hierarchyDeletionSmallRecordBytes)
}

func decodeHierarchyCoordination(value []byte) (HierarchyCoordinationRecord, error) {
	var record HierarchyCoordinationRecord
	if err := decodeHierarchyDeletionRecord(value, hierarchyDeletionSmallRecordBytes, &record); err != nil ||
		validateHierarchyCoordination(record) != nil {
		return HierarchyCoordinationRecord{}, corruptHierarchyDeletion()
	}
	return record, nil
}

func validateHierarchyDeletionAction(action HierarchyDeletionAction) error {
	if action.Schema != 1 || !validHierarchyDeletionPrivateID(action.ParentOperationID, "del") ||
		action.NodeID == "" || action.Ordinal < 0 || action.TargetID == "" || action.TargetRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion action identity is invalid")
	}
	if !slices.IsSorted(action.PrerequisiteOrdinals) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion prerequisites are not ordered")
	}
	previous := int64(-1)
	for _, ordinal := range action.PrerequisiteOrdinals {
		if ordinal < 0 || ordinal >= action.Ordinal || ordinal == previous {
			return errs.New(errs.KindValidationFailed, "hierarchy deletion prerequisites are invalid")
		}
		previous = ordinal
	}
	switch action.ProcedureKind {
	case HierarchyDeletionProcedureAgent:
		if action.AgentProcedure == nil || action.ControllerProcedure != nil ||
			ids.Validate(ids.KindOperation, action.AgentProcedure.ChildOperationID) != nil ||
			(action.AgentProcedure.TaskType != TaskRemove && action.AgentProcedure.TaskType != TaskDetach) ||
			action.AgentProcedure.TypedProcedure == "" ||
			!validHierarchyDeletionDigest(action.AgentProcedure.InputDigest) ||
			action.AgentProcedure.TimeoutSeconds <= 0 {
			return errs.New(errs.KindValidationFailed, "hierarchy deletion Agent procedure is invalid")
		}
	case HierarchyDeletionProcedureController:
		procedure := action.ControllerProcedure
		if procedure == nil || action.AgentProcedure != nil || procedure.Finalizer == "" ||
			procedure.FixedInputRevision <= 0 || !validHierarchyDeletionDigest(procedure.CompareTemplateDigest) ||
			!validHierarchyDeletionDigest(procedure.MutationTemplateDigest) ||
			!validHierarchyDeletionDigest(procedure.PostconditionTemplateDigest) {
			return errs.New(errs.KindValidationFailed, "hierarchy deletion Controller procedure is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion procedure kind is invalid")
	}
	if !hierarchyDeletionProcedureMatchesAction(action) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion action procedure does not match its kind")
	}
	return nil
}

func hierarchyDeletionProcedureMatchesAction(action HierarchyDeletionAction) bool {
	agent := map[HierarchyDeletionActionKind]string{
		HierarchyDeletionAttachGrantRevoke: "attach.grant-revoke", HierarchyDeletionAttachDetach: "attach.detach",
		HierarchyDeletionEnvironmentAgentCleanup: "environment.cleanup", HierarchyDeletionServiceRemove: "service.remove",
		HierarchyDeletionEntryRemove: "entry.remove", HierarchyDeletionRouteRemove: "route.remove",
		HierarchyDeletionComponentRemove: "component.remove", HierarchyDeletionMaterializationRemove: "materialization.remove",
		HierarchyDeletionVolumeAgentCleanup: "volume.cleanup", HierarchyDeletionZoneRemove: "zone.remove",
		HierarchyDeletionNetworkRemove: "network.remove", HierarchyDeletionRecoveryPointRemove: "recovery-point.remove",
		HierarchyDeletionOrphanObjectRemove:        "orphan-object.remove",
		HierarchyDeletionBackingRuntimeReconstruct: "backing.runtime-reconstruct",
	}
	if expected, ok := agent[action.ActionKind]; ok {
		return action.ProcedureKind == HierarchyDeletionProcedureAgent &&
			action.AgentProcedure.TypedProcedure == expected
	}
	controller := map[HierarchyDeletionActionKind]string{
		HierarchyDeletionScriptRemove: "script.remove", HierarchyDeletionReleaseGroupRemove: "release-group.remove",
		HierarchyDeletionReleaseFinalize:      "release.finalize",
		HierarchyDeletionBackupPolicyFinalize: "backup-policy.finalize",
		HierarchyDeletionKeyMaterialRemove:    "key-material.remove", HierarchyDeletionVolumeFinalize: "volume.finalize",
		HierarchyDeletionReservationRelease:  "reservation.release",
		HierarchyDeletionConnectorFinalize:   "connector.finalize",
		HierarchyDeletionEnvironmentFinalize: "environment.finalize",
		HierarchyDeletionRunnerLocalRemove:   "runner.remove", HierarchyDeletionProjectSecretRemove: "secret.remove",
		HierarchyDeletionBackingServiceFinalize: "backing.finalize",
		HierarchyDeletionProjectFinalize:        "project.finalize", HierarchyDeletionTenantFinalize: "tenant.finalize",
	}
	expected, ok := controller[action.ActionKind]
	return ok && action.ProcedureKind == HierarchyDeletionProcedureController &&
		action.ControllerProcedure.Finalizer == expected
}

func encodeHierarchyDeletionAction(action HierarchyDeletionAction) ([]byte, error) {
	if err := validateHierarchyDeletionAction(action); err != nil {
		return nil, err
	}
	return encodeHierarchyDeletionRecord(action, hierarchyDeletionActionRecordBytes)
}

func decodeHierarchyDeletionAction(value []byte) (HierarchyDeletionAction, error) {
	var action HierarchyDeletionAction
	if err := decodeHierarchyDeletionRecord(value, hierarchyDeletionActionRecordBytes, &action); err != nil ||
		validateHierarchyDeletionAction(action) != nil {
		return HierarchyDeletionAction{}, corruptHierarchyDeletion()
	}
	return action, nil
}

func hierarchyDeletionTransactionSize(conditions []Condition, mutations []Mutation) int {
	total := 0
	for _, condition := range conditions {
		total += len(condition.Key) + 24
	}
	for _, mutation := range mutations {
		total += len(mutation.Key) + len(mutation.Value) + 24
	}
	return total
}

func validateHierarchyDeletionTransaction(conditions []Condition, mutations []Mutation, operationLimit int) error {
	if operationLimit <= 0 || len(conditions)+len(mutations) > operationLimit ||
		len(conditions)+len(mutations) > maximumTransactionOperations {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion transaction exceeds its operation limit")
	}
	if hierarchyDeletionTransactionSize(conditions, mutations) > hierarchyDeletionTransactionBytes {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion transaction exceeds its byte limit")
	}
	return nil
}

func corruptHierarchyDeletion() error {
	return errs.New(errs.KindInternal, "hierarchy deletion evidence is corrupt")
}
