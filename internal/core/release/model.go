// Package release owns the immutable Release ledger and execution state model.
package release

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MinimumGroupMembers = 2
	MaximumGroupMembers = 32
	MaximumRecordBytes  = 256 << 10
)

type OperationKind string

const (
	OperationDeploy         OperationKind = "deploy"
	OperationRollback       OperationKind = "rollback"
	OperationBlueprintApply OperationKind = "blueprint_apply"
)

type Strategy string

const (
	StrategyBlueGreen Strategy = "blue-green"
	StrategyRecreate  Strategy = "recreate"
)

type OnFailure string

const (
	OnFailureSwitchBack  OnFailure = "switch_back"
	OnFailureLeaveActive OnFailure = "leave_active"
)

type Slot string

const (
	SlotBlue  Slot = "blue"
	SlotGreen Slot = "green"
)

type WorkspaceKind string

const (
	WorkspaceTenant   WorkspaceKind = "tenant"
	WorkspacePlatform WorkspaceKind = "platform"
)

type Workspace struct {
	Kind          WorkspaceKind `json:"kind"`
	TenantID      string        `json:"tenant_id,omitempty"`
	ProjectID     string        `json:"project_id"`
	EnvironmentID string        `json:"environment_id"`
}

// Intent is immutable after its publication marker becomes visible.
type Intent struct {
	ID                       string        `json:"id"`
	EnvironmentID            string        `json:"environment_id"`
	ServiceID                string        `json:"service_id"`
	OperationID              string        `json:"operation_id"`
	OperationKind            OperationKind `json:"operation_kind"`
	GroupOperationID         string        `json:"group_operation_id,omitempty"`
	GroupMemberOrdinal       uint32        `json:"group_member_ordinal,omitempty"`
	CandidateWorkload        WorkloadSeal  `json:"candidate_workload"`
	Tag                      string        `json:"tag"`
	Strategy                 Strategy      `json:"strategy"`
	Slot                     Slot          `json:"slot,omitempty"`
	OnFailure                OnFailure     `json:"on_failure"`
	RollbackSourceReleaseID  string        `json:"rollback_source_release_id,omitempty"`
	PriorServingReleaseID    string        `json:"prior_serving_release_id,omitempty"`
	PriorSuccessfulReleaseID string        `json:"prior_successful_release_id,omitempty"`
	RenderInputID            string        `json:"render_input_id"`
	RenderInputDigest        string        `json:"render_input_digest"`
	CreatedAt                time.Time     `json:"created_at"`
	Actor                    string        `json:"actor"`
	Workspace                Workspace     `json:"workspace"`
	OriginatingTaskID        string        `json:"originating_task_id"`
}

// FailureHookTargetReleaseID returns the only sealed Release definition that
// an on-failure Script may use for this operation. Runtime serving evidence
// must still confirm this identity before the runner may start.
func FailureHookTargetReleaseID(intent Intent) string {
	if intent.OnFailure == OnFailureSwitchBack && intent.PriorServingReleaseID != "" {
		return intent.PriorServingReleaseID
	}
	return intent.ID
}

type Attempt struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	RetryOf   string    `json:"retry_of,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

type State string

const (
	StatePending          State = "pending"
	StateRunning          State = "running"
	StateCandidateHealthy State = "candidate_healthy"
	StateSwitching        State = "switching"
	StateServing          State = "serving"
	StatePostHooks        State = "post_hooks"
	StateCompensating     State = "compensating"
	StateCompleted        State = "completed"
	StateFailed           State = "failed"
	StateTimedOut         State = "timed_out"
	StateAborted          State = "aborted"
	StateRecoveryRequired State = "recovery_required"
	StateRecovering       State = "recovering"
)

func (state State) Terminal() bool {
	switch state {
	case StateCompleted, StateFailed, StateTimedOut, StateAborted:
		return true
	default:
		return false
	}
}

type EffectEvidence struct {
	PlanID                    string    `json:"plan_id"`
	StepID                    string    `json:"step_id"`
	AttemptID                 string    `json:"attempt_id"`
	AgentID                   string    `json:"agent_id"`
	AcknowledgementID         string    `json:"acknowledgement_id"`
	ObservedReleaseID         string    `json:"observed_release_id"`
	ObservedSlot              Slot      `json:"observed_slot,omitempty"`
	ObservedRenderGeneration  uint64    `json:"observed_render_generation"`
	EffectDigest              string    `json:"effect_digest"`
	RouterConfigurationDigest string    `json:"router_configuration_digest,omitempty"`
	ObservedRouterTarget      string    `json:"observed_router_target,omitempty"`
	AcknowledgedAt            time.Time `json:"acknowledged_at"`
}

type Checkpoint struct {
	ReleaseID string           `json:"release_id"`
	State     State            `json:"state"`
	Evidence  []EffectEvidence `json:"evidence,omitempty"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type TerminalSummary struct {
	ReleaseID              string    `json:"release_id"`
	Outcome                State     `json:"outcome"`
	FinalServingReleaseID  string    `json:"final_serving_release_id,omitempty"`
	EffectDigests          []string  `json:"effect_digests"`
	AttemptIDs             []string  `json:"attempt_ids"`
	RollbackMaterialDigest string    `json:"rollback_material_digest"`
	CompletedAt            time.Time `json:"completed_at"`
}

type RetentionStatus string

const (
	RetentionAvailable RetentionStatus = "available"
	RetentionExpired   RetentionStatus = "expired"
)

type RollbackMaterial struct {
	ReleaseID  string          `json:"release_id"`
	Status     RetentionStatus `json:"status"`
	References []string        `json:"references"`
	Digest     string          `json:"digest"`
	Revision   uint64          `json:"revision"`
	ExpiredAt  *time.Time      `json:"expired_at,omitempty"`
}

type ServiceProjection struct {
	EnvironmentID              string `json:"environment_id"`
	ServiceID                  string `json:"service_id"`
	ServingReleaseID           string `json:"serving_release_id,omitempty"`
	CurrentSuccessfulReleaseID string `json:"current_successful_release_id,omitempty"`
	ServingSlot                Slot   `json:"serving_slot,omitempty"`
	ActiveOperationID          string `json:"active_operation_id,omitempty"`
	Revision                   uint64 `json:"revision"`
}

func ValidateIntent(value Intent) error {
	if ids.Validate(ids.KindDeployment, value.ID) != nil ||
		ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, value.ServiceID) != nil ||
		ids.Validate(ids.KindOperation, value.OperationID) != nil ||
		ids.Validate(ids.KindConfig, value.RenderInputID) != nil ||
		ids.Validate(ids.KindTask, value.OriginatingTaskID) != nil {
		return invalid("release intent identities are invalid")
	}
	if value.GroupOperationID == "" {
		if value.GroupMemberOrdinal != 0 {
			return invalid("single-service release has a group ordinal")
		}
	} else if ids.Validate(ids.KindOperation, value.GroupOperationID) != nil || value.GroupMemberOrdinal == 0 || value.GroupMemberOrdinal > MaximumGroupMembers {
		return invalid("release group identity or ordinal is invalid")
	}
	if value.OperationKind != OperationDeploy && value.OperationKind != OperationRollback &&
		value.OperationKind != OperationBlueprintApply {
		return invalid("release operation kind is invalid")
	}
	if value.OperationKind == OperationRollback {
		if ids.Validate(ids.KindDeployment, value.RollbackSourceReleaseID) != nil {
			return invalid("rollback source release id is invalid")
		}
	} else if value.RollbackSourceReleaseID != "" {
		return invalid("deploy release has a rollback source")
	}
	for _, candidate := range []string{value.PriorServingReleaseID, value.PriorSuccessfulReleaseID} {
		if candidate != "" && ids.Validate(ids.KindDeployment, candidate) != nil {
			return invalid("prior release identity is invalid")
		}
	}
	if ValidateWorkloadSeal(value.CandidateWorkload) != nil || !validText(value.Tag) ||
		strings.ContainsAny(value.Tag, "@/\\") {
		return invalid("release image or tag is invalid")
	}
	if value.Strategy == StrategyBlueGreen && value.CandidateWorkload.ReplicaCount != 1 {
		return invalid("blue-green requires a singleton workload")
	}
	if !validSHA256(value.RenderInputDigest) || value.CreatedAt.IsZero() || value.CreatedAt.Location() != time.UTC ||
		!validText(value.Actor) {
		return invalid("release immutable evidence is invalid")
	}
	if value.OnFailure != OnFailureSwitchBack && value.OnFailure != OnFailureLeaveActive {
		return invalid("release failure policy is invalid")
	}
	switch value.Strategy {
	case StrategyBlueGreen:
		if value.Slot != SlotBlue && value.Slot != SlotGreen {
			return invalid("blue-green release slot is invalid")
		}
	case StrategyRecreate:
		if value.Slot != "" {
			return invalid("recreate release must not have a slot")
		}
	default:
		return invalid("release strategy is invalid")
	}
	if err := validateWorkspace(value.Workspace, value.EnvironmentID); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > MaximumRecordBytes {
		return invalid("release intent exceeds the durable record limit")
	}
	return nil
}

func ValidateCheckpoint(value Checkpoint) error {
	if ids.Validate(ids.KindDeployment, value.ReleaseID) != nil || !validState(value.State) ||
		value.UpdatedAt.IsZero() ||
		value.UpdatedAt.Location() != time.UTC {
		return invalid("release checkpoint is invalid")
	}
	seen := make(map[string]struct{}, len(value.Evidence))
	for _, evidence := range value.Evidence {
		if err := ValidateEvidence(evidence, value.ReleaseID); err != nil {
			return err
		}
		if _, exists := seen[evidence.AcknowledgementID]; exists {
			return invalid("release checkpoint acknowledgement is duplicated")
		}
		seen[evidence.AcknowledgementID] = struct{}{}
	}
	return nil
}

func ValidateEvidence(value EffectEvidence, releaseID string) error {
	if ids.Validate(ids.KindPlan, value.PlanID) != nil || ids.Validate(ids.KindStep, value.StepID) != nil ||
		ids.Validate(ids.KindTask, value.AttemptID) != nil || ids.Validate(ids.KindAgent, value.AgentID) != nil ||
		ids.Validate(ids.KindAssignment, value.AcknowledgementID) != nil || value.ObservedReleaseID != releaseID ||
		value.ObservedRenderGeneration == 0 || !validSHA256(value.EffectDigest) || value.AcknowledgedAt.IsZero() ||
		value.AcknowledgedAt.Location() != time.UTC {
		return invalid("release checkpoint evidence is invalid")
	}
	if value.ObservedSlot != "" && value.ObservedSlot != SlotBlue && value.ObservedSlot != SlotGreen {
		return invalid("release checkpoint slot is invalid")
	}
	if value.RouterConfigurationDigest != "" && !validSHA256(value.RouterConfigurationDigest) {
		return invalid("release router evidence digest is invalid")
	}
	return nil
}

func CanTransition(from State, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case StatePending:
		return to == StateRunning || to == StateAborted || to == StateTimedOut
	case StateRunning:
		return to == StateCandidateHealthy || to == StateServing || to == StateCompensating || to == StateFailed ||
			to == StateTimedOut ||
			to == StateAborted ||
			to == StateRecoveryRequired
	case StateCandidateHealthy:
		return to == StateSwitching || to == StateServing || to == StateCompensating || to == StateFailed ||
			to == StateTimedOut ||
			to == StateAborted ||
			to == StateRecoveryRequired
	case StateSwitching:
		return to == StateServing || to == StateCompensating || to == StateRecoveryRequired
	case StateServing:
		return to == StatePostHooks || to == StateCompleted || to == StateCompensating || to == StateFailed ||
			to == StateTimedOut ||
			to == StateAborted ||
			to == StateRecoveryRequired
	case StatePostHooks:
		return to == StateCompleted || to == StateCompensating || to == StateFailed || to == StateTimedOut ||
			to == StateAborted ||
			to == StateRecoveryRequired
	case StateCompensating:
		return to == StateFailed || to == StateTimedOut || to == StateAborted || to == StateRecoveryRequired
	case StateRecoveryRequired:
		return to == StateRecovering
	case StateRecovering:
		return to == StateFailed || to == StateTimedOut || to == StateAborted || to == StateRecoveryRequired
	default:
		return false
	}
}

func Digest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func CloneIntent(value Intent) Intent { return value }

func CloneCheckpoint(value Checkpoint) Checkpoint {
	value.Evidence = slices.Clone(value.Evidence)
	return value
}

func validateWorkspace(value Workspace, environmentID string) error {
	if value.Kind != WorkspaceTenant || ids.Validate(ids.KindTenant, value.TenantID) != nil ||
		ids.Validate(ids.KindProject, value.ProjectID) != nil || value.EnvironmentID != environmentID {
		return invalid("release workspace is invalid")
	}
	return nil
}

func validState(value State) bool {
	switch value {
	case StatePending, StateRunning, StateCandidateHealthy, StateSwitching, StateServing, StatePostHooks,
		StateCompensating, StateCompleted, StateFailed, StateTimedOut, StateAborted, StateRecoveryRequired, StateRecovering:
		return true
	default:
		return false
	}
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func validText(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
