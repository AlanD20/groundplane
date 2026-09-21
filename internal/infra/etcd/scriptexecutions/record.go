package scriptexecutions

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
	"strings"
	"time"
)

const (
	ScriptExecutionIDParam = "script_execution_id"
	ScriptGenerationParam  = "script_generation"

	scriptExecutionPrefix           = "/v1/script-executions/"
	ScriptRunnerSnapshotPrefix      = "/v1/script-runner-snapshots/"
	scriptBodyForwardRefSegment     = "/references/"
	scriptBodyReverseRefSegment     = "/body-reference"
	MaximumReleaseHookTerminalBatch = 8
)

type ScriptExecutionState string

type ScriptControllerCleanupAuthority string

const (
	ScriptExecutionNotStarted       ScriptExecutionState = "not_started"
	ScriptExecutionStartAuthorized  ScriptExecutionState = "start_authorized"
	ScriptExecutionBodyPrepared     ScriptExecutionState = "body_prepared"
	ScriptExecutionContainerCreated ScriptExecutionState = "container_created"
	ScriptExecutionOutcomeRecorded  ScriptExecutionState = "outcome_recorded"
	ScriptExecutionCleanupProven    ScriptExecutionState = "cleanup_proven"

	ScriptControllerCleanupBlueprintPendingAbort        ScriptControllerCleanupAuthority = "blueprint_pending_abort"
	ScriptControllerCleanupManualPendingAbort           ScriptControllerCleanupAuthority = "manual_pending_abort"
	ScriptControllerCleanupManualAssignedAbort          ScriptControllerCleanupAuthority = "manual_assigned_abort"
	ScriptControllerCleanupManualRetryExpiry            ScriptControllerCleanupAuthority = "manual_retry_expiry"
	ScriptControllerCleanupReleaseRecoveryParentFailure ScriptControllerCleanupAuthority = "release_recovery_parent_failure"
)

// ScriptExecutionRecord is the durable recovery authority for one one-off
// Script container. Plan and Snapshot contain no Script body or secret bytes.
type ScriptExecutionRecord struct {
	ID                     string                           `json:"id"`
	SnapshotID             string                           `json:"snapshot_id"`
	OperationID            string                           `json:"operation_id"`
	CurrentTaskID          string                           `json:"current_task_id"`
	AssignmentID           string                           `json:"assignment_id,omitempty"`
	StepID                 string                           `json:"step_id"`
	ScriptID               string                           `json:"script_id"`
	ScriptGeneration       uint64                           `json:"script_generation"`
	ScriptSetGeneration    string                           `json:"script_set_generation"`
	EnvironmentID          string                           `json:"environment_id"`
	ServiceID              string                           `json:"service_id"`
	ReleaseID              string                           `json:"release_id"`
	RenderGeneration       uint64                           `json:"render_generation"`
	PlanHash               string                           `json:"plan_hash"`
	SourceMembershipCount  uint64                           `json:"source_membership_count,omitempty"`
	SourceMembershipSHA256 string                           `json:"source_membership_sha256,omitempty"`
	SnapshotSHA256         string                           `json:"snapshot_sha256"`
	BodySHA256             string                           `json:"body_sha256"`
	RunnerProjectionSHA256 string                           `json:"runner_projection_sha256"`
	Plan                   []byte                           `json:"plan"`
	Snapshot               []byte                           `json:"snapshot"`
	State                  ScriptExecutionState             `json:"state"`
	StartAuthorized        bool                             `json:"start_authorized"`
	BodyPrepared           *ScriptBodyPreparedEvidence      `json:"body_prepared,omitempty"`
	ContainerCreated       *ScriptContainerCreatedEvidence  `json:"container_created,omitempty"`
	Outcome                *ScriptOutcomeEvidence           `json:"outcome,omitempty"`
	Cleanup                *ScriptCleanupEvidence           `json:"cleanup,omitempty"`
	ControllerCleanup      ScriptControllerCleanupAuthority `json:"controller_cleanup,omitempty"`
	LastCheckpointSHA256   string                           `json:"last_checkpoint_sha256,omitempty"`
	ReconciliationRequired bool                             `json:"reconciliation_required"`
	ActiveReference        bool                             `json:"active_reference"`
	CreatedAt              time.Time                        `json:"created_at"`
	UpdatedAt              time.Time                        `json:"updated_at"`
}

func ValidateScriptExecutionRecord(record ScriptExecutionRecord) error {
	if (record.SourceMembershipCount == 0) != (record.SourceMembershipSHA256 == "") ||
		(record.SourceMembershipCount > 0 && !ValidLowerSHA256(record.SourceMembershipSHA256)) {
		return errs.New(errs.KindValidationFailed, "Script execution source membership is invalid")
	}
	if !ValidRawScriptExecutionID(record.ID) || !ValidRawScriptExecutionID(record.SnapshotID) ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.CurrentTaskID) != nil || ids.Validate(ids.KindStep, record.StepID) != nil ||
		ids.Validate(ids.KindScript, record.ScriptID) != nil ||
		record.ScriptGeneration == 0 || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(
			ids.KindService,
			record.ServiceID,
		) != nil || ids.Validate(ids.KindDeployment, record.ReleaseID) != nil ||
		record.RenderGeneration == 0 || !ValidLowerSHA256(record.PlanHash) || !ValidLowerSHA256(record.SnapshotSHA256) ||
		!ValidLowerSHA256(record.BodySHA256) || !ValidLowerSHA256(record.RunnerProjectionSHA256) ||
		len(record.Plan) == 0 || len(record.Plan) > executionplan.MaximumPlanBytes || len(record.Snapshot) == 0 ||
		!validScriptExecutionState(
			record.State,
		) || record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) ||
		validateScriptExecutionCheckpointShape(record) != nil {
		return errs.New(errs.KindValidationFailed, "Script execution record is invalid")
	}
	return nil
}

func validScriptExecutionState(state ScriptExecutionState) bool {
	switch state {
	case ScriptExecutionNotStarted, ScriptExecutionStartAuthorized, ScriptExecutionBodyPrepared,
		ScriptExecutionContainerCreated, ScriptExecutionOutcomeRecorded, ScriptExecutionCleanupProven:
		return true
	default:
		return false
	}
}

func ScriptExecutionKey(executionID string) string { return scriptExecutionPrefix + executionID }

func ScriptRunnerSnapshotKey(snapshotID string) string {
	return ScriptRunnerSnapshotPrefix + snapshotID
}

func ScriptSetBodyForwardReferenceKey(
	environmentID, setGeneration, scriptID string,
	generation uint64,
	executionID string,
) string {
	return scriptrecord.ScriptSetBodyGenerationKey(
		environmentID,
		setGeneration,
		scriptID,
		generation,
	) + scriptBodyForwardRefSegment + executionID
}

func ScriptBodyReverseReferenceKey(executionID string) string {
	return ScriptExecutionKey(executionID) + scriptBodyReverseRefSegment
}

func ValidRawScriptExecutionID(value string) bool {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return false
	}
	_, err := ulid.ParseStrict(value)
	return err == nil
}

func ValidLowerSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}
