package taskassignments

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const managedNetworkRecoveryPrefix = "/v1/records/managed-network-recoveries/"

type ManagedNetworkRecoveryRecord struct {
	Schema                 int                               `json:"schema"`
	TaskID                 string                            `json:"task_id"`
	AssignmentID           string                            `json:"assignment_id"`
	OperationID            string                            `json:"operation_id"`
	PlanHash               string                            `json:"plan_hash"`
	ProcedureSHA256        string                            `json:"procedure_sha256"`
	PrimaryReportSHA256    string                            `json:"primary_report_sha256"`
	PrimaryStatus          taskjournal.TaskStatus            `json:"primary_status"`
	PrimaryResult          taskjournal.TaskResultData        `json:"primary_result"`
	RecoveryDeadline       time.Time                         `json:"recovery_deadline"`
	ForwardStepIDs         []string                          `json:"forward_step_ids"`
	SettlementStepIDs      []string                          `json:"settlement_step_ids"`
	OriginalExecutionEpoch uint32                            `json:"original_execution_epoch"`
	MutationEvidence       []ReleaseRecoveryMutationEvidence `json:"mutation_evidence"`
	EvidenceRevision       int64                             `json:"evidence_revision"`
}

func ManagedNetworkRecoveryKey(taskID string) string { return managedNetworkRecoveryPrefix + taskID }

func ManagedNetworkRecoveryProcedureSHA256(procedure *agentpb.ManagedNetworkRecoveryProcedure) (string, error) {
	if procedure == nil {
		return "", errs.New(errs.KindValidationFailed, "managed network recovery procedure is absent")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(procedure)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validateManagedNetworkRecoveryRecord(record ManagedNetworkRecoveryRecord) error {
	if record.Schema != 2 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		record.OriginalExecutionEpoch == 0 ||
		!recordcodec.ValidSHA256(record.PlanHash) || !recordcodec.ValidSHA256(record.ProcedureSHA256) ||
		!recordcodec.ValidSHA256(record.PrimaryReportSHA256) ||
		record.PrimaryStatus == taskjournal.TaskStatusCompleted || !ValidTerminalTaskStatus(record.PrimaryStatus) ||
		!record.PrimaryResult.ReconciliationRequired ||
		!record.RecoveryDeadline.Equal(record.RecoveryDeadline.UTC()) || record.RecoveryDeadline.IsZero() ||
		record.EvidenceRevision <= 0 {
		return errs.New(errs.KindInternal, "managed network recovery record is corrupt")
	}
	primary, err := taskjournal.TaskResultFromData(&record.PrimaryResult)
	if err != nil || primary == nil {
		return errs.New(errs.KindInternal, "managed network recovery result is corrupt")
	}
	reportDigest, err := CanonicalPrimaryReportSHA256(record.PrimaryStatus, *primary)
	if err != nil || reportDigest != record.PrimaryReportSHA256 ||
		record.PrimaryResult.ExecutionEpoch != record.OriginalExecutionEpoch ||
		record.PrimaryResult.ReleaseRecoveryRecordSHA256 != "" {
		return errs.New(errs.KindInternal, "managed network recovery record is corrupt")
	}
	if len(record.ForwardStepIDs) == 0 || len(record.ForwardStepIDs) > 256 ||
		len(record.SettlementStepIDs) != len(record.ForwardStepIDs) || len(record.MutationEvidence) == 0 ||
		len(record.MutationEvidence) > len(record.ForwardStepIDs) {
		return errs.New(errs.KindInternal, "managed network recovery record is corrupt")
	}
	for _, steps := range [][]string{record.ForwardStepIDs, record.SettlementStepIDs} {
		seen := make(map[string]bool, len(steps))
		for _, stepID := range steps {
			if ids.Validate(ids.KindStep, stepID) != nil || seen[stepID] {
				return CorruptTaskAssignment()
			}
			seen[stepID] = true
		}
	}
	seen := make(map[string]bool, len(record.MutationEvidence))
	for _, evidence := range record.MutationEvidence {
		if !evidence.Running || seen[evidence.StepID] || !slices.Contains(record.ForwardStepIDs, evidence.StepID) {
			return CorruptTaskAssignment()
		}
		seen[evidence.StepID] = true
	}
	return nil
}

func EncodeManagedNetworkRecoveryRecord(record ManagedNetworkRecoveryRecord) ([]byte, error) {
	if err := validateManagedNetworkRecoveryRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("managed-network-recovery", record)
}

func DecodeManagedNetworkRecoveryRecord(value []byte) (ManagedNetworkRecoveryRecord, error) {
	record, err := recordcodec.Decode[ManagedNetworkRecoveryRecord](value, "managed-network-recovery")
	if err != nil || validateManagedNetworkRecoveryRecord(record) != nil {
		return ManagedNetworkRecoveryRecord{}, errs.New(errs.KindInternal, "managed network recovery record is corrupt")
	}
	return record, nil
}

func ManagedNetworkRecoveryRecordSHA256(record ManagedNetworkRecoveryRecord) (string, error) {
	if err := validateManagedNetworkRecoveryRecord(record); err != nil {
		return "", err
	}
	return domain.Digest(record)
}
