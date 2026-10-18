package edgechecks

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumRecordBytes = 768 << 10

type Owner struct {
	TaskID        string `json:"task_id"`
	WorkItemID    string `json:"work_item_id"`
	PlanID        string `json:"plan_id"`
	OperationID   string `json:"operation_id"`
	EnvironmentID string `json:"environment_id"`
}

func (owner Owner) Validate() error {
	if ids.Validate(ids.KindTask, owner.TaskID) != nil || ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil || ids.Validate(ids.KindPlan, owner.PlanID) != nil ||
		ids.Validate(
			ids.KindOperation,
			owner.OperationID,
		) != nil || ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil {
		return invalidRecord()
	}
	return nil
}

type Phase string

const (
	Ready           Phase = "ready"
	PrivateChecking Phase = "private_checking"
	PublicChecking  Phase = "public_checking"
	Cleaning        Phase = "cleaning"
	CleanupRequired Phase = "cleanup_required"
	Finished        Phase = "finished"
)

type Record struct {
	Owner               Owner                   `json:"owner"`
	Source              Source                  `json:"source"`
	ConfigurationSHA256 string                  `json:"configuration_sha256"`
	HelperTaskID        string                  `json:"helper_task_id"`
	Phase               Phase                   `json:"phase"`
	CleanupPending      bool                    `json:"cleanup_pending"`
	Private             *contract.PrivateResult `json:"private,omitempty"`
	Public              *contract.PublicResult  `json:"public,omitempty"`
	PrivateFailure      string                  `json:"private_failure,omitempty"`
	PublicFailure       string                  `json:"public_failure,omitempty"`
	Outcome             string                  `json:"outcome,omitempty"`
	Failure             string                  `json:"failure,omitempty"`
	CreatedAt           time.Time               `json:"created_at"`
	StartedAt           *time.Time              `json:"started_at,omitempty"`
	PublicStartedAt     *time.Time              `json:"public_started_at,omitempty"`
	FinishedAt          *time.Time              `json:"finished_at,omitempty"`
}

func NewRecord(owner Owner, source Source, now time.Time) (Record, error) {
	record := Record{
		Owner:        owner,
		Source:       cloneSource(source),
		HelperTaskID: owner.TaskID,
		Phase:        Ready,
		CreatedAt:    now,
	}
	var err error
	record.ConfigurationSHA256, err = sourceDigest(record.Source)
	if err != nil {
		return Record{}, err
	}
	return record, record.Validate()
}

func (record Record) Validate() error {
	if record.Owner.Validate() != nil || record.Source.Validate() != nil ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) ||
		record.Owner.EnvironmentID != record.Source.Request.EnvironmentID ||
		record.Owner.OperationID != record.Source.Request.RequestID ||
		ids.Validate(ids.KindTask, record.HelperTaskID) != nil ||
		!recordcodec.ValidSHA256(record.ConfigurationSHA256) ||
		!validFailure(record.PrivateFailure) ||
		!validFailure(record.PublicFailure) ||
		!validFailure(record.Failure) {
		return invalidRecord()
	}
	digest, err := sourceDigest(record.Source)
	if err != nil || digest != record.ConfigurationSHA256 {
		return invalidRecord()
	}
	if record.HelperTaskID != record.Owner.TaskID {
		return invalidRecord()
	}
	for _, instant := range []*time.Time{record.StartedAt, record.PublicStartedAt, record.FinishedAt} {
		if instant != nil &&
			(!recordcodec.IsCanonicalUTC(*instant) || instant.Before(record.CreatedAt) || record.StartedAt != nil && instant.Before(*record.StartedAt)) {
			return invalidRecord()
		}
	}
	if record.FinishedAt != nil && record.PublicStartedAt != nil && record.FinishedAt.Before(*record.PublicStartedAt) {
		return invalidRecord()
	}
	if record.Private != nil && (record.StartedAt == nil || record.Private.Validate(record.Source.Request) != nil) ||
		record.Public != nil &&
			(record.PublicStartedAt == nil || record.Public.Validate(record.Source.Request.Target) != nil) ||
		record.PublicStartedAt != nil &&
			(record.StartedAt == nil || record.Private == nil && record.PrivateFailure == "") {
		return invalidRecord()
	}
	switch record.Phase {
	case Ready:
		if record.StartedAt != nil || record.PublicStartedAt != nil || record.FinishedAt != nil ||
			record.Private != nil ||
			record.Public != nil ||
			record.CleanupPending ||
			record.PrivateFailure != "" ||
			record.PublicFailure != "" ||
			record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
	case PrivateChecking:
		if record.StartedAt == nil || record.PublicStartedAt != nil || record.FinishedAt != nil ||
			record.Private != nil ||
			record.Public != nil ||
			!record.CleanupPending ||
			record.PrivateFailure != "" ||
			record.PublicFailure != "" ||
			record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
	case PublicChecking:
		if record.StartedAt == nil || record.FinishedAt != nil ||
			record.Private == nil && record.PrivateFailure == "" ||
			record.CleanupPending ||
			record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
		if record.PublicStartedAt == nil && (record.Public != nil || record.PublicFailure != "") {
			return invalidRecord()
		}
	case Cleaning:
		if record.StartedAt == nil || !record.CleanupPending || record.FinishedAt != nil || record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
	case CleanupRequired, Finished:
		if record.FinishedAt == nil || record.Phase == Finished && record.CleanupPending ||
			record.Phase != Finished && !record.CleanupPending {
			return invalidRecord()
		}
		if record.Outcome == "passed" || record.Outcome == "protected" {
			outcome, failure := observationOutcome(record)
			if record.Phase != Finished || record.Failure != "" || record.Outcome != outcome || failure != "" {
				return invalidRecord()
			}
		} else if (record.Outcome != "failed" && record.Outcome != "interrupted") || record.Failure == "" {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func validFailure(failure string) bool {
	switch failure {
	case "",
		"interrupted",
		"canceled",
		"timeout",
		"agent_unavailable",
		"execution_failed",
		"configuration_changed",
		"cleanup",
		"journal",
		string(
			contract.ConnectionFailure,
		),
		string(contract.DNSFailure),
		string(contract.UnsafeAddress),
		string(contract.TLSFailure),
		string(contract.HTTPFailure),
		string(contract.NotReady),
		string(contract.ProtocolFailure):
		return true
	default:
		return false
	}
}

func observationOutcome(record Record) (string, string) {
	if record.PrivateFailure != "" {
		return "failed", record.PrivateFailure
	}
	if record.PublicFailure != "" {
		return "failed", record.PublicFailure
	}
	if record.Private == nil || record.Public == nil {
		return "interrupted", "interrupted"
	}
	protected := false
	for _, stages := range [][]contract.Stage{record.Private.Stages, record.Public.Stages} {
		for _, stage := range stages {
			if stage.Outcome == contract.Failed {
				return "failed", string(stage.Failure)
			}
			if stage.Outcome == contract.Skipped {
				return "interrupted", "interrupted"
			}
			if stage.Outcome == contract.Protected {
				protected = true
			}
		}
	}
	if protected {
		return "protected", ""
	}
	return "passed", ""
}

func Clone(record Record) Record {
	record.Source = cloneSource(record.Source)
	if record.Private != nil {
		value := *record.Private
		value.Stages = slices.Clone(value.Stages)
		record.Private = &value
	}
	if record.Public != nil {
		value := *record.Public
		value.Stages, value.Addresses = slices.Clone(value.Stages), slices.Clone(value.Addresses)
		if value.CertificateExpiresAt != nil {
			instant := *value.CertificateExpiresAt
			value.CertificateExpiresAt = &instant
		}
		record.Public = &value
	}
	if record.StartedAt != nil {
		instant := *record.StartedAt
		record.StartedAt = &instant
	}
	if record.PublicStartedAt != nil {
		instant := *record.PublicStartedAt
		record.PublicStartedAt = &instant
	}
	if record.FinishedAt != nil {
		instant := *record.FinishedAt
		record.FinishedAt = &instant
	}
	return record
}

func sourceDigest(source Source) (string, error) {
	value, err := json.Marshal(source)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func PlanHash(record Record) (string, error) {
	if record.Validate() != nil {
		return "", invalidRecord()
	}
	value, err := json.Marshal(struct {
		Procedure           string `json:"procedure"`
		PlanID              string `json:"plan_id"`
		WorkItemID          string `json:"work_item_id"`
		OperationID         string `json:"operation_id"`
		EnvironmentID       string `json:"environment_id"`
		ConfigurationSHA256 string `json:"configuration_sha256"`
		HelperTaskID        string `json:"helper_task_id"`
	}{"edge-connections-v1", record.Owner.PlanID, record.Owner.WorkItemID, record.Owner.OperationID, record.Owner.EnvironmentID, record.ConfigurationSHA256, record.HelperTaskID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func Encode(record Record) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("edge-connection-check", record)
	if err == nil && len(value) > MaximumRecordBytes {
		return nil, errs.New(errs.KindValidationFailed, "edge diagnostic exceeds its durable result size limit")
	}
	return value, err
}

func Decode(value []byte) (Record, error) {
	if len(value) > MaximumRecordBytes {
		return Record{}, errs.New(errs.KindInternal, "edge diagnostic record exceeds its limit")
	}
	record, err := recordcodec.Decode[Record](value, "edge-connection-check")
	if err != nil || record.Validate() != nil {
		return Record{}, errs.New(errs.KindInternal, "edge diagnostic record is corrupt")
	}
	return record, nil
}
