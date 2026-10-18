// Package backingchecks owns captured consumer access checks and their exact
// helper cleanup authority. It never stores credential plaintext.
package backingchecks

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Owner struct {
	TaskID        string `json:"task_id"`
	WorkItemID    string `json:"work_item_id"`
	PlanID        string `json:"plan_id"`
	OperationID   string `json:"operation_id"`
	EnvironmentID string `json:"environment_id"`
}

// Source is safe captured configuration, including revisions for stale-result
// detection. Runtime contains the exact protobuf target, not a later lookup.
// Parameters must contain no password; the full request is encrypted separately.
type Source struct {
	Parameters              backingaccess.Request `json:"parameters"`
	AttachRevision          int64                 `json:"attach_revision"`
	CredentialRevision      int64                 `json:"credential_revision"`
	FactsRevision           int64                 `json:"facts_revision"`
	GrantID                 string                `json:"grant_id,omitempty"`
	GrantRevision           int64                 `json:"grant_revision,omitempty"`
	AgentID                 string                `json:"agent_id"`
	AgentRevision           int64                 `json:"agent_revision"`
	HelperImage             string                `json:"helper_image"`
	HelperImageID           string                `json:"helper_image_id"`
	ContainerID             string                `json:"container_id"`
	RuntimeRevision         int64                 `json:"runtime_revision"`
	ConsumerDesiredRevision int64                 `json:"consumer_desired_revision"`
	ConsumerIntentRevision  int64                 `json:"consumer_intent_revision"`
	BackingDesiredRevision  int64                 `json:"backing_desired_revision"`
	BackingRuntimeRevision  int64                 `json:"backing_runtime_revision"`
	BackingEnvironmentID    string                `json:"backing_environment_id"`
	Runtime                 []byte                `json:"runtime"`
	CredentialDigest        string                `json:"credential_digest"`
}

type Phase string

const (
	Ready           Phase = "ready"
	Checking        Phase = "checking"
	Cleaning        Phase = "cleaning"
	Finished        Phase = "finished"
	CleanupRequired Phase = "cleanup_required"
)

type Outcome string

const (
	Passed      Outcome = "passed"
	Failed      Outcome = "failed"
	Interrupted Outcome = "interrupted"
)

const (
	InterruptedFailure = "interrupted"
	JournalFailure     = "journal"
)

type Record struct {
	Schema              uint8                 `json:"schema"`
	Owner               Owner                 `json:"owner"`
	Source              Source                `json:"source"`
	ConfigurationSHA256 string                `json:"configuration_sha256"`
	EncryptedInput      []byte                `json:"encrypted_input,omitempty"`
	HelperTaskID        string                `json:"helper_task_id"`
	Phase               Phase                 `json:"phase"`
	CleanupPending      bool                  `json:"cleanup_pending"`
	ProbeRecorded       bool                  `json:"probe_recorded"`
	ProbeFailure        string                `json:"probe_failure,omitempty"`
	Observation         *backingaccess.Result `json:"observation,omitempty"`
	Outcome             Outcome               `json:"outcome,omitempty"`
	Failure             string                `json:"failure,omitempty"`
	CreatedAt           time.Time             `json:"created_at"`
	StartedAt           *time.Time            `json:"started_at,omitempty"`
	FinishedAt          *time.Time            `json:"finished_at,omitempty"`
}

func (source Source) Target() (*agentpb.ServiceObservationTarget, error) {
	if len(source.Runtime) == 0 || len(source.Runtime) > 16<<10 {
		return nil, invalidRecord()
	}
	target := &agentpb.ServiceObservationTarget{}
	if proto.Unmarshal(source.Runtime, target) != nil || serviceobservation.ValidateTarget(target) != nil ||
		target.DatabaseProbe != nil || target.EnvironmentId != source.Parameters.EnvironmentID ||
		target.ServiceId != source.Parameters.ServiceID {
		return nil, invalidRecord()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(target)
	if err != nil || !slices.Equal(canonical, source.Runtime) {
		return nil, invalidRecord()
	}
	return target, nil
}

func (source Source) Validate() error {
	if len(source.Parameters.Password) != 0 || source.Parameters.Validate() != nil ||
		source.AttachRevision <= 0 || source.CredentialRevision <= 0 || source.FactsRevision <= 0 ||
		source.AgentRevision <= 0 || source.RuntimeRevision <= 0 || ids.Validate(ids.KindAgent, source.AgentID) != nil ||
		source.ConsumerDesiredRevision <= 0 || source.BackingDesiredRevision <= 0 ||
		source.ConsumerIntentRevision < 0 || source.BackingRuntimeRevision < 0 ||
		ids.Validate(ids.KindEnvironment, source.BackingEnvironmentID) != nil ||
		!imageref.IsDigestPinned(source.HelperImage) || !validImageID(source.HelperImageID) ||
		!recordcodec.ValidSHA256(source.ContainerID) || !recordcodec.ValidSHA256(source.CredentialDigest) ||
		source.GrantID == "" && source.GrantRevision != 0 || source.GrantID != "" &&
		(ids.Validate(ids.KindAttach, source.GrantID) != nil || source.GrantRevision <= 0) {
		return invalidRecord()
	}
	_, err := source.Target()
	return err
}

func NewRecord(owner Owner, source Source, ciphertext []byte, createdAt time.Time) (Record, error) {
	if err := source.Validate(); err != nil {
		return Record{}, err
	}
	source.Runtime = slices.Clone(source.Runtime)
	source.Parameters.Password = slices.Clone(source.Parameters.Password)
	record := Record{Schema: 1, Owner: owner, Source: source, EncryptedInput: slices.Clone(ciphertext),
		HelperTaskID: owner.TaskID, Phase: Ready, CreatedAt: createdAt}
	var err error
	record.ConfigurationSHA256, err = sourceDigest(source)
	if err == nil {
		err = record.Validate()
	}
	if err != nil {
		record.Clear()
		return Record{}, err
	}
	return record, nil
}

func (record Record) Validate() error {
	if record.Schema != 1 || ids.Validate(ids.KindTask, record.Owner.TaskID) != nil || ids.Validate(ids.KindWorkItem, record.Owner.WorkItemID) != nil ||
		ids.Validate(
			ids.KindPlan,
			record.Owner.PlanID,
		) != nil || ids.Validate(ids.KindOperation, record.Owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, record.Owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.HelperTaskID) != nil || record.Source.Validate() != nil ||
		record.Source.Parameters.EnvironmentID != record.Owner.EnvironmentID || !validTime(record.CreatedAt) ||
		record.HelperTaskID != record.Owner.TaskID {
		return invalidRecord()
	}
	digest, err := sourceDigest(record.Source)
	if err != nil || digest != record.ConfigurationSHA256 {
		return invalidRecord()
	}
	if record.StartedAt != nil && (!validTime(*record.StartedAt) || record.StartedAt.Before(record.CreatedAt)) ||
		record.FinishedAt != nil && (!validTime(*record.FinishedAt) || record.FinishedAt.Before(record.CreatedAt) ||
			record.StartedAt != nil && record.FinishedAt.Before(*record.StartedAt)) {
		return invalidRecord()
	}
	if !validFailure(record.ProbeFailure) ||
		!record.ProbeRecorded && (record.ProbeFailure != "" || record.Observation != nil) ||
		record.ProbeRecorded && (record.StartedAt == nil || record.Observation == nil && record.ProbeFailure == "") ||
		record.Observation != nil &&
			(record.StartedAt == nil || record.Observation.ValidateFor(record.Source.Parameters) != nil) {
		return invalidRecord()
	}
	if record.Phase == Ready {
		if record.StartedAt != nil || record.FinishedAt != nil || record.ProbeRecorded || record.CleanupPending ||
			record.Outcome != "" || record.Failure != "" ||
			len(record.EncryptedInput) == 0 || len(record.EncryptedInput) > 256<<10 {
			return invalidRecord()
		}
		digest := sha256.Sum256(record.EncryptedInput)
		want, _ := hex.DecodeString(record.Source.CredentialDigest)
		if subtle.ConstantTimeCompare(digest[:], want) != 1 {
			return invalidRecord()
		}
		return nil
	}
	// Once Start commits, only the transient exchange owns plaintext. Cleanup
	// needs identities, not passwords, and must not retain an encrypted copy.
	if len(record.EncryptedInput) != 0 {
		return invalidRecord()
	}
	switch record.Phase {
	case Checking, Cleaning:
		if record.StartedAt == nil || record.FinishedAt != nil || record.Outcome != "" || record.Failure != "" ||
			record.Phase == Cleaning && !record.CleanupPending ||
			record.Phase == Checking && !record.ProbeRecorded && !record.CleanupPending {
			return invalidRecord()
		}
	case Finished:
		if record.FinishedAt == nil || record.CleanupPending {
			return invalidRecord()
		}
		if record.Outcome == Passed {
			if record.Failure != "" || record.ProbeFailure != "" || !record.ProbeRecorded ||
				!observationPassed(record.Observation) {
				return invalidRecord()
			}
		} else if record.Outcome != Failed && record.Outcome != Interrupted || !validFailure(record.Failure) || record.Failure == "" ||
			record.Outcome == Interrupted && (record.StartedAt == nil || record.Failure != InterruptedFailure) {
			return invalidRecord()
		}
	case CleanupRequired:
		if record.StartedAt == nil || record.FinishedAt == nil || !record.CleanupPending || record.Outcome != Failed ||
			record.Failure == "" || !validFailure(record.Failure) {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func observationPassed(result *backingaccess.Result) bool {
	return result != nil && len(result.Stages) == 3 && result.Stages[0].Outcome == backingaccess.Passed &&
		result.Stages[1].Outcome != backingaccess.Failed && result.Stages[2].Outcome == backingaccess.Passed
}

func validFailure(value string) bool {
	switch value {
	case "", InterruptedFailure, JournalFailure, backingaccess.ExecutionFailure, backingaccess.RuntimeUnavailable,
		backingaccess.OwnershipFailure, backingaccess.CleanupFailure, string(backingaccess.ConnectionFailure),
		string(backingaccess.AuthenticationFailed), string(backingaccess.AccessDenied), string(backingaccess.TimeoutFailure),
		string(backingaccess.Canceled), string(backingaccess.ProtocolFailure):
		return true
	default:
		return false
	}
}

func validTime(value time.Time) bool { return !value.IsZero() && value.Location() == time.UTC }
func validImageID(value string) bool {
	return strings.HasPrefix(value, "sha256:") && recordcodec.ValidSHA256(strings.TrimPrefix(value, "sha256:"))
}
func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "Backing access check record is invalid")
}

func sourceDigest(source Source) (string, error) {
	value, err := json.Marshal(source)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func PlanHash(record Record) (string, error) {
	value, err := json.Marshal(struct {
		Procedure           string `json:"procedure"`
		WorkItemID          string `json:"work_item_id"`
		PlanID              string `json:"plan_id"`
		OperationID         string `json:"operation_id"`
		EnvironmentID       string `json:"environment_id"`
		ConfigurationSHA256 string `json:"configuration_sha256"`
		HelperTaskID        string `json:"helper_task_id"`
	}{"backing-access", record.Owner.WorkItemID, record.Owner.PlanID, record.Owner.OperationID, record.Owner.EnvironmentID,
		record.ConfigurationSHA256, record.HelperTaskID})
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
	return recordcodec.Encode("backing-access-check", record)
}

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "backing-access-check")
	if err != nil || record.Validate() != nil {
		record.Clear()
		return Record{}, errs.New(errs.KindInternal, "Backing access check record is corrupt")
	}
	return record, nil
}

func (record *Record) Clear() {
	if record != nil {
		clear(record.EncryptedInput)
		record.EncryptedInput = nil
		record.Source.Parameters.Clear()
	}
}

func Clone(record Record) Record {
	copyOf := record
	copyOf.EncryptedInput, copyOf.Source.Runtime = slices.Clone(
		record.EncryptedInput,
	), slices.Clone(
		record.Source.Runtime,
	)
	copyOf.Source.Parameters.Password = slices.Clone(record.Source.Parameters.Password)
	if record.Observation != nil {
		result := *record.Observation
		result.Stages = slices.Clone(result.Stages)
		copyOf.Observation = &result
	}
	if record.StartedAt != nil {
		at := *record.StartedAt
		copyOf.StartedAt = &at
	}
	if record.FinishedAt != nil {
		at := *record.FinishedAt
		copyOf.FinishedAt = &at
	}
	return copyOf
}
