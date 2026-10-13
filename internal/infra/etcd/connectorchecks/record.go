// Package connectorchecks owns the captured input and cleanup journal of one
// native Controller Connector permission-check Task.
package connectorchecks

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Owner struct {
	TaskID        string `json:"task_id"`
	PlanID        string `json:"plan_id"`
	OperationID   string `json:"operation_id"`
	EnvironmentID string `json:"environment_id"`
}

type SecretSource struct {
	Name      string                     `json:"name"`
	Reference string                     `json:"reference"`
	Pin       tasksecretpinrecord.Record `json:"pin"`
}

type Phase string

const (
	Ready           Phase = "ready"
	Checking        Phase = "checking"
	Cleaning        Phase = "cleaning"
	Finished        Phase = "finished"
	CleanupRequired Phase = "cleanup_required"
	Transferred     Phase = "transferred"
)

type Outcome string

const (
	Passed      Outcome = "passed"
	Failed      Outcome = "failed"
	Interrupted Outcome = "interrupted"
)

// Credential plaintext never enters this record. Credentials contains the
// encrypted resolved pair, including Secret-backed values protected by Pins.
// ConnectorRevision is zero for an unsaved draft; it cannot validate a saved
// Connector merely because the generated draft ID has the same settings.
type Record struct {
	Schema              uint8                                 `json:"schema"`
	Owner               Owner                                 `json:"owner"`
	Connector           connectorrecord.Record                `json:"connector"`
	ConnectorRevision   int64                                 `json:"connector_revision"`
	Draft               bool                                  `json:"draft"`
	CredentialDigest    string                                `json:"credential_digest"`
	ConfigurationSHA256 string                                `json:"configuration_sha256"`
	SecretSources       []SecretSource                        `json:"secret_sources"`
	Credentials         *connectorrecord.EncryptedCredentials `json:"credentials,omitempty"`
	Phase               Phase                                 `json:"phase"`
	Cleanup             s3connector.ConnectionCheckCleanup    `json:"cleanup"`
	Stages              []s3connector.ConnectionCheckStage    `json:"stages,omitempty"`
	Outcome             Outcome                               `json:"outcome,omitempty"`
	Failure             s3connector.ConnectionCheckFailure    `json:"failure,omitempty"`
	CreatedAt           time.Time                             `json:"created_at"`
	StartedAt           *time.Time                            `json:"started_at,omitempty"`
	FinishedAt          *time.Time                            `json:"finished_at,omitempty"`
	RetryOf             string                                `json:"retry_of,omitempty"`
	TransferredTo       string                                `json:"transferred_to,omitempty"`
}

func Key(taskID string) string { return "/v1/private/connector-checks/" + taskID }

func OperationKey(operationID string) string {
	return "/v1/indexes/connector-checks/by-operation/" + operationID
}

func LatestKey(connectorID string) string {
	return "/v1/indexes/connector-checks/latest/" + connectorID
}

func ReferencePrefix(connectorID string) string {
	return "/v1/indexes/connector-checks/by-connector/" + connectorID + "/"
}

func ReferenceKey(connectorID, taskID string) string { return ReferencePrefix(connectorID) + taskID }

func EnvironmentReferencePrefix(environmentID string) string {
	return "/v1/indexes/connector-checks/by-environment/" + environmentID + "/"
}

func EnvironmentReferenceKey(environmentID, taskID string) string {
	return EnvironmentReferencePrefix(environmentID) + taskID
}

func NewRecord(owner Owner, connector connectorrecord.Record, revision int64, draft bool,
	credentials connectorrecord.EncryptedCredentials, sources []SecretSource, createdAt time.Time,
) (Record, error) {
	connector, err := connectorrecord.NewRecord(connector.Connector)
	if err != nil {
		return Record{}, err
	}
	captured := credentials
	captured.Ciphertext = slices.Clone(credentials.Ciphertext)
	record := Record{Schema: 1, Owner: owner, Connector: connector, ConnectorRevision: revision, Draft: draft,
		CredentialDigest: credentials.CiphertextSHA256, SecretSources: slices.Clone(sources),
		Credentials: &captured, Phase: Ready, CreatedAt: createdAt}
	record.Cleanup, err = s3connector.NewConnectionCheckCleanup(owner.OperationID, connector.Connector.Prefix)
	if err == nil {
		record.ConfigurationSHA256, err = configurationDigest(record)
	}
	if err == nil {
		err = record.Validate()
	}
	if err != nil {
		record.Clear()
		return Record{}, err
	}
	return record, nil
}

func configurationDigest(record Record) (string, error) {
	encoded, err := json.Marshal(struct {
		Connector        connectorrecord.Record `json:"connector"`
		Revision         int64                  `json:"revision"`
		Draft            bool                   `json:"draft"`
		CredentialDigest string                 `json:"credential_digest"`
		Sources          []SecretSource         `json:"sources"`
	}{record.Connector, record.ConnectorRevision, record.Draft, record.CredentialDigest, record.SecretSources})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (record Record) Validate() error {
	owner, connector := record.Owner, record.Connector.Connector
	if record.Schema != 1 || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil || ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		connectorrecord.ValidateRecord(record.Connector) != nil || connector.EnvironmentID != owner.EnvironmentID ||
		record.ConnectorRevision < 0 || record.ConnectorRevision == 0 && !record.Draft || !recordcodec.ValidSHA256(record.CredentialDigest) ||
		!validTime(record.CreatedAt) || record.Cleanup.Validate(connector.Prefix) != nil ||
		record.Cleanup.OperationID != owner.OperationID || len(record.Stages) > 4 || len(record.SecretSources) > 2 {
		return invalidRecord()
	}
	digest, err := configurationDigest(record)
	if err != nil || digest != record.ConfigurationSHA256 || !validSecretSources(record) {
		return invalidRecord()
	}
	for index, stage := range record.Stages {
		if !stage.Valid() || stage.Name == s3connector.CheckCleanup && index != len(record.Stages)-1 ||
			stage.Name != s3connector.CheckCleanup && (index >= 3 ||
				stage.Name != []s3connector.ConnectionCheckStageName{s3connector.CheckWriteObject,
					s3connector.CheckReadObject, s3connector.CheckMultipart}[index] ||
				index > 0 && !record.Stages[index-1].Succeeded) {
			return invalidRecord()
		}
	}
	if record.StartedAt != nil && (!validTime(*record.StartedAt) || record.StartedAt.Before(record.CreatedAt)) {
		return invalidRecord()
	}
	if record.RetryOf != "" && (ids.Validate(ids.KindTask, record.RetryOf) != nil || record.RetryOf == owner.TaskID) {
		return invalidRecord()
	}
	if record.Phase == Transferred {
		if ids.Validate(ids.KindTask, record.TransferredTo) != nil || record.TransferredTo == owner.TaskID ||
			record.Credentials != nil || record.StartedAt == nil || !record.RequiresCleanup() ||
			record.FinishedAt == nil || !validTime(*record.FinishedAt) || record.FinishedAt.Before(*record.StartedAt) ||
			record.Outcome != Failed || !validFailure(record.Failure) {
			return invalidRecord()
		}
		return nil
	}
	if record.TransferredTo != "" {
		return invalidRecord()
	}
	if record.Phase == CleanupRequired {
		if record.Credentials == nil || connectorrecord.ValidateEncryptedCredentials(*record.Credentials) != nil ||
			record.Credentials.ConnectorID != connector.ID || record.Credentials.CiphertextSHA256 != record.CredentialDigest ||
			record.StartedAt == nil || !record.RequiresCleanup() || record.Outcome != Failed || !validFailure(record.Failure) ||
			record.FinishedAt == nil || !validTime(*record.FinishedAt) || record.FinishedAt.Before(*record.StartedAt) {
			return invalidRecord()
		}
		return nil
	}
	if record.Phase == Finished {
		if record.Credentials != nil || record.FinishedAt == nil || !validTime(*record.FinishedAt) ||
			record.FinishedAt.Before(record.CreatedAt) || cleanupPending(record.Cleanup) ||
			record.StartedAt != nil && record.FinishedAt.Before(*record.StartedAt) ||
			record.StartedAt == nil && (record.Outcome != Failed || len(record.Stages) != 0) {
			return invalidRecord()
		}
		switch record.Outcome {
		case Passed:
			if !completePassedStages(record.Stages) || record.Failure != s3connector.CheckFailureNone {
				return invalidRecord()
			}
		case Failed, Interrupted:
			if !(s3connector.ConnectionCheckStage{Name: s3connector.CheckCleanup, Failure: record.Failure}).Valid() ||
				record.Outcome == Interrupted && record.Failure != s3connector.CheckFailureInterrupted {
				return invalidRecord()
			}
		default:
			return invalidRecord()
		}
		return nil
	}
	if record.Credentials == nil || connectorrecord.ValidateEncryptedCredentials(*record.Credentials) != nil ||
		record.Credentials.ConnectorID != connector.ID || record.Credentials.CiphertextSHA256 != record.CredentialDigest ||
		record.Outcome != "" || record.Failure != s3connector.CheckFailureNone || record.FinishedAt != nil ||
		len(record.Cleanup.UploadIDs) != 0 && !record.Cleanup.MultipartPending {
		return invalidRecord()
	}
	switch record.Phase {
	case Ready:
		if record.StartedAt != nil || len(record.Stages) != 0 || cleanupPending(record.Cleanup) {
			return invalidRecord()
		}
	case Checking, Cleaning:
		if record.StartedAt == nil {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func validSecretSources(record Record) bool {
	want := make(map[string]string)
	for name, credential := range record.Connector.Connector.Credentials {
		if credential.SecretRef != "" {
			want[string(name)] = credential.SecretRef
		}
	}
	if len(want) != len(record.SecretSources) {
		return false
	}
	for _, source := range record.SecretSources {
		if want[source.Name] == "" || want[source.Name] != source.Reference ||
			tasksecretpinrecord.Validate(source.Pin) != nil || source.Pin.OperationID != record.Owner.OperationID {
			return false
		}
		delete(want, source.Name)
	}
	return len(want) == 0
}

func completePassedStages(stages []s3connector.ConnectionCheckStage) bool {
	names := []s3connector.ConnectionCheckStageName{s3connector.CheckWriteObject, s3connector.CheckReadObject,
		s3connector.CheckMultipart, s3connector.CheckCleanup}
	if len(stages) != len(names) {
		return false
	}
	for index, stage := range stages {
		if stage.Name != names[index] || !stage.Succeeded {
			return false
		}
	}
	return true
}

func cleanupPending(cleanup s3connector.ConnectionCheckCleanup) bool {
	return cleanup.ObjectPending || cleanup.MultipartPending || len(cleanup.UploadIDs) != 0
}

func (record Record) RequiresCleanup() bool { return cleanupPending(record.Cleanup) }

func validFailure(failure s3connector.ConnectionCheckFailure) bool {
	return (s3connector.ConnectionCheckStage{Name: s3connector.CheckCleanup, Failure: failure}).Valid()
}

// Stop is joined to native terminal publication. Pending effects retain their
// original input for an explicit cleanup Retry; they never retain the queue claim.
func (record *Record) Stop(failure s3connector.ConnectionCheckFailure, now time.Time) error {
	if record.Phase != Ready && record.Phase != Checking && record.Phase != Cleaning ||
		!validFailure(failure) || !validTime(now) {
		return errs.New(errs.KindStateConflict, "connector check cannot stop in this phase")
	}
	if now.Before(record.CreatedAt) {
		now = record.CreatedAt
	}
	if record.StartedAt != nil && now.Before(*record.StartedAt) {
		now = *record.StartedAt
	}
	record.Outcome, record.Failure, record.FinishedAt = Failed, failure, &now
	if record.RequiresCleanup() {
		record.Phase = CleanupRequired
	} else {
		record.Clear()
		record.Phase = Finished
	}
	return record.Validate()
}

func validTime(value time.Time) bool { return !value.IsZero() && value.Location() == time.UTC }

func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "connector connection-check record is invalid")
}

func Encode(record Record) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("connector-connection-check", record)
}

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "connector-connection-check")
	if err != nil || record.Validate() != nil {
		record.Clear()
		return Record{}, errs.New(errs.KindInternal, "connector connection-check record is corrupt")
	}
	return record, nil
}

func (record *Record) Clear() {
	if record != nil && record.Credentials != nil {
		clear(record.Credentials.Ciphertext)
		record.Credentials = nil
	}
}

func PlanHash(record Record) (string, error) {
	encoded, err := json.Marshal(struct {
		Procedure           string `json:"procedure"`
		PlanID              string `json:"plan_id"`
		OperationID         string `json:"operation_id"`
		EnvironmentID       string `json:"environment_id"`
		ConfigurationSHA256 string `json:"configuration_sha256"`
	}{"connector-check-v1", record.Owner.PlanID, record.Owner.OperationID, record.Owner.EnvironmentID, record.ConfigurationSHA256})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
