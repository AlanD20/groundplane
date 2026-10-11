package dnschecks

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Owner struct {
	TaskID      string `json:"task_id"`
	PlanID      string `json:"plan_id"`
	OperationID string `json:"operation_id"`
}

type Phase string

const (
	Ready    Phase = "ready"
	Checking Phase = "checking"
	Finished Phase = "finished"
)

// Record freezes the read-only exchange separately from replaceable results.
// Protobuf bytes retain exact typed input without storing generated Go's JSON
// shape as an additional public or persisted contract.
type Record struct {
	Owner               Owner      `json:"owner"`
	Source              Source     `json:"source"`
	Request             []byte     `json:"request"`
	ConfigurationSHA256 string     `json:"configuration_sha256"`
	Phase               Phase      `json:"phase"`
	Result              []byte     `json:"result,omitempty"`
	Outcome             string     `json:"outcome,omitempty"`
	Failure             string     `json:"failure,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	FinishedAt          *time.Time `json:"finished_at,omitempty"`
}

func NewRecord(owner Owner, source Source, request *agentpb.CheckDNSConnections, now time.Time) (Record, error) {
	if err := dnsconnectioncheck.ValidateRequest(request); err != nil {
		return Record{}, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return Record{}, errs.Wrap(errs.KindInternal, err)
	}
	record := Record{Owner: owner, Source: source, Request: encoded, Phase: Ready, CreatedAt: now}
	record.ConfigurationSHA256, err = configurationDigest(source, encoded)
	if err != nil {
		return Record{}, err
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (record Record) Input() (*agentpb.CheckDNSConnections, error) {
	if len(record.Request) == 0 || len(record.Request) > dnsconnectioncheck.MaximumRequestBytes {
		return nil, invalidRecord()
	}
	request := &agentpb.CheckDNSConnections{}
	if proto.Unmarshal(record.Request, request) != nil || dnsconnectioncheck.ValidateRequest(request) != nil {
		return nil, invalidRecord()
	}
	proof := request.PriorObservation
	if proof.ComponentId != record.Source.ComponentID || proof.ServiceId != record.Source.ServiceID ||
		proof.ArtifactId != record.Source.ArtifactID || proof.RenderGeneration != record.Source.RenderGeneration ||
		hex.EncodeToString(proof.ArtifactSha256) != record.Source.ArtifactSHA256 ||
		proof.ImageReference != record.Source.ImageReference ||
		hex.EncodeToString(proof.ImageConfigDigest) != record.Source.ImageConfigDigest {
		return nil, invalidRecord()
	}
	return request, nil
}

func (record Record) Observation() (*agentpb.DNSConnectionResult, error) {
	if len(record.Result) == 0 {
		return nil, nil
	}
	if len(record.Result) > dnsconnectioncheck.MaximumResultBytes {
		return nil, invalidRecord()
	}
	input, err := record.Input()
	if err != nil {
		return nil, err
	}
	result := &agentpb.DNSConnectionResult{}
	if proto.Unmarshal(record.Result, result) != nil || dnsconnectioncheck.ValidateResult(input, result) != nil {
		return nil, invalidRecord()
	}
	return result, nil
}

func (record Record) Validate() error {
	if ids.Validate(ids.KindTask, record.Owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, record.Owner.PlanID) != nil ||
		ids.Validate(ids.KindOperation, record.Owner.OperationID) != nil ||
		record.Source.Validate() != nil ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) ||
		!recordcodec.ValidSHA256(record.ConfigurationSHA256) {
		return invalidRecord()
	}
	if _, err := record.Input(); err != nil {
		return err
	}
	digest, err := configurationDigest(record.Source, record.Request)
	if err != nil || digest != record.ConfigurationSHA256 {
		return invalidRecord()
	}
	if record.StartedAt != nil &&
		(!recordcodec.IsCanonicalUTC(*record.StartedAt) || record.StartedAt.Before(record.CreatedAt)) ||
		record.FinishedAt != nil &&
			(!recordcodec.IsCanonicalUTC(*record.FinishedAt) || record.FinishedAt.Before(record.CreatedAt) ||
				record.StartedAt != nil && record.FinishedAt.Before(*record.StartedAt)) {
		return invalidRecord()
	}
	switch record.Phase {
	case Ready:
		if record.StartedAt != nil || record.FinishedAt != nil || len(record.Result) != 0 || record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
	case Checking:
		if record.StartedAt == nil || record.FinishedAt != nil || len(record.Result) != 0 || record.Outcome != "" ||
			record.Failure != "" {
			return invalidRecord()
		}
	case Finished:
		if record.FinishedAt == nil {
			return invalidRecord()
		}
		result, err := record.Observation()
		if err != nil {
			return err
		}
		if result != nil {
			outcome, failure := resultOutcome(result)
			if record.StartedAt == nil || record.Outcome != outcome || record.Failure != failure {
				return invalidRecord()
			}
		} else if !validStoppedFailure(record.Failure) || record.Outcome != "interrupted" {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func (record *Record) Start(now, deadline time.Time) error {
	if record.Phase != Ready || !recordcodec.IsCanonicalUTC(now) || now.Before(record.CreatedAt) ||
		!now.Before(deadline) {
		return errs.New(errs.KindStateConflict, "DNS check has already started or its deadline expired")
	}
	record.Phase, record.StartedAt = Checking, &now
	return record.Validate()
}

func (record *Record) Finish(result *agentpb.DNSConnectionResult, now time.Time) error {
	if record.Phase != Checking || result == nil {
		return errs.New(errs.KindStateConflict, "DNS check has no running exchange to finish")
	}
	input, err := record.Input()
	if err != nil {
		return err
	}
	if err := dnsconnectioncheck.ValidateResult(input, result); err != nil {
		return err
	}
	record.Result, err = proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	record.Outcome, record.Failure = resultOutcome(result)
	record.Phase, record.FinishedAt = Finished, &now
	return record.Validate()
}

func (record *Record) Stop(failure string, now time.Time) error {
	if record.Phase != Ready && record.Phase != Checking || !validStoppedFailure(failure) {
		return errs.New(errs.KindStateConflict, "DNS check cannot be interrupted in its current phase")
	}
	record.Phase, record.Outcome, record.Failure, record.FinishedAt = Finished, "interrupted", failure, &now
	return record.Validate()
}

func resultOutcome(result *agentpb.DNSConnectionResult) (string, string) {
	if result.Failure != "" {
		return "failed", result.Failure
	}
	for _, answer := range result.Answers {
		if answer.Outcome != "resolved" {
			return "failed", "dns_response"
		}
	}
	return "passed", ""
}

func validStoppedFailure(failure string) bool {
	switch failure {
	case "interrupted", "canceled", "timeout", "agent_unavailable", "execution_failed", "journal":
		return true
	default:
		return false
	}
}

func configurationDigest(source Source, request []byte) (string, error) {
	value, err := json.Marshal(struct {
		Source  Source `json:"source"`
		Request []byte `json:"request"`
	}{source, request})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func PlanHash(record Record) (string, error) {
	value, err := json.Marshal(struct {
		Procedure           string `json:"procedure"`
		Owner               Owner  `json:"owner"`
		ConfigurationSHA256 string `json:"configuration_sha256"`
	}{"dns-connections-v1", record.Owner, record.ConfigurationSHA256})
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
	value, err := recordcodec.Encode("dns-connection-check", record)
	// Leave room for native claim comparisons and transaction framing. Samples
	// retain total answer counts; storage never silently truncates a result.
	if err == nil && len(value) > keyvalue.MaximumBytes-(16<<10) {
		return nil, errs.New(errs.KindValidationFailed, "DNS check exceeds its durable result size limit")
	}
	return value, err
}

func Decode(value []byte) (Record, error) {
	if len(value) > keyvalue.MaximumBytes-(16<<10) {
		return Record{}, invalidRecord()
	}
	record, err := recordcodec.Decode[Record](value, "dns-connection-check")
	if err != nil || record.Validate() != nil {
		return Record{}, errs.New(errs.KindInternal, "DNS check record is corrupt")
	}
	return record, nil
}

func invalidRecord() error { return errs.New(errs.KindValidationFailed, "DNS check record is invalid") }
