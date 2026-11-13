// Package runnerupdates owns captured Runner updates and retained results.
package runnerupdates

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumBytes = 128 << 10

type Phase string

const (
	Ready    Phase = "ready"
	Updating Phase = "updating"
	Finished Phase = "finished"
)

type Owner struct {
	TaskID      string `json:"task_id"`
	WorkItemID  string `json:"work_item_id"`
	PlanID      string `json:"plan_id"`
	OperationID string `json:"operation_id"`
}
type Record struct {
	Owner       Owner                        `json:"owner"`
	Source      runners.CapturedRuntime      `json:"source"`
	Plan        runnerallocation.RuntimePlan `json:"plan"`
	Request     runnersoftware.Request       `json:"request"`
	InputSHA256 string                       `json:"input_sha256"`
	Phase       Phase                        `json:"phase"`
	Result      *runnersoftware.Result       `json:"result,omitempty"`
	Outcome     string                       `json:"outcome,omitempty"`
	Failure     string                       `json:"failure,omitempty"`
	CreatedAt   time.Time                    `json:"created_at"`
	StartedAt   *time.Time                   `json:"started_at,omitempty"`
	FinishedAt  *time.Time                   `json:"finished_at,omitempty"`
}

func NewRecord(
	owner Owner,
	source runners.CapturedRuntime,
	plan runnerallocation.RuntimePlan,
	request runnersoftware.Request,
	at time.Time,
) (Record, error) {
	source.Desired.Labels = slices.Clone(source.Desired.Labels)
	record := Record{Owner: owner, Source: source, Plan: plan.Clone(), Request: request, Phase: Ready, CreatedAt: at}
	value, err := record.inputDigest()
	if err != nil {
		return Record{}, err
	}
	record.InputSHA256 = value
	return record, record.Validate()
}

func (record Record) inputDigest() (string, error) {
	value, err := json.Marshal(struct {
		Source  runners.CapturedRuntime
		Plan    runnerallocation.RuntimePlan
		Request runnersoftware.Request
	}{record.Source, record.Plan, record.Request})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func (record Record) Validate() error {
	source, plan, request := record.Source, record.Plan, record.Request
	digest, err := record.inputDigest()
	if ids.Validate(ids.KindTask, record.Owner.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, record.Owner.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, record.Owner.PlanID) != nil ||
		ids.Validate(ids.KindOperation, record.Owner.OperationID) != nil ||
		source.Validate() != nil ||
		request.Validate() != nil ||
		err != nil ||
		digest != record.InputSHA256 ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) ||
		plan.RunnerID != source.RunnerID ||
		plan.TenantID != source.Desired.TenantID ||
		plan.OwnerKind != string(source.Desired.OwnerKind) ||
		plan.OwnerID != source.Desired.OwnerID ||
		plan.Container.ImageRef != source.Desired.ImageRef ||
		plan.Container.GitHubURL != source.Desired.GitHubURL || plan.Container.RunnerGroup != source.Desired.RunnerGroup || plan.Container.RunnerName != runnerallocation.RegistrationName(source.Desired.ID, source.Desired.GitHubName) || plan.CPUMillis != source.Desired.CPUMillis || plan.MemoryBytes != source.Desired.MemoryBytes ||
		!slices.Equal(plan.Container.Labels, source.Desired.Labels) ||
		plan.RuntimeEpoch != source.Ownership.RuntimeEpoch ||
		plan.Paths.RawSocket != runnerallocation.DaemonSocketPath(source.RunnerID) ||
		source.Ownership.DaemonSocketEndpoint != "unix://"+plan.Paths.RawSocket ||
		request.RunnerID != source.RunnerID ||
		request.RuntimeEpoch != plan.RuntimeEpoch ||
		request.PlanSHA256 != plan.Digest() ||
		request.RunnerHome != plan.Paths.RunnerHome ||
		request.ControllerEndpoint != plan.Egress.ControllerEndpoint.String() {
		return errs.New(errs.KindValidationFailed, "Runner update record is invalid")
	}
	if record.StartedAt != nil &&
		(!recordcodec.IsCanonicalUTC(*record.StartedAt) || record.StartedAt.Before(record.CreatedAt)) ||
		record.FinishedAt != nil &&
			(!recordcodec.IsCanonicalUTC(*record.FinishedAt) || record.FinishedAt.Before(record.CreatedAt) || record.StartedAt != nil && record.FinishedAt.Before(*record.StartedAt)) {
		return errs.New(errs.KindValidationFailed, "Runner update timestamps are invalid")
	}
	switch record.Phase {
	case Ready:
		if record.StartedAt != nil || record.FinishedAt != nil || record.Result != nil || record.Outcome != "" ||
			record.Failure != "" {
			return invalidPhase()
		}
	case Updating:
		if record.StartedAt == nil || record.FinishedAt != nil || record.Result != nil || record.Outcome != "" ||
			record.Failure != "" {
			return invalidPhase()
		}
	case Finished:
		if record.FinishedAt == nil {
			return invalidPhase()
		}
		if record.Result != nil {
			if record.StartedAt == nil || record.Result.Validate(request) != nil {
				return invalidPhase()
			}
			outcome, failure := observedOutcome(*record.Result)
			if record.Outcome != outcome || record.Failure != failure {
				return invalidPhase()
			}
		} else if record.Outcome != "interrupted" || !validInterruption(record.Failure) {
			return invalidPhase()
		}
	default:
		return invalidPhase()
	}
	return nil
}

func observedOutcome(result runnersoftware.Result) (string, string) {
	for _, stage := range result.Stages {
		if stage.Outcome != "passed" {
			return "failed", stage.Name
		}
	}
	return "passed", ""
}
func validInterruption(value string) bool {
	switch value {
	case "interrupted", "canceled", "timeout", "runtime_changed", "execution_failed", "journal":
		return true
	default:
		return false
	}
}
func invalidPhase() error {
	return errs.New(errs.KindValidationFailed, "Runner update phase or result is invalid")
}

func (record *Record) Start(at, deadline time.Time) error {
	if record.Validate() != nil || record.Phase != Ready || at.Before(record.CreatedAt) || !at.Before(deadline) {
		return invalidPhase()
	}
	record.Phase, record.StartedAt = Updating, &at
	return record.Validate()
}
func (record *Record) Finish(result runnersoftware.Result, at time.Time) error {
	if record.Validate() != nil || record.Phase != Updating || result.Validate(record.Request) != nil {
		return invalidPhase()
	}
	record.Result, record.FinishedAt, record.Phase = &result, &at, Finished
	record.Outcome, record.Failure = observedOutcome(result)
	return record.Validate()
}
func (record *Record) Stop(failure string, at time.Time) error {
	if record.Validate() != nil || record.Phase == Finished || !validInterruption(failure) {
		return invalidPhase()
	}
	record.Phase, record.FinishedAt, record.Outcome, record.Failure = Finished, &at, "interrupted", failure
	return record.Validate()
}
func PlanHash(record Record) (string, error) {
	if err := record.Validate(); err != nil {
		return "", err
	}
	value, err := json.Marshal(struct {
		Procedure string
		Owner     Owner
		Input     string
	}{"runner-software-v1", record.Owner, record.InputSHA256})
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
	value, err := recordcodec.Encode("runner_update", record)
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumBytes {
		return nil, errs.New(errs.KindResourceInUse, "Runner update capture exceeds its storage bound")
	}
	return value, nil
}
func Decode(value []byte) (Record, error) {
	if len(value) > MaximumBytes {
		return Record{}, errs.New(errs.KindInternal, "Runner update record exceeds its bound")
	}
	record, err := recordcodec.Decode[Record](value, "runner_update")
	if err != nil || record.Validate() != nil {
		return Record{}, errs.New(errs.KindInternal, "Runner update record is corrupt")
	}
	return record, nil
}
