package runnergrants

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskProvenanceKind = "runner_task_provenance"

// TaskProvenance captures one scoped Runner mutation as part of native
// acceptance. It authorizes polling only, without adding any Task effect.
type TaskProvenance struct {
	RunnerID           string               `json:"runner_id"`
	RunnerEpoch        uint64               `json:"runner_epoch"`
	GrantRevision      int64                `json:"grant_revision"`
	CredentialRevision int64                `json:"credential_revision"`
	ExpiresAt          time.Time            `json:"expires_at"`
	TokenDigest        string               `json:"token_digest"`
	Scope              workexecution.Scope  `json:"scope"`
	ServiceID          string               `json:"service_id"`
	Action             workexecution.Action `json:"action"`
	Image              string               `json:"image"`
}

type TaskProvenanceRecord struct {
	Authority TaskProvenance      `json:"authority"`
	TaskID    string              `json:"task_id"`
	Intent    workexecution.Input `json:"intent"`
}

func (authority TaskProvenance) ValidateFetch(serviceID, image string) error {
	return authority.validateAction(workexecution.ActionFetch, serviceID, image)
}

func (authority TaskProvenance) ValidateDeploy(serviceID, image string) error {
	return authority.validateAction(workexecution.ActionDeploy, serviceID, image)
}

func (authority TaskProvenance) validateAction(action workexecution.Action, serviceID, image string) error {
	if !authority.valid() || authority.Action != action || authority.ServiceID != serviceID ||
		authority.Image != image {
		return errs.New(errs.KindValidationFailed, "Runner Task does not match its Service and image grant")
	}
	return nil
}

func (authority TaskProvenance) valid() bool {
	if ids.Validate(ids.KindRunner, authority.RunnerID) != nil ||
		ids.Validate(ids.KindService, authority.ServiceID) != nil ||
		authority.RunnerEpoch == 0 ||
		authority.GrantRevision <= 0 ||
		authority.CredentialRevision <= 0 ||
		authority.ExpiresAt.IsZero() ||
		!validTokenDigest(authority.TokenDigest) ||
		!validScope(authority.Scope) ||
		authority.Action != workexecution.ActionFetch && authority.Action != workexecution.ActionDeploy ||
		!strings.HasPrefix(authority.Image, imagefetch.RegistryAuthority+"/") ||
		!imageref.IsDigestPinned(authority.Image) {
		return false
	}
	_, err := imagefetch.ParseRequested(authority.Image)
	return err == nil
}

func (authority TaskProvenance) PublicationForTask(task workexecution.Task) (workqueue.Change, error) {
	if task.AcceptedIntent == nil || !task.PendingPreparation() {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner Task provenance requires an accepted input",
		)
	}
	if !authority.ExpiresAt.After(time.Now()) {
		return workqueue.Change{}, errs.New(errs.KindRequestForbidden, "Runner Task authority has expired")
	}
	record := TaskProvenanceRecord{Authority: authority, TaskID: task.ID, Intent: *task.AcceptedIntent}
	if !record.valid() || !record.MatchesTask(task) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner Task provenance does not match native acceptance",
		)
	}
	value, err := recordcodec.Encode(taskProvenanceKind, record)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := TaskProvenanceKey(task.ID)
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: key},
			{Key: Key(authority.RunnerID), ModRevision: authority.GrantRevision},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}

// The accepted input prefix is collected with native Task history.
func TaskProvenanceKey(taskID string) string {
	return acceptedactions.Prefix(taskID) + "runner-provenance"
}

func (repository *Repository) ReadTaskProvenance(
	ctx context.Context, taskID string, revision int64,
) (keyvalue.Versioned[TaskProvenanceRecord], error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision <= 0 {
		return keyvalue.Versioned[TaskProvenanceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Runner Task provenance requires a fixed snapshot",
		)
	}
	key := TaskProvenanceKey(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[TaskProvenanceRecord]{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[TaskProvenanceRecord]{}, corrupt()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return keyvalue.Versioned[TaskProvenanceRecord]{ReadRevision: revision}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || len(value.Value) > maxRecord {
		return keyvalue.Versioned[TaskProvenanceRecord]{}, corrupt()
	}
	record, err := recordcodec.Decode[TaskProvenanceRecord](value.Value, taskProvenanceKind)
	if err != nil || record.TaskID != taskID || !record.valid() {
		return keyvalue.Versioned[TaskProvenanceRecord]{}, corrupt()
	}
	return keyvalue.Versioned[TaskProvenanceRecord]{
		Record:       record,
		Revision:     value.ModRevision,
		ReadRevision: revision,
	}, nil
}

func (record TaskProvenanceRecord) valid() bool {
	return record.Authority.valid() && ids.Validate(ids.KindTask, record.TaskID) == nil &&
		record.Intent.Validate() == nil
}

func (record TaskProvenanceRecord) MatchesTask(task workexecution.Task) bool {
	authority := record.Authority
	if record.TaskID != task.ID || task.Scope != authority.Scope || task.Action != authority.Action ||
		task.Actor != workexecution.OperatorActor || task.AcceptedIntent == nil || *task.AcceptedIntent != record.Intent {
		return false
	}
	if authority.Action == workexecution.ActionFetch {
		return task.TargetName == authority.Image && ids.Validate(ids.KindHost, task.TargetID) == nil
	}
	return authority.Action == workexecution.ActionDeploy && task.TargetID == authority.ServiceID
}
