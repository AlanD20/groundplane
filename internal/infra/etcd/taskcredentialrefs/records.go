// Package taskcredentialrefs retains stable credential identities selected by
// pure accepted operator input. It never captures plaintext or value generations.
package taskcredentialrefs

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumCredentials = 514 // 512 authored Entries and two Components.
const rootPrefix = "/v1/execution/task-credential-references/"

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "pending credential reference storage is required")
	}
	return &Repository{store: store}, nil
}

func Key(taskID string) string { return rootPrefix + taskID }
func SecretPrefix(secretID string) string {
	return "/v1/indexes/secrets/by-accepted-task/" + secretID + "/"
}
func taskKey(taskID string) string { return "/v1/execution/tasks/" + taskID + "/record" }

type record struct {
	TaskID     string              `json:"task_id"`
	ProjectID  string              `json:"project_id,omitempty"`
	Intent     workexecution.Input `json:"intent"`
	SecretIDs  []string            `json:"secret_ids"`
	Published  int                 `json:"published"`
	Accepted   bool                `json:"accepted"`
	Collecting bool                `json:"collecting"`
	Next       int                 `json:"next"`
	ExpiresAt  time.Time           `json:"expires_at"`
}

type Publication struct {
	Conditions []keyvalue.Condition
	Mutations  []keyvalue.Mutation
}

func (value record) validate() error {
	if ids.Validate(ids.KindTask, value.TaskID) != nil || value.Intent.Validate() != nil ||
		value.ProjectID != "" && ids.Validate(ids.KindProject, value.ProjectID) != nil ||
		len(value.SecretIDs) == 0 || len(value.SecretIDs) > maximumCredentials ||
		!slices.IsSorted(value.SecretIDs) || value.Published < 0 || value.Published > len(value.SecretIDs) ||
		value.Next < 0 || value.Next > value.Published || value.Next > 0 && !value.Collecting ||
		value.Accepted && value.Published != len(value.SecretIDs) ||
		value.ExpiresAt.IsZero() || value.ExpiresAt.Location() != time.UTC {
		return corrupt()
	}
	for index, id := range value.SecretIDs {
		if ids.Validate(ids.KindSecret, id) != nil || index > 0 && value.SecretIDs[index-1] == id {
			return corrupt()
		}
	}
	return nil
}

func encode(value record) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task-credential-references", value)
}

func decode(raw []byte) (record, error) {
	value, err := recordcodec.Decode[record](raw, "task-credential-references")
	if err != nil || value.validate() != nil {
		return record{}, corrupt()
	}
	return value, nil
}

func corrupt() error { return errs.New(errs.KindInternal, "pending credential references are corrupt") }
