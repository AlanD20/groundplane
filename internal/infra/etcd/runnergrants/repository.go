// Package runnergrants owns the persistent scope of an explicitly issued CLI bearer.
package runnergrants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	recordKind = "runner_cli_grant"
	MaxActions = 16
	maxRecord  = 32 << 10
)

type Action struct {
	Operation       string   `json:"operation"`
	TargetID        string   `json:"target_id"`
	TaskIDs         []string `json:"task_ids,omitempty"`
	Image           string   `json:"image,omitempty"`
	DesiredRevision int64    `json:"desired_revision,omitempty"`
}

type Record struct {
	Revoked     bool                `json:"revoked,omitempty"`
	RunnerID    string              `json:"runner_id"`
	RunnerEpoch uint64              `json:"runner_epoch"`
	TokenDigest string              `json:"token_digest"`
	ExpiresAt   time.Time           `json:"expires_at"`
	Scope       workexecution.Scope `json:"scope"`
	Actions     []Action            `json:"actions"`
}

func TokenDigest(digest [sha256.Size]byte) string { return hex.EncodeToString(digest[:]) }
func Key(runnerID string) string                  { return "/v1/runner-cli-grants/" + runnerID }

type Repository struct{ store keyvalue.Store }

func New(store keyvalue.Store) *Repository { return &Repository{store: store} }

func (repository *Repository) Get(ctx context.Context, runnerID string) (keyvalue.Versioned[Record], error) {
	if err := ids.Validate(ids.KindRunner, runnerID); err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	key := Key(runnerID)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return keyvalue.Versioned[Record]{}, corrupt()
	}
	if read.Entry == nil {
		return keyvalue.Versioned[Record]{ReadRevision: read.ReadRevision}, nil
	}
	defer clear(read.Entry.Value)
	if read.Entry.Key != key || read.Entry.ModRevision <= 0 || read.Entry.ModRevision > read.ReadRevision ||
		len(read.Entry.Value) > maxRecord {
		return keyvalue.Versioned[Record]{}, corrupt()
	}
	record, err := recordcodec.Decode[Record](read.Entry.Value, recordKind)
	if err != nil || record.RunnerID != runnerID || !record.valid() {
		return keyvalue.Versioned[Record]{}, corrupt()
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Entry.ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func (repository *Repository) Publish(
	ctx context.Context, expected int64, record Record, now time.Time,
	guards []keyvalue.Condition, receipt workqueue.Change,
) (int64, error) {
	if !record.valid() || !record.Revoked && (!record.ExpiresAt.After(now) || len(guards) == 0) ||
		expected < 0 || record.Revoked && expected == 0 {
		return 0, errs.New(errs.KindValidationFailed, "Runner CLI grant is invalid")
	}
	encoded, err := recordcodec.Encode(recordKind, record)
	if err != nil {
		return 0, err
	}
	defer clear(encoded)
	if len(encoded) > maxRecord {
		return 0, errs.New(errs.KindValidationFailed, "Runner CLI grant exceeds its size limit")
	}
	key := Key(record.RunnerID)
	change, err := workqueue.JoinChanges(workqueue.Change{
		Conditions: append([]keyvalue.Condition{{Key: key, ModRevision: expected}}, guards...),
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}},
	}, receipt)
	if err != nil {
		return 0, err
	}
	result, err := repository.store.Transact(ctx, change.Conditions, change.Mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, conflict()
	}
	if result.Revision <= 0 {
		return 0, corrupt()
	}
	return result.Revision, nil
}

func (record Record) valid() bool {
	if record.Revoked {
		return ids.Validate(ids.KindRunner, record.RunnerID) == nil && record.RunnerEpoch == 0 &&
			record.TokenDigest == "" && record.ExpiresAt.IsZero() && record.Scope == (workexecution.Scope{}) &&
			len(record.Actions) == 0
	}
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.RunnerEpoch == 0 || record.ExpiresAt.IsZero() ||
		!validScope(record.Scope) ||
		len(record.Actions) == 0 || len(record.Actions) > MaxActions {
		return false
	}
	return validTokenDigest(record.TokenDigest)
}

func validTokenDigest(value string) bool {
	digest, err := hex.DecodeString(value)
	return err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == value
}

func validScope(scope workexecution.Scope) bool {
	return ids.Validate(ids.KindTenant, scope.TenantID) == nil &&
		ids.Validate(ids.KindProject, scope.ProjectID) == nil &&
		ids.Validate(ids.KindEnvironment, scope.EnvironmentID) == nil
}

func corrupt() error { return errs.New(errs.KindInternal, "Runner CLI grant record is invalid") }
func conflict() error {
	return errs.New(errs.KindStateConflict, "Runner CLI grant authority changed or is already in use")
}
