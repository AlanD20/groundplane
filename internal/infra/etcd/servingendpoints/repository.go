// Package servingendpoints owns the durable serving endpoint document for each
// Environment Service port.
package servingendpoints

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	documentKind = "serving-endpoints"
	maxPayload   = 128 << 10
)

type Target struct {
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
	Port          uint16 `json:"port"`
}

type Cell struct {
	Target       Target          `json:"target"`
	Payload      json.RawMessage `json:"payload"`
	Revision     int64           `json:"-"`
	ReadRevision int64           `json:"-"`
}

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) *Repository { return &Repository{store: store} }

func Key(target Target) string {
	return "/v1/serving-endpoints/" + target.EnvironmentID + "/" + target.ServiceID + "/" +
		strconv.FormatUint(uint64(target.Port), 10)
}

func validTarget(target Target) bool {
	return target.Port != 0 && ids.Validate(ids.KindEnvironment, target.EnvironmentID) == nil &&
		ids.Validate(ids.KindService, target.ServiceID) == nil
}

func (repository *Repository) ReadAt(ctx context.Context, target Target, revision int64) (Cell, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Cell{}, err
	}
	if !validTarget(target) || revision < 0 {
		return Cell{}, errs.New(errs.KindValidationFailed, "serving endpoint target or read revision is invalid")
	}
	key := Key(target)
	result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Cell{}, err
	}
	if result == nil || len(result.Values) != 1 || result.ReadRevision <= 0 ||
		revision != 0 && result.ReadRevision != revision {
		return Cell{}, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(result.Values)
	value := result.Values[0]
	if value == nil {
		return Cell{Target: target, ReadRevision: result.ReadRevision}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > result.ReadRevision {
		return Cell{}, recordcodec.CorruptRecord()
	}
	record, err := recordcodec.Decode[Cell](value.Value, documentKind)
	if err != nil || record.Target != target || len(record.Payload) == 0 || len(record.Payload) > maxPayload {
		return Cell{}, recordcodec.CorruptRecord()
	}
	record.Revision = value.ModRevision
	record.ReadRevision = result.ReadRevision
	return record, nil
}

// Replace commits a document and its captured authority at the same compare-
// and-swap boundary. Revision zero is the compare for a previously absent key.
func (repository *Repository) Replace(
	ctx context.Context, before Cell, payload json.RawMessage, guards []keyvalue.Condition,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if !validTarget(before.Target) || before.ReadRevision <= 0 || before.Revision < 0 ||
		before.Revision > before.ReadRevision || len(payload) == 0 || len(payload) > maxPayload {
		return errs.New(errs.KindValidationFailed, "serving endpoint write is invalid")
	}
	key := Key(before.Target)
	conditions := make([]keyvalue.Condition, 0, len(guards)+1)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: before.Revision})
	observed := make(map[string]int64, len(guards))
	for _, guard := range guards {
		if guard.Key == "" || guard.Key == key || guard.Prefix || guard.ModRevision < 0 ||
			guard.ModRevision > before.ReadRevision {
			return errs.New(errs.KindValidationFailed, "serving endpoint write authority is invalid")
		}
		if previous, exists := observed[guard.Key]; exists {
			if previous != guard.ModRevision {
				return errs.New(errs.KindStateConflict, "serving endpoint proofs disagree about current authority")
			}
			continue
		}
		observed[guard.Key] = guard.ModRevision
		conditions = append(conditions, guard)
	}
	value, err := recordcodec.Encode(documentKind, Cell{Target: before.Target, Payload: payload})
	if err != nil {
		return err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: key, Value: value},
	})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "serving endpoint state or its authority changed")
	}
	if result.Revision <= before.ReadRevision {
		return recordcodec.CorruptRecord()
	}
	return nil
}
