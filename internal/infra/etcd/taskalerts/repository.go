package taskalerts

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}
type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Task alert storage is required")
	}
	return &Repository{store: store}, nil
}

type Snapshot struct {
	Config       *Config
	Delivery     *Delivery
	Revision     int64
	ReadRevision int64
}
type Change struct {
	Conditions []keyvalue.Condition
	Mutations  []keyvalue.Mutation
}

func (repository *Repository) Read(ctx context.Context, scope string) (Snapshot, error) {
	if !ValidScope(scope) {
		return Snapshot{}, errs.New(errs.KindValidationFailed, "Select Platform or an Environment for alerting")
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ConfigKey(scope), DeliveryKey(scope)}},
	)
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || len(read.Values) != 2 {
		return Snapshot{}, errs.New(errs.KindInternal, "Task alert settings could not be read")
	}
	defer keyvalue.ClearValues(read.Values)
	result := Snapshot{Revision: keyvalue.RevisionOf(read.Values[0]), ReadRevision: read.ReadRevision}
	if read.Values[0] != nil {
		value, err := DecodeConfig(read.Values[0].Value)
		if err != nil {
			return Snapshot{}, err
		}
		result.Config = &value
	}
	if read.Values[1] != nil {
		value, err := recordcodec.Decode[Delivery](read.Values[1].Value, "task_alert_delivery")
		if err != nil {
			return Snapshot{}, err
		}
		if result.Config != nil && value.Generation == result.Config.Generation {
			result.Delivery = &value
		}
	}
	return result, nil
}

func (repository *Repository) PrepareSave(
	ctx context.Context,
	scope string,
	current Snapshot,
	next Config,
) (Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Change{}, err
	}
	raw, err := recordcodec.Encode("task_alert_config", next)
	if err != nil {
		return Change{}, err
	}
	if _, err := DecodeConfig(raw); err != nil {
		clear(raw)
		return Change{}, err
	}
	conditions, err := repository.OwnerConditions(ctx, scope, current.ReadRevision)
	if err != nil {
		clear(raw)
		return Change{}, err
	}
	return Change{
		Conditions: append(conditions, keyvalue.Condition{Key: ConfigKey(scope), ModRevision: current.Revision}),
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ConfigKey(scope), Value: raw},
			{Type: keyvalue.MutationDelete, Key: DeliveryKey(scope)}},
	}, nil
}

func (repository *Repository) PrepareTest(
	ctx context.Context,
	scope string,
	current Snapshot,
	event Event,
) (Change, error) {
	conditions, err := repository.OwnerConditions(ctx, scope, current.ReadRevision)
	if err != nil {
		return Change{}, err
	}
	mutations, err := PutEvent(event)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Conditions: append(conditions, keyvalue.Condition{Key: ConfigKey(scope), ModRevision: current.Revision}),
		Mutations:  mutations,
	}, nil
}

type Pending struct {
	Event    Event
	Key      string
	Revision int64
}

func (repository *Repository) Pending(ctx context.Context, after string) ([]Pending, bool, error) {
	if !ValidEventCursor(after) {
		return nil, false, errs.New(errs.KindInternal, "Task alert queue cursor is invalid")
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: OutboxPrefix, StartExclusive: after, Limit: 32},
	)
	if err != nil {
		return nil, false, err
	}
	if page == nil {
		return nil, false, errs.New(errs.KindInternal, "Task alert queue is unavailable")
	}
	defer keyvalue.ClearRangeValues(page.Values)
	result := make([]Pending, 0, len(page.Values))
	for _, raw := range page.Values {
		event, err := DecodeEvent(raw.Value)
		if err != nil {
			return nil, false, err
		}
		if raw.Key != EventKey(event.Scope, event.ID) {
			return nil, false, errs.New(errs.KindInternal, "Task alert queue identity is invalid")
		}
		result = append(result, Pending{Event: event, Key: raw.Key, Revision: raw.ModRevision})
	}
	return result, page.More, nil
}
func (repository *Repository) Drop(ctx context.Context, pending Pending) error {
	_, err := repository.store.Transact(ctx, []keyvalue.Condition{{Key: pending.Key, ModRevision: pending.Revision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: pending.Key}})
	return err
}

func (repository *Repository) Settle(
	ctx context.Context,
	pending Pending,
	current Snapshot,
	delivery Delivery,
	retry *Event,
) error {
	raw, err := recordcodec.Encode("task_alert_delivery", delivery)
	if err != nil {
		return err
	}
	defer clear(raw)
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: DeliveryKey(pending.Event.Scope), Value: raw}}
	if retry == nil {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: pending.Key})
	} else {
		more, err := PutEvent(*retry)
		if err != nil {
			return err
		}
		mutations = append(mutations, more...)
		defer keyvalue.ClearMutationValues(more)
	}
	_, err = repository.store.Transact(ctx, []keyvalue.Condition{{Key: pending.Key, ModRevision: pending.Revision},
		{Key: ConfigKey(pending.Event.Scope), ModRevision: current.Revision}}, mutations)
	return err
}
