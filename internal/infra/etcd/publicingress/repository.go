package publicingress

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	stateKind = "public-ingress"
	claimKind = "public-ingress-hostname-claim"
)

type Store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) *Repository { return &Repository{store: store} }

func Key(hostID string) string        { return "/v1/public-ingress/hosts/" + hostID }
func claimKey(hostname string) string { return "/v1/public-ingress/hostnames/" + hostname }

func (repository *Repository) Read(ctx context.Context, hostID string) (State, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return State{}, err
	}
	if err := recordcodec.ValidateID(ids.KindHost, hostID); err != nil {
		return State{}, err
	}
	key := Key(hostID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return State{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return State{}, recordcodec.CorruptRecord()
	}
	if result.Entry == nil {
		return State{HostID: hostID}, nil
	}
	defer clear(result.Entry.Value)
	state, err := recordcodec.Decode[State](result.Entry.Value, stateKind)
	if err != nil || state.HostID != hostID || result.Entry.Key != key ||
		result.Entry.ModRevision <= 0 || result.Entry.ModRevision > result.ReadRevision || !state.valid() {
		return State{}, recordcodec.CorruptRecord()
	}
	state.Revision = result.Entry.ModRevision
	return state, nil
}

// CheckClaims observes conflicts without changing desired or live state.
func (repository *Repository) CheckClaims(ctx context.Context, before, after State) error {
	_, mutations, err := repository.claimChanges(ctx, before, after)
	keyvalue.ClearMutationValues(mutations)
	return err
}

// Replace publishes durable ingress state and its global hostname claims with
// the same revision fence. Pending and applied ownership coexist until an
// exact activation settles, so a failed candidate cannot release live claims.
func (repository *Repository) Replace(
	ctx context.Context,
	before, after State,
	guards []keyvalue.Condition,
) (State, error) {
	change, err := repository.PrepareReplace(ctx, before, after, guards)
	if err != nil {
		return State{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := repository.store.Transact(ctx, change.Conditions, change.Mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return State{}, err
	}
	if !result.Succeeded {
		return State{}, errs.New(errs.KindStateConflict, "public ingress changed while the plan was being reviewed")
	}
	if result.Revision <= before.Revision {
		return State{}, recordcodec.CorruptRecord()
	}
	after.Revision = result.Revision
	return after, nil
}

// PrepareReplace joins the claim and publication fences to accepted Tasks.
// The caller owns the returned mutation bytes.
func (repository *Repository) PrepareReplace(
	ctx context.Context,
	before, after State,
	guards []keyvalue.Condition,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if before.HostID != after.HostID || before.Revision != after.Revision || !before.valid() || !after.valid() {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "public ingress state transition is invalid")
	}
	conditions, mutations, err := repository.claimChanges(ctx, before, after)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode(stateKind, after)
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return workqueue.Change{}, err
	}
	key := Key(before.HostID)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: before.Revision})
	conditions = append(conditions, guards...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}

type owner struct {
	HostID        string `json:"host_id"`
	EnvironmentID string `json:"environment_id"`
}

func (repository *Repository) claimChanges(
	ctx context.Context,
	before, after State,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	current, err := ownedHosts(before)
	if err != nil {
		return nil, nil, err
	}
	proposed, err := ownedHosts(after)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(current)+len(proposed))
	for key := range current {
		keys = append(keys, key)
	}
	for key := range proposed {
		if _, exists := current[key]; !exists {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	if len(keys) == 0 {
		return nil, nil, nil
	}
	result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(result.Values)
	if len(result.Values) != len(keys) || result.ReadRevision <= 0 {
		return nil, nil, recordcodec.CorruptRecord()
	}
	conditions := make([]keyvalue.Condition, 0, len(keys))
	mutations := make([]keyvalue.Mutation, 0, len(keys))
	for index, key := range keys {
		entry := result.Values[index]
		previous, previouslyOwned := current[key]
		next, willOwn := proposed[key]
		revision, err := checkOwner(key, entry, previous, previouslyOwned, result.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(mutations)
			return nil, nil, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: revision})
		switch {
		case !willOwn:
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		case !previouslyOwned:
			value, err := recordcodec.Encode(claimKind, next)
			if err != nil {
				keyvalue.ClearMutationValues(mutations)
				return nil, nil, err
			}
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
		case next != previous:
			keyvalue.ClearMutationValues(mutations)
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"public ingress hostname requires explicit withdrawal before changing ownership",
			)
		}
	}
	if len(mutations) != 0 {
		condition, err := readClaimGeneration(ctx, repository.store, result.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(mutations)
			return nil, nil, err
		}
		value, err := recordcodec.Encode(claimGenerationKind, claimGeneration{})
		if err != nil {
			keyvalue.ClearMutationValues(mutations)
			return nil, nil, err
		}
		conditions = append(conditions, condition)
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: claimGenerationKey, Value: value},
		)
	}
	return conditions, mutations, nil
}

func ownedHosts(state State) (map[string]owner, error) {
	claims := make(map[string]owner)
	for _, publication := range []*Publication{state.Applied, state.Pending} {
		if publication == nil {
			continue
		}
		for _, claim := range publication.Claims {
			key := claimKey(claim.Hostname)
			wanted := owner{HostID: state.HostID, EnvironmentID: claim.EnvironmentID}
			if existing, exists := claims[key]; exists && existing != wanted {
				return nil, errs.New(
					errs.KindStateConflict,
					"public ingress hostname requires explicit withdrawal before changing ownership",
				)
			}
			claims[key] = wanted
		}
	}
	return claims, nil
}

func checkOwner(
	key string,
	value *keyvalue.KeyValue,
	previous owner,
	previouslyOwned bool,
	revision int64,
) (int64, error) {
	if value == nil {
		if previouslyOwned {
			return 0, recordcodec.CorruptRecord()
		}
		return 0, nil
	}
	actual, err := recordcodec.Decode[owner](value.Value, claimKind)
	if err != nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision ||
		ids.Validate(
			ids.KindHost,
			actual.HostID,
		) != nil || ids.Validate(ids.KindEnvironment, actual.EnvironmentID) != nil {
		return 0, recordcodec.CorruptRecord()
	}
	if !previouslyOwned || actual != previous {
		return 0, errs.New(errs.KindStateConflict, "public ingress hostname is reserved by a different publication")
	}
	return value.ModRevision, nil
}
