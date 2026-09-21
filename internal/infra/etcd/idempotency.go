package etcd

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"sync"
)

const (
	maximumPruneMarkers     = 16
	maximumPruneCASAttempts = 3
)

type idempotencyPlanClassifier func(int64, []*etcdstore.KeyValue) error

type idempotencyMutationPlan struct {
	mu               sync.Mutex
	consumed         bool
	markerKind       idempotencyrecord.IdempotencyMarkerKind
	conditions       []etcdstore.Condition
	mutations        []etcdstore.Mutation
	classify         idempotencyPlanClassifier
	validate         func([]etcdstore.Condition, []etcdstore.Mutation) error
	validateExisting func(context.Context, idempotencyrecord.IdempotencyMarker, int64, int64) error
}

func (plan *idempotencyMutationPlan) enforceExistingReplay(
	validate func(context.Context, idempotencyrecord.IdempotencyMarker, int64, int64) error,
) error {
	if plan == nil || validate == nil {
		return errs.New(errs.KindInternal, "idempotency replay validator is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed || plan.validateExisting != nil {
		return errs.New(errs.KindInternal, "idempotency replay validator cannot be replaced")
	}
	plan.validateExisting = validate
	return nil
}

func (plan *idempotencyMutationPlan) existingReplayValidator() func(
	context.Context,
	idempotencyrecord.IdempotencyMarker,
	int64,
	int64,
) error {
	if plan == nil {
		return nil
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	return plan.validateExisting
}

// enforceTransactionBounds defers a domain envelope check until Apply has
// appended the real idempotency marker, replay target, and retention writes.
func (plan *idempotencyMutationPlan) enforceTransactionBounds(
	validate func([]etcdstore.Condition, []etcdstore.Mutation) error,
) error {
	if plan == nil || validate == nil {
		return errs.New(errs.KindInternal, "idempotency transaction validator is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed || plan.validate != nil {
		return errs.New(errs.KindInternal, "idempotency transaction validator cannot be replaced")
	}
	plan.validate = validate
	return nil
}

func (plan *idempotencyMutationPlan) transactionValidator() func([]etcdstore.Condition, []etcdstore.Mutation) error {
	if plan == nil {
		return nil
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	return plan.validate
}

func NewIdempotencyMutationPlan(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	return newIdempotencyMutationPlanForMarker(
		idempotencyrecord.IdempotencyMarkerDirect,
		conditions,
		mutations,
		classify,
	)
}

func newTaskIdempotencyMutationPlan(
	record TaskRecord,
	initiation TaskInitiation,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	if err := validateTaskInitiation(record, initiation, true); err != nil {
		return nil, err
	}
	conditions, classify, err := prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return nil, err
	}
	conditions, mutations, classify, err = prepareTaskOwnerIndexPlan(record, conditions, mutations, classify)
	if err != nil {
		return nil, err
	}
	return newIdempotencyMutationPlanForMarker(
		idempotencyrecord.IdempotencyMarkerTask,
		conditions,
		mutations,
		classify,
	)
}

func newIdempotencyMutationPlanForMarker(
	markerKind idempotencyrecord.IdempotencyMarkerKind,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	if markerKind != idempotencyrecord.IdempotencyMarkerDirect &&
		markerKind != idempotencyrecord.IdempotencyMarkerTask {
		return nil, errs.New(errs.KindInternal, "idempotency mutation plan marker kind is invalid")
	}
	if classify == nil || markerKind == idempotencyrecord.IdempotencyMarkerTask && len(mutations) == 0 {
		return nil, errs.New(errs.KindInternal, "idempotency mutation plan is incomplete")
	}
	if err := validateIdempotencyPlanKeys(conditions, mutations); err != nil {
		return nil, err
	}
	return &idempotencyMutationPlan{
		markerKind: markerKind,
		conditions: append([]etcdstore.Condition(nil), conditions...),
		mutations:  etcdstore.CloneMutations(mutations),
		classify:   classify,
	}, nil
}

func validateIdempotencyPlanKeys(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
	compareKeys := make(map[string]struct{}, len(conditions))
	for _, condition := range conditions {
		if invalidIdempotencyPlanKey(condition.Key) {
			return errs.New(errs.KindInternal, "idempotency plan compare key is invalid")
		}
		if _, duplicate := compareKeys[condition.Key]; duplicate {
			return errs.New(errs.KindInternal, "idempotency plan contains a duplicate compare key")
		}
		compareKeys[condition.Key] = struct{}{}
	}
	mutationKeys := make(map[string]struct{}, len(mutations))
	for _, mutation := range mutations {
		if invalidIdempotencyPlanKey(mutation.Key) {
			return errs.New(errs.KindInternal, "idempotency plan mutation key is invalid")
		}
		if _, duplicate := mutationKeys[mutation.Key]; duplicate {
			return errs.New(errs.KindInternal, "idempotency plan contains a duplicate mutation key")
		}
		mutationKeys[mutation.Key] = struct{}{}
	}
	return nil
}

func invalidIdempotencyPlanKey(key string) bool {
	return key == "" || strings.HasPrefix(key, idempotencyrecord.IdempotencyMarkerPrefix) ||
		strings.HasPrefix(key, idempotencyrecord.IdempotencyRetentionPrefix) ||
		strings.HasPrefix(key, idempotencyrecord.IdempotencyReplayTargetPrefix)
}

func (plan *idempotencyMutationPlan) consume() ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if plan == nil {
		return nil, nil, nil, errs.New(errs.KindInternal, "idempotency mutation plan is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed {
		return nil, nil, nil, errs.New(errs.KindInternal, "idempotency mutation plan was already consumed")
	}
	plan.consumed = true
	conditions := append([]etcdstore.Condition(nil), plan.conditions...)
	mutations := etcdstore.CloneMutations(plan.mutations)
	classify := plan.classify
	etcdstore.ClearMutationValues(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.classify = nil
	plan.validate = nil
	plan.validateExisting = nil
	return conditions, mutations, classify, nil
}

type IdempotencyTransactionResult struct {
	kind     idempotencyTransactionResultKind
	revision int64
	marker   idempotencyrecord.IdempotencyMarker
	conflict error
}

// Revision returns the MVCC revision at which the transaction outcome was observed.
func (result IdempotencyTransactionResult) Revision() int64 {
	return result.revision
}

type idempotencyTransactionResultKind uint8

const (
	idempotencyTransactionApplied idempotencyTransactionResultKind = iota + 1
	idempotencyTransactionExisting
	idempotencyTransactionConflict
)

type IdempotencyKnownOutcome uint8

const (
	IdempotencyKnownApplied IdempotencyKnownOutcome = iota + 1
	IdempotencyKnownExisting
	IdempotencyKnownConflict
)

func (result *IdempotencyTransactionResult) Classify() (
	IdempotencyKnownOutcome,
	idempotencyrecord.IdempotencyMarker,
	error,
	error,
) {
	if result == nil || result.revision <= 0 {
		return 0, idempotencyrecord.IdempotencyMarker{}, nil, idempotencyrecord.CorruptIdempotencyMarker()
	}
	switch result.kind {
	case idempotencyTransactionApplied:
		if result.conflict != nil || result.marker.Kind != "" {
			return 0, idempotencyrecord.IdempotencyMarker{}, nil, idempotencyrecord.CorruptIdempotencyMarker()
		}
		return IdempotencyKnownApplied, idempotencyrecord.IdempotencyMarker{}, nil, nil
	case idempotencyTransactionExisting:
		if result.conflict != nil || idempotencyrecord.ValidateIdempotencyMarker(result.marker) != nil {
			return 0, idempotencyrecord.IdempotencyMarker{}, nil, idempotencyrecord.CorruptIdempotencyMarker()
		}
		marker := idempotencyrecord.CloneIdempotencyMarker(result.marker)
		clear(result.marker.Intent.Ciphertext)
		clear(result.marker.Response.Body)
		result.marker = idempotencyrecord.IdempotencyMarker{}
		return IdempotencyKnownExisting, marker, nil, nil
	case idempotencyTransactionConflict:
		if result.conflict == nil || result.marker.Kind != "" {
			return 0, idempotencyrecord.IdempotencyMarker{}, nil, idempotencyrecord.CorruptIdempotencyMarker()
		}
		return IdempotencyKnownConflict, idempotencyrecord.IdempotencyMarker{}, result.conflict, nil
	default:
		return 0, idempotencyrecord.IdempotencyMarker{}, nil, idempotencyrecord.CorruptIdempotencyMarker()
	}
}

type idempotencyRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type IdempotencyRepository struct{ store idempotencyRepositoryStore }

type idempotencyPruneCandidate struct {
	Marker                  IdempotencyEvidence
	RetentionKey            string
	RetentionValue          []byte
	RetentionModRevision    int64
	ReplayTargetKey         string
	ReplayTargetValue       []byte
	ReplayTargetModRevision int64
}

func NewIdempotencyRepository(store idempotencyRepositoryStore) (*IdempotencyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "idempotency store is required")
	}
	return &IdempotencyRepository{store: store}, nil
}

func (repository *IdempotencyRepository) Apply(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	plan *idempotencyMutationPlan,
) (IdempotencyTransactionResult, error) {
	return repository.apply(ctx, marker, plan, repository.store.Transact)
}

func (repository *IdempotencyRepository) applyEnvironmentBlueprint(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	plan *idempotencyMutationPlan,
	transactions environmentBlueprintTransactionStore,
) (IdempotencyTransactionResult, error) {
	return repository.apply(ctx, marker, plan, func(
		ctx context.Context,
		conditions []etcdstore.Condition,
		mutations []etcdstore.Mutation,
	) (etcdstore.TransactionResult, error) {
		return executeEnvironmentBlueprintTransaction(ctx, transactions, conditions, mutations)
	})
}

func (repository *IdempotencyRepository) apply(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	plan *idempotencyMutationPlan,
	transact func(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error),
) (IdempotencyTransactionResult, error) {
	if ctx == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if plan == nil || plan.markerKind != marker.Kind ||
		(marker.Kind == idempotencyrecord.IdempotencyMarkerTask && marker.State != idempotencyrecord.IdempotencyMarkerPending) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"idempotency marker does not match its mutation plan",
		)
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(markerValue)
	validateTransaction := plan.transactionValidator()
	validateExisting := plan.existingReplayValidator()
	conditions, mutations, classify, err := plan.consume()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	conditions = append([]etcdstore.Condition{{Key: markerKey, ModRevision: 0}}, conditions...)
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue})
	if marker.ReplayTarget != nil {
		targetKey, targetErr := idempotencyrecord.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if targetErr != nil {
			return IdempotencyTransactionResult{}, targetErr
		}
		targetValue, targetErr := idempotencyrecord.EncodeReplayTargetReference(markerKey)
		if targetErr != nil {
			return IdempotencyTransactionResult{}, targetErr
		}
		defer clear(targetValue)
		conditions = append(
			conditions[:1],
			append([]etcdstore.Condition{{Key: targetKey, ModRevision: 0}}, conditions[1:]...)...)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: targetKey, Value: targetValue},
		)
	}
	if !marker.RetainUntil.IsZero() {
		retentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			return IdempotencyTransactionResult{}, errs.Wrap(errs.KindInternal, err)
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		)
	}
	if validateTransaction != nil {
		if err := validateTransaction(conditions, mutations); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	result, err := transact(ctx, conditions, mutations)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearValues(result.FailureReads)
	if result.Succeeded {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionApplied, revision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "idempotency compare evidence is incomplete")
	}
	if result.FailureReads[0] != nil {
		existing, err := idempotencyrecord.DecodeIdempotencyMarker(result.FailureReads[0].Value, marker.Locator)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		if validateExisting != nil {
			if err := validateExisting(
				ctx,
				existing,
				result.Revision,
				result.FailureReads[0].ModRevision,
			); err != nil {
				return IdempotencyTransactionResult{}, err
			}
		}
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionExisting, revision: result.Revision, marker: existing,
		}, nil
	}
	planOffset := 1
	if marker.ReplayTarget != nil {
		if result.FailureReads[1] != nil {
			if err := idempotencyrecord.DecodeReplayTargetReference(result.FailureReads[1].Value, markerKey); err != nil {
				return IdempotencyTransactionResult{}, err
			}
			return IdempotencyTransactionResult{}, idempotencyrecord.CorruptIdempotencyMarker()
		}
		planOffset++
	}
	conflict := classify(result.Revision, result.FailureReads[planOffset:])
	if conflict == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"idempotency plan conflict was not classified",
		)
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionConflict, revision: result.Revision, conflict: conflict,
	}, nil
}

func (evidence *IdempotencyEvidence) Marker() (idempotencyrecord.IdempotencyMarker, error) {
	if evidence == nil || evidence.modRevision <= 0 ||
		idempotencyrecord.ValidateIdempotencyMarker(evidence.marker) != nil {
		return idempotencyrecord.IdempotencyMarker{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	marker := idempotencyrecord.CloneIdempotencyMarker(evidence.marker)
	clear(evidence.marker.Intent.Ciphertext)
	clear(evidence.marker.Response.Body)
	evidence.marker = idempotencyrecord.IdempotencyMarker{}
	return marker, nil
}
