package app

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTestStoreVersion struct {
	revision int64
	present  bool
	value    etcd.KeyValue
}

type blueprintTestStoreFaultKind uint8

const (
	blueprintTestStoreFailOrdinaryBeforeCommit blueprintTestStoreFaultKind = iota + 1
	blueprintTestStoreAdvanceComparedValueBeforeTerminal
	blueprintTestStoreErrorAfterTerminalCommit
)

type blueprintTestStoreFaultDirective struct {
	kind                blueprintTestStoreFaultKind
	ordinaryTransaction int
	comparedCondition   int
}

type blueprintTestStoreStats struct {
	Transactions         int
	CommittedRevision    int64
	InjectedRaceRevision int64
}

// blueprintTestStore is the app test package's in-memory MVCC persistence
// witness. It provides only the public Store behavior needed to compose real
// repositories; production persistence mechanics remain owned by etcd.
type blueprintTestStore struct {
	transactionMu sync.Mutex
	mu            sync.RWMutex

	values      map[string]etcd.KeyValue
	history     map[string][]blueprintTestStoreVersion
	revision    int64
	initialized bool
	readOnly    bool
	writes      int

	fault                func([]etcd.Condition, []etcd.Mutation) blueprintTestStoreFaultDirective
	faultGeneration      uint64
	transactions         int
	ordinaryTransactions int
	committedRevision    int64
	injectedRaceRevision int64
}

func newBlueprintTestStore() *blueprintTestStore {
	return &blueprintTestStore{
		values:   map[string]etcd.KeyValue{},
		history:  map[string][]blueprintTestStoreVersion{},
		revision: 1,
	}
}

func (*blueprintTestStore) Health(context.Context) error { return nil }
func (*blueprintTestStore) Close() error                 { return nil }

func (s *blueprintTestStore) Get(_ context.Context, key string) (*etcd.GetResult, error) {
	if err := validateBlueprintTestStoreKey(key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeLocked()
	result := &etcd.GetResult{ReadRevision: s.revision}
	result.Entry = s.valueAtLocked(key, s.revision)
	return result, nil
}

func (s *blueprintTestStore) GetMany(
	_ context.Context,
	request etcd.GetManyRequest,
) (*etcd.GetManyResult, error) {
	if len(request.Keys) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get requires at least one key")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get revision must not be negative")
	}
	for _, key := range request.Keys {
		if err := validateBlueprintTestStoreKey(key); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeLocked()
	view, err := s.readRevisionLocked(request.Revision)
	if err != nil {
		return nil, err
	}
	result := &etcd.GetManyResult{
		Values:           make([]*etcd.KeyValue, len(request.Keys)),
		ReadRevision:     view,
		ResponseRevision: s.revision,
	}
	for index, key := range request.Keys {
		result.Values[index] = s.valueAtLocked(key, view)
	}
	return result, nil
}

func (s *blueprintTestStore) Range(
	_ context.Context,
	request etcd.RangeRequest,
) (*etcd.RangeResult, error) {
	if request.Limit <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range limit must be positive")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range revision must not be negative")
	}
	if err := validateBlueprintTestStoreKey(request.Prefix); err != nil {
		return nil, err
	}
	if request.StartExclusive != "" {
		if err := validateBlueprintTestStoreKey(request.StartExclusive); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(request.StartExclusive, request.Prefix) {
			return nil, errs.New(errs.KindValidationFailed, "etcd range start must be within its prefix")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeLocked()
	view, err := s.readRevisionLocked(request.Revision)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(s.history))
	for key := range s.history {
		if !strings.HasPrefix(key, request.Prefix) {
			continue
		}
		if request.StartExclusive != "" && ((!request.Descending && key <= request.StartExclusive) ||
			(request.Descending && key >= request.StartExclusive)) {
			continue
		}
		if s.valueAtLocked(key, view) != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if request.Descending {
		slicesReverse(keys)
	}
	result := &etcd.RangeResult{
		ReadRevision:     view,
		ResponseRevision: s.revision,
		More:             len(keys) > int(request.Limit),
	}
	if result.More {
		keys = keys[:request.Limit]
	}
	result.Values = make([]etcd.KeyValue, 0, len(keys))
	for _, key := range keys {
		result.Values = append(result.Values, *s.valueAtLocked(key, view))
	}
	return result, nil
}

func (s *blueprintTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := s.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (s *blueprintTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := s.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationDelete, Key: key}})
	return result.Revision, err
}

func (s *blueprintTestStore) MeasureTransaction(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (s *blueprintTestStore) Transact(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	return s.transact(ctx, conditions, mutations, false)
}

func (s *blueprintTestStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	return s.transact(ctx, conditions, mutations, false)
}

func (*blueprintTestStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}

func (s *blueprintTestStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) (etcd.TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return etcd.TransactionResult{}, err
	}
	return s.transact(ctx, conditions, mutations, true)
}

func (*blueprintTestStore) Watch(context.Context, string, int64) (*etcd.WatchStream, error) {
	return nil, errors.New("unexpected watch")
}

func (*blueprintTestStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected snapshot")
}

func (s *blueprintTestStore) setFault(
	fault func([]etcd.Condition, []etcd.Mutation) blueprintTestStoreFaultDirective,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faultGeneration++
	s.fault = fault
}

func (s *blueprintTestStore) stats() blueprintTestStoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return blueprintTestStoreStats{
		Transactions:         s.transactions,
		CommittedRevision:    s.committedRevision,
		InjectedRaceRevision: s.injectedRaceRevision,
	}
}

func (s *blueprintTestStore) currentRevision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeLocked()
	return s.revision
}

func (s *blueprintTestStore) transact(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
	terminal bool,
) (etcd.TransactionResult, error) {
	clonedConditions := append([]etcd.Condition(nil), conditions...)
	clonedMutations := cloneBlueprintTestStoreMutations(mutations)
	if err := validateBlueprintTestStoreTransaction(clonedConditions, clonedMutations); err != nil {
		return etcd.TransactionResult{}, err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return etcd.TransactionResult{}, err
		}
	}

	s.transactionMu.Lock()
	defer s.transactionMu.Unlock()

	s.mu.Lock()
	s.initializeLocked()
	s.transactions++
	if !terminal {
		s.ordinaryTransactions++
	}
	ordinaryTransaction := s.ordinaryTransactions
	s.writes++
	if s.readOnly {
		s.mu.Unlock()
		return etcd.TransactionResult{}, errors.New("durable write before image preflight succeeded")
	}
	fault, generation := s.fault, s.faultGeneration
	s.mu.Unlock()

	directive := blueprintTestStoreFaultDirective{}
	if fault != nil {
		directive = fault(
			append([]etcd.Condition(nil), clonedConditions...),
			cloneBlueprintTestStoreMutations(clonedMutations),
		)
	}
	triggered, err := s.consumeFault(generation, directive, terminal, ordinaryTransaction, clonedConditions)
	if err != nil {
		return etcd.TransactionResult{}, err
	}
	if triggered == blueprintTestStoreFailOrdinaryBeforeCommit {
		return etcd.TransactionResult{}, errs.New(errs.KindInternal, "injected transaction failure before commit")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	failed := s.failureReadsLocked(clonedConditions)
	if failed != nil {
		return etcd.TransactionResult{
			Revision: s.revision, FailureReads: failed,
		}, nil
	}
	changed := s.transactionChangesStateLocked(clonedMutations)
	if changed {
		s.revision++
		for _, mutation := range clonedMutations {
			s.applyMutationLocked(mutation, s.revision)
		}
	}
	s.committedRevision = s.revision
	if triggered == blueprintTestStoreErrorAfterTerminalCommit {
		return etcd.TransactionResult{}, errs.New(errs.KindInternal, "injected terminal response loss after commit")
	}
	return etcd.TransactionResult{Succeeded: true, Revision: s.revision}, nil
}

func (s *blueprintTestStore) consumeFault(
	generation uint64,
	directive blueprintTestStoreFaultDirective,
	terminal bool,
	ordinaryTransaction int,
	conditions []etcd.Condition,
) (blueprintTestStoreFaultKind, error) {
	trigger := false
	switch directive.kind {
	case 0:
		return 0, nil
	case blueprintTestStoreFailOrdinaryBeforeCommit:
		if directive.ordinaryTransaction <= 0 {
			return 0, errs.New(errs.KindInternal, "injected ordinary transaction number must be positive")
		}
		trigger = !terminal && ordinaryTransaction == directive.ordinaryTransaction
	case blueprintTestStoreAdvanceComparedValueBeforeTerminal,
		blueprintTestStoreErrorAfterTerminalCommit:
		trigger = terminal
	default:
		return 0, errs.New(errs.KindInternal, "unknown Blueprint test Store fault")
	}
	if !trigger {
		return 0, nil
	}

	s.mu.Lock()
	if generation == s.faultGeneration {
		s.fault = nil
	}
	s.mu.Unlock()
	if directive.kind != blueprintTestStoreAdvanceComparedValueBeforeTerminal {
		return directive.kind, nil
	}
	if directive.comparedCondition < 0 || directive.comparedCondition >= len(conditions) ||
		conditions[directive.comparedCondition].Prefix {
		return 0, errs.New(errs.KindInternal, "injected terminal compare condition is invalid")
	}
	key := conditions[directive.comparedCondition].Key
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.values[key]
	if !found {
		return 0, errs.New(errs.KindInternal, "injected terminal compare value is absent")
	}
	s.revision++
	current.Value = append([]byte(nil), current.Value...)
	current.Version++
	current.ModRevision = s.revision
	s.values[key] = current
	s.history[key] = append(s.history[key], blueprintTestStoreVersion{
		revision: s.revision, present: true, value: cloneBlueprintTestStoreValue(current),
	})
	s.committedRevision = s.revision
	s.injectedRaceRevision = s.revision
	return directive.kind, nil
}

func (s *blueprintTestStore) failureReadsLocked(conditions []etcd.Condition) []*etcd.KeyValue {
	matched := true
	for _, condition := range conditions {
		if condition.Prefix {
			if s.firstPrefixValueLocked(condition.Key) != nil {
				matched = false
			}
			continue
		}
		actual := int64(0)
		if value, found := s.values[condition.Key]; found {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			matched = false
		}
	}
	if matched {
		return nil
	}
	reads := make([]*etcd.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.Prefix {
			reads[index] = s.firstPrefixValueLocked(condition.Key)
			continue
		}
		if value, found := s.values[condition.Key]; found {
			cloned := cloneBlueprintTestStoreValue(value)
			reads[index] = &cloned
		}
	}
	return reads
}

func (s *blueprintTestStore) firstPrefixValueLocked(prefix string) *etcd.KeyValue {
	keys := make([]string, 0)
	for key := range s.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	value := cloneBlueprintTestStoreValue(s.values[keys[0]])
	return &value
}

func (s *blueprintTestStore) transactionChangesStateLocked(mutations []etcd.Mutation) bool {
	for _, mutation := range mutations {
		switch mutation.Type {
		case etcd.MutationPut:
			return true
		case etcd.MutationDelete:
			if mutation.Prefix {
				for key := range s.values {
					if strings.HasPrefix(key, mutation.Key) {
						return true
					}
				}
			} else if _, found := s.values[mutation.Key]; found {
				return true
			}
		}
	}
	return false
}

func (s *blueprintTestStore) applyMutationLocked(mutation etcd.Mutation, revision int64) {
	if mutation.Type == etcd.MutationPut {
		version := int64(1)
		if prior, found := s.values[mutation.Key]; found {
			version = prior.Version + 1
		}
		value := etcd.KeyValue{
			Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), Version: version, ModRevision: revision,
		}
		s.values[mutation.Key] = value
		s.history[mutation.Key] = append(s.history[mutation.Key], blueprintTestStoreVersion{
			revision: revision, present: true, value: cloneBlueprintTestStoreValue(value),
		})
		return
	}
	if !mutation.Prefix {
		s.deleteKeyLocked(mutation.Key, revision)
		return
	}
	keys := make([]string, 0)
	for key := range s.values {
		if strings.HasPrefix(key, mutation.Key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		s.deleteKeyLocked(key, revision)
	}
}

func (s *blueprintTestStore) deleteKeyLocked(key string, revision int64) {
	if _, found := s.values[key]; !found {
		return
	}
	delete(s.values, key)
	s.history[key] = append(s.history[key], blueprintTestStoreVersion{revision: revision})
}

func (s *blueprintTestStore) initializeLocked() {
	if s.initialized {
		return
	}
	if s.revision <= 0 {
		s.revision = 1
	}
	if s.values == nil {
		s.values = map[string]etcd.KeyValue{}
	}
	if s.history == nil {
		s.history = map[string][]blueprintTestStoreVersion{}
	}
	for key, item := range s.values {
		item.Key = key
		if item.Version <= 0 {
			item.Version = 1
		}
		if item.ModRevision <= 0 {
			item.ModRevision = s.revision
		}
		item.Value = append([]byte(nil), item.Value...)
		s.values[key] = item
		s.history[key] = append(s.history[key], blueprintTestStoreVersion{
			revision: item.ModRevision, present: true, value: cloneBlueprintTestStoreValue(item),
		})
	}
	s.initialized = true
}

func (s *blueprintTestStore) readRevisionLocked(requested int64) (int64, error) {
	if requested == 0 {
		return s.revision, nil
	}
	if requested > s.revision {
		return 0, errs.New(errs.KindInternal, "etcd requested revision is in the future")
	}
	return requested, nil
}

func (s *blueprintTestStore) valueAtLocked(key string, revision int64) *etcd.KeyValue {
	versions := s.history[key]
	var selected *blueprintTestStoreVersion
	for index := range versions {
		if versions[index].revision <= revision {
			selected = &versions[index]
		}
	}
	if selected == nil || !selected.present {
		return nil
	}
	value := cloneBlueprintTestStoreValue(selected.value)
	return &value
}

func validateBlueprintTestStoreTransaction(conditions []etcd.Condition, mutations []etcd.Mutation) error {
	if len(mutations) == 0 {
		return errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	for _, condition := range conditions {
		if condition.ModRevision < 0 {
			return errs.New(errs.KindValidationFailed, "etcd transaction revisions must not be negative")
		}
		if err := validateBlueprintTestStoreKey(condition.Key); err != nil {
			return err
		}
		if condition.Prefix && condition.ModRevision != 0 {
			return errs.New(errs.KindValidationFailed, "etcd prefix transaction condition requires a zero revision")
		}
	}
	for _, mutation := range mutations {
		if err := validateBlueprintTestStoreKey(mutation.Key); err != nil {
			return err
		}
		switch mutation.Type {
		case etcd.MutationPut:
			if mutation.Prefix {
				return errs.New(errs.KindValidationFailed, "etcd put mutation must not use prefix semantics")
			}
		case etcd.MutationDelete:
		default:
			return errs.New(errs.KindValidationFailed, "invalid etcd transaction mutation")
		}
	}
	return nil
}

func validateBlueprintTestStoreKey(key string) error {
	if key == "" || !strings.HasPrefix(key, "/") {
		return errs.New(errs.KindValidationFailed, "etcd logical keys must begin with /")
	}
	return nil
}

func cloneBlueprintTestStoreMutations(mutations []etcd.Mutation) []etcd.Mutation {
	cloned := append([]etcd.Mutation(nil), mutations...)
	for index := range cloned {
		cloned[index].Value = append([]byte(nil), cloned[index].Value...)
	}
	return cloned
}

func cloneBlueprintTestStoreValue(value etcd.KeyValue) etcd.KeyValue {
	value.Value = append([]byte(nil), value.Value...)
	return value
}

func slicesReverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
