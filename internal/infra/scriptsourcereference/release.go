package scriptsourcereference

import (
	"context"
	"math"
)

const (
	normalReleaseWindowSize          = 16
	releaseTransactionOperationLimit = 96
)

type ReleaseFragment struct {
	Conditions []Condition
	Mutations  []Mutation
}

func (fragment *ReleaseFragment) Clear() {
	if fragment == nil {
		return
	}
	clearMutations(fragment.Mutations)
	*fragment = ReleaseFragment{}
}

// PrepareNormalRelease returns the root transition that the lifecycle owner
// commits atomically with its assignment fence and retry decision.
func (repository *Repository) PrepareNormalRelease(
	ctx context.Context,
	operationID string,
	disposition RetryDisposition,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" ||
		(disposition != RetryDispositionForbidden && disposition != RetryDispositionAbandoned) {
		return ReleaseFragment{}, validation("normal source release input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return ReleaseFragment{}, conflict("operation source root is unavailable")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID {
		return ReleaseFragment{}, corruption("operation source root identity changed")
	}
	if root.Phase == operationSourcePhaseReleasing &&
		root.ReleasePath == sourceReleasePathNormal && root.RetryDisposition == disposition {
		return ReleaseFragment{}, nil
	}
	if root.Phase != operationSourcePhaseActive || root.ReleasePath != sourceReleasePathAbsent ||
		root.ReleaseCursor != 0 ||
		(root.RetryDisposition != RetryDispositionUndecided &&
			root.RetryDisposition != RetryDispositionTransferred) {
		return ReleaseFragment{}, conflict("operation source root cannot begin normal release")
	}
	root.Phase = operationSourcePhaseReleasing
	root.ReleasePath = sourceReleasePathNormal
	root.RetryDisposition = disposition
	value, err := encodeRoot(root)
	if err != nil {
		return ReleaseFragment{}, err
	}
	return ReleaseFragment{
		Conditions: []Condition{{Key: RootKey(operationID), ModRevision: read.Values[0].ModRevision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(operationID), Value: value}},
	}, nil
}

// ReleaseNext removes one replay-safe physical page. It reads at most sixteen
// memberships and deterministically selects the largest prefix whose exact
// comparisons and mutations fit the ordinary transaction ceiling.
func (repository *Repository) ReleaseNext(
	ctx context.Context,
	operationID string,
	guards []Condition,
) (bool, bool, error) {
	if ctx == nil || operationID == "" || len(guards) == 0 {
		return false, false, validation("source release input is invalid")
	}
	if err := validateReleaseGuards(guards); err != nil {
		return false, false, err
	}
	rootRead, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return false, false, err
	}
	if rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
		return false, false, conflict("operation source root is unavailable")
	}
	root, err := decodeRoot(rootRead.Values[0].Value)
	if err != nil {
		return false, false, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseReleasing ||
		root.ReleasePath != sourceReleasePathNormal ||
		(root.RetryDisposition != RetryDispositionForbidden &&
			root.RetryDisposition != RetryDispositionAbandoned) {
		return false, false, conflict("operation source root is not in normal release")
	}
	page, err := repository.store.Range(ctx, ReversePrefix(operationID), normalReleaseWindowSize)
	if err != nil {
		return false, false, err
	}
	if page == nil || len(page.Values) > normalReleaseWindowSize {
		return false, false, corruption("source release page is invalid")
	}
	if len(page.Values) == 0 {
		if root.ReleaseCursor != root.MembershipCount {
			return false, false, corruption("source release membership count is inconsistent")
		}
		return false, true, nil
	}
	physicalPage, err := boundedReleasePage(root, page.Values, len(guards))
	if err != nil {
		return false, false, err
	}
	if err := repository.releaseActivePage(
		ctx,
		root,
		rootRead.Values[0].ModRevision,
		physicalPage,
		guards,
	); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func validateReleaseGuards(guards []Condition) error {
	for _, guard := range guards {
		if guard.Key == "" || guard.ModRevision < 0 || guard.Prefix && guard.ModRevision != 0 {
			return validation("source release guard is invalid")
		}
	}
	return nil
}

func boundedReleasePage(
	root OperationSourceRoot,
	page []KeyValue,
	guardCount int,
) ([]KeyValue, error) {
	sources := make(map[string]struct{}, len(page))
	scripts := make(map[string]struct{}, len(page))
	for index, reverse := range page {
		reference, err := decodeReference(reverse.Value)
		if err != nil || reference.OperationID != root.OperationID || ReverseKey(reference) != reverse.Key {
			return nil, corruption("source reverse membership is corrupt")
		}
		sources[SourceSuffix(reference.Source)] = struct{}{}
		if reference.Source.Kind == SourceBody {
			scripts[ScriptPrimaryKey(reference.Source)] = struct{}{}
		}
		membershipCount := index + 1
		operationCount := 2 + guardCount + 4*membershipCount + 2*len(sources) + 2*len(scripts)
		if operationCount > releaseTransactionOperationLimit {
			if index == 0 {
				return nil, validation("source release guards exceed the transaction ceiling")
			}
			return page[:index], nil
		}
	}
	return page, nil
}

func (repository *Repository) releaseActivePage(
	ctx context.Context,
	root OperationSourceRoot,
	rootRevision int64,
	page []KeyValue,
	guards []Condition,
) error {
	references := make([]Reference, len(page))
	uniqueSources := make([]sourceBatch, 0, len(page))
	sourceIndexes := make(map[string]int)
	bodySources := make([]sourceBatch, 0, len(page))
	bodyIndexes := make(map[string]int)
	lookupKeys := make([]string, 0, len(page)*2)
	for index, reverse := range page {
		reference, err := decodeReference(reverse.Value)
		if err != nil || reference.OperationID != root.OperationID || ReverseKey(reference) != reverse.Key {
			return corruption("source reverse membership is corrupt")
		}
		references[index] = reference
		lookupKeys = append(lookupKeys, ForwardKey(reference))
		suffix := SourceSuffix(reference.Source)
		sourceIndex, exists := sourceIndexes[suffix]
		if !exists {
			sourceIndexes[suffix] = len(uniqueSources)
			uniqueSources = append(uniqueSources, sourceBatch{source: reference.Source})
			sourceIndex = len(uniqueSources) - 1
		}
		uniqueSources[sourceIndex].count++
		if reference.Source.Kind == SourceBody {
			key := ScriptPrimaryKey(reference.Source)
			bodyIndex, found := bodyIndexes[key]
			if !found {
				bodyIndexes[key] = len(bodySources)
				bodySources = append(bodySources, sourceBatch{source: reference.Source})
				bodyIndex = len(bodySources) - 1
			}
			bodySources[bodyIndex].count++
		}
	}
	for _, source := range uniqueSources {
		lookupKeys = append(lookupKeys, CountKey(source.source))
	}
	for _, source := range bodySources {
		lookupKeys = append(lookupKeys, ScriptPrimaryKey(source.source))
	}
	read, err := repository.store.GetMany(ctx, lookupKeys, 0)
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(lookupKeys) {
		return corruption("source release membership evidence is incomplete")
	}
	conditions := make([]Condition, 0, 1+len(guards)+2*len(page)+len(uniqueSources)+len(bodySources))
	conditions = append(conditions, Condition{Key: RootKey(root.OperationID), ModRevision: rootRevision})
	conditions = append(conditions, guards...)
	mutations := make([]Mutation, 0, len(page)*2+len(uniqueSources)+len(bodySources)+1)
	defer clearMutations(mutations)
	for index, reverse := range page {
		forward := read.Values[index]
		if forward == nil || forward.Key != ForwardKey(references[index]) ||
			!bytesEqual(forward.Value, reverse.Value) {
			return corruption("source forward and reverse memberships differ")
		}
		conditions = append(conditions,
			Condition{Key: reverse.Key, ModRevision: reverse.ModRevision},
			Condition{Key: forward.Key, ModRevision: forward.ModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: reverse.Key},
			Mutation{Type: MutationDelete, Key: forward.Key},
		)
	}
	countOffset := len(page)
	for index, source := range uniqueSources {
		current := read.Values[countOffset+index]
		if current == nil || current.Key != CountKey(source.source) {
			return corruption("source count is missing during release")
		}
		count, err := decodeCount(current.Value)
		if err != nil || count.Source != source.source || count.ReferencedExecutionCount < source.count {
			return corruption("source count underflows during release")
		}
		conditions = append(conditions, Condition{Key: current.Key, ModRevision: current.ModRevision})
		count.ReferencedExecutionCount -= source.count
		if count.ReferencedExecutionCount == 0 {
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: current.Key})
		} else {
			value, encodeErr := encodeCount(count)
			if encodeErr != nil {
				return encodeErr
			}
			mutations = append(mutations, Mutation{Type: MutationPut, Key: current.Key, Value: value})
		}
	}
	bodyOffset := countOffset + len(uniqueSources)
	for index, source := range bodySources {
		current := read.Values[bodyOffset+index]
		if current == nil || current.Key != ScriptPrimaryKey(source.source) || source.count > math.MaxInt64 {
			return corruption("Script source primary is missing during release")
		}
		value, err := repository.script.AdjustScriptPrimary(current.Value, source.source, -int64(source.count))
		if err != nil {
			return err
		}
		conditions = append(conditions, Condition{Key: current.Key, ModRevision: current.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: current.Key, Value: value})
	}
	root.ReleaseCursor += uint64(len(page))
	if root.ReleaseCursor > root.MembershipCount {
		return corruption("source release cursor overflowed")
	}
	rootValue, err := encodeRoot(root)
	if err != nil {
		return err
	}
	mutations = append(mutations, Mutation{Type: MutationPut, Key: RootKey(root.OperationID), Value: rootValue})
	if len(page) > normalReleaseWindowSize || len(conditions)+len(mutations) > releaseTransactionOperationLimit {
		return corruption("source release batch exceeds transaction ceiling")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict("source release raced durable state")
	}
	return nil
}

// PrepareReleaseFinalization proves the cursor complete and the only
// operation-addressable membership prefix empty. The caller adds the returned
// exact root deletion to its terminal transaction.
func (repository *Repository) PrepareReleaseFinalization(
	ctx context.Context,
	operationID string,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" {
		return ReleaseFragment{}, validation("source release finalization input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return ReleaseFragment{}, conflict("operation source root is unavailable")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	page, err := repository.store.Range(ctx, ReversePrefix(operationID), 1)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseReleasing ||
		root.ReleasePath != sourceReleasePathNormal ||
		(root.RetryDisposition != RetryDispositionForbidden &&
			root.RetryDisposition != RetryDispositionAbandoned) ||
		root.ReleaseCursor != root.MembershipCount || page == nil || len(page.Values) != 0 {
		return ReleaseFragment{}, corruption("source release cannot be finalized")
	}
	return ReleaseFragment{
		Conditions: []Condition{
			{Key: RootKey(operationID), ModRevision: read.Values[0].ModRevision},
			{Key: ReversePrefix(operationID), Prefix: true},
		},
		Mutations: []Mutation{{Type: MutationDelete, Key: RootKey(operationID)}},
	}, nil
}
