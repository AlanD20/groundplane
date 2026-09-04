package scriptsourcereference

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Rationale: normal completion must retain the accepted 16-member read window
// while splitting only the physical transaction needed to stay below the
// ordinary 96-operation ceiling, and restart must resume without a duplicate
// decrement.
func TestNormalReleaseDrainsBoundedPagesExactlyOnceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_release_restart"
	members := releaseServiceMembers(store, operationID, 35)
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	guard := store.put("/tasks/current", []byte("pending"))
	startNormalRelease(t, ctx, store, repository, operationID, RetryDispositionAbandoned)

	store.commitThenError = true
	if _, _, err := repository.ReleaseNext(ctx, operationID, []Condition{{
		Key: guard.Key, ModRevision: guard.ModRevision,
	}}); err == nil {
		t.Fatal("ReleaseNext(unknown committed outcome) error = nil")
	}

	restarted, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for {
		processed, drained, releaseErr := restarted.ReleaseNext(ctx, operationID, []Condition{{
			Key: guard.Key, ModRevision: guard.ModRevision,
		}})
		if releaseErr != nil {
			t.Fatalf("ReleaseNext(restarted) error = %v", releaseErr)
		}
		if drained {
			break
		}
		if !processed {
			t.Fatal("ReleaseNext() made no progress before drain")
		}
	}
	if store.maximumRangeLimit != normalReleaseWindowSize {
		t.Fatalf("maximum range limit = %d, want %d", store.maximumRangeLimit, normalReleaseWindowSize)
	}
	if store.maximumTransactionOperations > releaseTransactionOperationLimit {
		t.Fatalf(
			"maximum transaction operations = %d, want <= %d",
			store.maximumTransactionOperations,
			releaseTransactionOperationLimit,
		)
	}
	assertReleaseMembersAbsent(t, store, members)

	finalization, err := restarted.PrepareReleaseFinalization(ctx, operationID)
	if err != nil {
		t.Fatalf("PrepareReleaseFinalization() error = %v", err)
	}
	result, err := store.Transact(ctx, finalization.Conditions, finalization.Mutations)
	finalization.Clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("finalize release = %#v, %v", result, err)
	}
	if _, exists := store.values[RootKey(operationID)]; exists {
		t.Fatal("release retained operation root")
	}
}

// Rationale: every physical release transaction must retain caller authority;
// a changed Task fence cannot remove a membership or advance the durable
// cursor.
func TestNormalReleaseFailsClosedWhenCallerGuardRaces(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_release_guard"
	members := releaseServiceMembers(store, operationID, 2)
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	guard := store.put("/tasks/current", []byte("pending"))
	startNormalRelease(t, ctx, store, repository, operationID, RetryDispositionForbidden)
	store.put(guard.Key, []byte("changed"))

	processed, drained, err := repository.ReleaseNext(ctx, operationID, []Condition{{
		Key: guard.Key, ModRevision: guard.ModRevision,
	}})
	if err == nil || processed || drained {
		t.Fatalf("ReleaseNext(raced guard) = %t, %t, %v", processed, drained, err)
	}
	root, decodeErr := decodeRoot(store.values[RootKey(operationID)].Value)
	if decodeErr != nil || root.ReleaseCursor != 0 {
		t.Fatalf("raced root = %#v, %v", root, decodeErr)
	}
	for _, member := range members {
		if store.values[ForwardKey(member.Reference)] == nil || store.values[ReverseKey(member.Reference)] == nil {
			t.Fatal("raced release removed a membership")
		}
	}
}

// Rationale: a corrupt aggregate count must never authorize membership
// removal, and finalization must require both the exact cursor and the
// operation-addressable reverse-prefix absence proof.
func TestNormalReleaseRejectsCountUnderflowAndPrematureFinalization(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_release_corrupt"
	members := releaseServiceMembers(store, operationID, 2)
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	guard := store.put("/tasks/current", []byte("pending"))
	startNormalRelease(t, ctx, store, repository, operationID, RetryDispositionAbandoned)

	if _, err := repository.PrepareReleaseFinalization(ctx, operationID); err == nil {
		t.Fatal("PrepareReleaseFinalization(before drain) error = nil")
	}
	countKey := CountKey(members[0].Reference.Source)
	corrupt := Count{Source: members[0].Reference.Source}
	value, err := encodeCount(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	store.put(countKey, value)
	clear(value)
	processed, drained, err := repository.ReleaseNext(ctx, operationID, []Condition{{
		Key: guard.Key, ModRevision: guard.ModRevision,
	}})
	if err == nil || processed || drained {
		t.Fatalf("ReleaseNext(underflow) = %t, %t, %v", processed, drained, err)
	}
	if store.values[ForwardKey(members[0].Reference)] == nil || store.values[ReverseKey(members[0].Reference)] == nil {
		t.Fatal("underflow release removed a membership")
	}
}

// Rationale: schema 1 has no legacy/default retry disposition; publication
// writes it explicitly and both encode and decode reject an absent or unknown
// closed-union value.
func TestOperationSourceRootRequiresExplicitRetryDisposition(t *testing.T) {
	valid := OperationSourceRoot{
		OperationID:      "op_root_schema",
		MembershipCount:  1,
		MembershipSHA256: strings.Repeat("a", 64),
		Phase:            operationSourcePhaseActive,
		ReleasePath:      sourceReleasePathAbsent,
		RetryDisposition: RetryDispositionUndecided,
	}
	encoded, err := encodeRoot(valid)
	if err != nil {
		t.Fatalf("encodeRoot(valid) error = %v", err)
	}
	decoded, err := decodeRoot(encoded)
	clear(encoded)
	if err != nil || decoded != valid {
		t.Fatalf("decodeRoot(valid) = %#v, %v", decoded, err)
	}

	invalid := valid
	invalid.RetryDisposition = ""
	if _, err := encodeRoot(invalid); err == nil {
		t.Fatal("encodeRoot(missing retry disposition) error = nil")
	}
	legacy, err := encode("script-operation-source-root", struct {
		OperationID      string `json:"operation_id"`
		MembershipCount  uint64 `json:"membership_count"`
		MembershipSHA256 string `json:"membership_sha256"`
		Phase            string `json:"phase"`
		ReleasePath      string `json:"release_path"`
		ReleaseCursor    uint64 `json:"release_cursor"`
	}{
		OperationID: valid.OperationID, MembershipCount: valid.MembershipCount,
		MembershipSHA256: valid.MembershipSHA256, Phase: valid.Phase,
		ReleasePath: valid.ReleasePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeRoot(legacy); err == nil {
		t.Fatal("decodeRoot(legacy root) error = nil")
	}
	clear(legacy)
}

// Rationale: body release decrements the exact Script-wide aggregate in the
// same transaction as its symmetric memberships and source count.
func TestNormalReleaseDecrementsScriptAggregate(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseCounterScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_release_body"
	source := SourceIdentity{
		Kind: SourceBody, EnvironmentID: "env_release", ScriptSetGeneration: "generation",
		ScriptID: "script", BodyGeneration: 1,
	}
	sourceKey := "/sources/body"
	sourceRecord := store.put(sourceKey, []byte("body"))
	store.put(ScriptPrimaryKey(source), []byte("0"))
	members := make([]Member, 16)
	for index := range members {
		members[index] = Member{
			Reference: Reference{
				OperationID: operationID, ScriptExecutionID: releaseTestID(index), Source: source,
				SourceOwnerID: "env_release", SourceModRevision: sourceRecord.ModRevision,
			},
			SourceKey: sourceKey, Mode: EvidenceExisting,
		}
	}
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	if got := string(store.values[ScriptPrimaryKey(source)].Value); got != "16" {
		t.Fatalf("prepared Script aggregate = %q, want 16", got)
	}
	guard := store.put("/tasks/current", []byte("pending"))
	startNormalRelease(t, ctx, store, repository, operationID, RetryDispositionForbidden)
	for {
		_, drained, releaseErr := repository.ReleaseNext(ctx, operationID, []Condition{{
			Key: guard.Key, ModRevision: guard.ModRevision,
		}})
		if releaseErr != nil {
			t.Fatal(releaseErr)
		}
		if drained {
			break
		}
	}
	if got := string(store.values[ScriptPrimaryKey(source)].Value); got != "0" {
		t.Fatalf("released Script aggregate = %q, want 0", got)
	}
}

// Rationale: sixteen distinct body memberships are the legal worst-case
// logical window; release must split that window deterministically instead of
// weakening the cardinality contract or exceeding the store ceiling.
func TestNormalReleaseDecomposesWorstCaseBodyWindow(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseCounterScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_release_body_window"
	members := make([]Member, normalReleaseWindowSize)
	for index := range members {
		id := releaseTestID(index)
		source := SourceIdentity{
			Kind: SourceBody, EnvironmentID: "env_release", ScriptSetGeneration: "generation",
			ScriptID: "script-" + id, BodyGeneration: 1,
		}
		sourceRecord := store.put("/sources/body/"+id, []byte("body"))
		store.put(ScriptPrimaryKey(source), []byte("0"))
		members[index] = Member{
			Reference: Reference{
				OperationID: operationID, ScriptExecutionID: "execution-" + id, Source: source,
				SourceOwnerID: "env_release", SourceModRevision: sourceRecord.ModRevision,
			},
			SourceKey: sourceRecord.Key, Mode: EvidenceExisting,
		}
	}
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	guard := store.put("/tasks/current", []byte("pending"))
	startNormalRelease(t, ctx, store, repository, operationID, RetryDispositionForbidden)
	processed, drained, err := repository.ReleaseNext(ctx, operationID, []Condition{{
		Key: guard.Key, ModRevision: guard.ModRevision,
	}})
	if err != nil || !processed || drained {
		t.Fatalf("ReleaseNext(worst-case body window) = %t, %t, %v", processed, drained, err)
	}
	root, err := decodeRoot(store.values[RootKey(operationID)].Value)
	if err != nil || root.ReleaseCursor != 11 {
		t.Fatalf("first physical cursor = %d, want 11; error = %v", root.ReleaseCursor, err)
	}
	if store.maximumTransactionOperations > releaseTransactionOperationLimit {
		t.Fatalf(
			"maximum transaction operations = %d, want <= %d",
			store.maximumTransactionOperations,
			releaseTransactionOperationLimit,
		)
	}
}

func releaseServiceMembers(store *releaseTestStore, operationID string, count int) []Member {
	members := make([]Member, count)
	for index := range members {
		id := releaseTestID(index)
		source := SourceIdentity{Kind: SourceService, ServiceID: "service-" + id}
		sourceRecord := store.put("/sources/"+id, []byte("source"))
		members[index] = Member{
			Reference: Reference{
				OperationID: operationID, ScriptExecutionID: "execution-" + id,
				Source: source, SourceOwnerID: "environment-test", SourceModRevision: sourceRecord.ModRevision,
			},
			SourceKey: sourceRecord.Key, Mode: EvidenceExisting,
		}
	}
	return members
}

func activateReleaseMembers(
	t *testing.T,
	ctx context.Context,
	store *releaseTestStore,
	repository *Repository,
	operationID string,
	members []Member,
) {
	t.Helper()
	prepared, err := repository.Prepare(ctx, operationID, members)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	publication, err := repository.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	result, err := store.Transact(ctx, publication.Conditions, publication.Mutations)
	publication.Clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("activate source set = %#v, %v", result, err)
	}
	root, err := decodeRoot(store.values[RootKey(operationID)].Value)
	if err != nil || root.RetryDisposition != RetryDispositionUndecided {
		t.Fatalf("active root = %#v, %v", root, err)
	}
}

func startNormalRelease(
	t *testing.T,
	ctx context.Context,
	store *releaseTestStore,
	repository *Repository,
	operationID string,
	disposition RetryDisposition,
) {
	t.Helper()
	start, err := repository.PrepareNormalRelease(ctx, operationID, disposition)
	if err != nil {
		t.Fatalf("PrepareNormalRelease() error = %v", err)
	}
	result, err := store.Transact(ctx, start.Conditions, start.Mutations)
	start.Clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("start release = %#v, %v", result, err)
	}
}

func assertReleaseMembersAbsent(t *testing.T, store *releaseTestStore, members []Member) {
	t.Helper()
	for _, member := range members {
		for _, key := range []string{
			ForwardKey(member.Reference), ReverseKey(member.Reference), CountKey(member.Reference.Source),
		} {
			if _, exists := store.values[key]; exists {
				t.Fatalf("release retained %q", key)
			}
		}
	}
}

type releaseTestScriptCodec struct{}

func (releaseTestScriptCodec) AdjustScriptPrimary([]byte, SourceIdentity, int64) ([]byte, error) {
	return nil, corruption("unexpected Script primary adjustment")
}

type releaseCounterScriptCodec struct{}

func (releaseCounterScriptCodec) AdjustScriptPrimary(value []byte, _ SourceIdentity, delta int64) ([]byte, error) {
	current, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil || current+delta < 0 {
		return nil, corruption("test Script aggregate underflowed")
	}
	return []byte(strconv.FormatInt(current+delta, 10)), nil
}

type releaseTestStore struct {
	revision                     int64
	values                       map[string]*KeyValue
	maximumRangeLimit            int64
	maximumTransactionOperations int
	commitThenError              bool
}

func newReleaseTestStore() *releaseTestStore {
	return &releaseTestStore{values: make(map[string]*KeyValue)}
}

func (store *releaseTestStore) put(key string, value []byte) *KeyValue {
	store.revision++
	stored := &KeyValue{Key: key, Value: append([]byte(nil), value...), ModRevision: store.revision}
	store.values[key] = stored
	copy := *stored
	copy.Value = append([]byte(nil), stored.Value...)
	return &copy
}

func (store *releaseTestStore) GetMany(_ context.Context, keys []string, _ int64) (*GetManyResult, error) {
	result := &GetManyResult{Values: make([]*KeyValue, len(keys)), ReadRevision: store.revision}
	for index, key := range keys {
		if value := store.values[key]; value != nil {
			copy := *value
			copy.Value = append([]byte(nil), value.Value...)
			result.Values[index] = &copy
		}
	}
	return result, nil
}

func (store *releaseTestStore) Range(_ context.Context, prefix string, limit int64) (*RangeResult, error) {
	store.maximumRangeLimit = max(store.maximumRangeLimit, limit)
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := &RangeResult{ReadRevision: store.revision, More: int64(len(keys)) > limit}
	if int64(len(keys)) > limit {
		keys = keys[:limit]
	}
	for _, key := range keys {
		value := store.values[key]
		result.Values = append(result.Values, KeyValue{
			Key: key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision,
		})
	}
	return result, nil
}

func (store *releaseTestStore) Transact(
	_ context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.maximumTransactionOperations = max(store.maximumTransactionOperations, len(conditions)+len(mutations))
	if len(conditions)+len(mutations) > releaseTransactionOperationLimit {
		return TransactionResult{}, validation("test transaction exceeds operation ceiling")
	}
	for _, condition := range conditions {
		if condition.Prefix {
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					return TransactionResult{Revision: store.revision}, nil
				}
			}
			continue
		}
		value := store.values[condition.Key]
		if condition.ModRevision == 0 && value != nil ||
			condition.ModRevision != 0 && (value == nil || value.ModRevision != condition.ModRevision) {
			return TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case MutationPut:
			store.values[mutation.Key] = &KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision,
			}
		case MutationDelete:
			delete(store.values, mutation.Key)
		}
	}
	if store.commitThenError {
		store.commitThenError = false
		return TransactionResult{}, errors.New("unknown transaction result")
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func releaseTestID(index int) string {
	return strconv.FormatInt(int64(index), 36)
}
