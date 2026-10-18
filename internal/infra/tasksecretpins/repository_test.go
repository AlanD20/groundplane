package tasksecretpins

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: SEC-07 reservation must fence the exact metadata, ciphertext,
// deletion, and owner authorities per Secret; a partial preparation must resume
// without duplicating membership needed by SVC-15 and JOURNEY-02 recovery.
func TestPrepareFencesSecretAuthorityAndResumesPartialWork(t *testing.T) {
	ctx := context.Background()
	store, operationID, taskID, workID, pins := pinFixture(t, 3)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	store.failTransaction = 3
	if _, err := repository.Prepare(ctx, operationID, taskID, workID, pins); !isKind(err, errs.KindStorageUnavailable) {
		t.Fatalf("interrupted preparation = %v", err)
	}
	descriptor := store.mustSet(t, PreparationKey(operationID))
	if descriptor.PreparationCursor != 1 || descriptor.Phase != phasePreparing {
		t.Fatalf("partial preparation = %#v", descriptor)
	}
	store.failTransaction = 0
	prepared, err := repository.Prepare(ctx, operationID, taskID, workID, pins)
	if err != nil || prepared.MembershipCount() != 3 || prepared.TaskID() != taskID {
		t.Fatalf("resumed preparation = %#v/%v", prepared, err)
	}
	for ordinal, pin := range pins {
		forward := store.values[tasksecretpinrecord.Key(pin.SecretID, operationID)]
		reverse := store.values[ReverseKey(operationID, uint64(ordinal+1))]
		if forward == nil || reverse == nil || !equalValue(forward.Value, reverse.Value) {
			t.Fatalf("membership %d missing or divergent", ordinal+1)
		}
	}
	if store.maximumOperations > 96 || store.maximumBytes >= 1<<20 {
		t.Fatalf("preparation transaction size = %d operations/%d bytes", store.maximumOperations, store.maximumBytes)
	}

	changed := append([]tasksecretpinrecord.Record(nil), pins...)
	changed[0].CiphertextSHA256 = digestString("different")
	if _, err := repository.Prepare(ctx, operationID, taskID, workID, changed); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("mismatched replay = %v", err)
	}
	unsorted := []tasksecretpinrecord.Record{pins[1], pins[0]}
	if _, err := repository.Prepare(ctx, operationID, taskID, workID, unsorted); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("unsorted pins = %v", err)
	}
	tooMany := make([]tasksecretpinrecord.Record, MaximumPins+1)
	if _, err := repository.Prepare(ctx, operationID, taskID, workID, tooMany); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("oversized pin set = %v", err)
	}
}

// Rationale: SEC-07 requires pin reservation and Secret deletion publication
// to be mutually fenced; a tombstone committed after verification must win
// without leaving a forward or reverse recovery membership.
func TestPrepareLosesSecretTombstoneRace(t *testing.T) {
	ctx := context.Background()
	store, operationID, taskID, workID, pins := pinFixture(t, 1)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	store.beforeTransaction = func(store *pinStore, conditions []Condition, mutations []Mutation) {
		for _, mutation := range mutations {
			if mutation.Key != tasksecretpinrecord.Key(pins[0].SecretID, operationID) {
				continue
			}
			for _, key := range []string{
				secretMetadataKey(pins[0].SecretID), secretValueKey(pins[0].SecretID),
				secretTombstoneKey(pins[0].SecretID),
				projectTombstoneKey(store.authorities[pins[0].SecretID].ProjectID),
				tenantTombstoneKey(store.authorities[pins[0].SecretID].TenantID),
			} {
				if !containsCondition(conditions, key) {
					t.Fatalf("member preparation omitted source fence %s", key)
				}
			}
			store.beforeTransaction = nil
			store.put(secretTombstoneKey(pins[0].SecretID), []byte("deleting"))
			return
		}
	}
	if _, err := repository.Prepare(ctx, operationID, taskID, workID, pins); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("tombstone race = %v", err)
	}
	if store.values[tasksecretpinrecord.Key(pins[0].SecretID, operationID)] != nil ||
		store.values[ReverseKey(operationID, 1)] != nil {
		t.Fatal("losing preparation retained membership")
	}
}

// Rationale: SEC-07 activation must remain constant size and caller-owned;
// restart loading must recover the exact Task and set identity before terminal
// code can release SVC-15/JOURNEY-02 source pins.
func TestActivationIsConstantSizeAndLoadActiveRestoresOwnership(t *testing.T) {
	ctx := context.Background()
	store, operationID, taskID, workID, pins := pinFixture(t, 24)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repository.Prepare(ctx, operationID, taskID, workID, pins)
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := Activation(prepared)
	if err != nil {
		t.Fatalf("activation fragment = %#v/%v", fragment, err)
	}
	defer fragment.Clear()
	conditions := append(fragment.Conditions,
		Condition{Key: fixtureWorkInputKey(workID)})
	mutations := append(fragment.Mutations,
		Mutation{Type: MutationPut, Key: fixtureWorkInputKey(workID), Value: []byte("captured input")})
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("caller-owned activation = %#v/%v", result, err)
	}
	root, found, err := repository.LoadActive(ctx, operationID, 0)
	if err != nil || !found || root.OperationID() != operationID || root.TaskID() != taskID ||
		root.MembershipCount() != uint64(len(pins)) || root.MembershipSHA256() != prepared.MembershipSHA256() ||
		root.Revision() != result.Revision || root.Phase() != RootPhaseActive {
		t.Fatalf("LoadActive() = %#v/%t/%v", root, found, err)
	}
}

// Rationale: SEC-07 release may begin only through an explicit caller-owned
// root fragment, then bounded restart-safe batches remove exact memberships;
// another operation's pin for the same Secret must survive.
func TestExplicitReleaseIsBoundedResumableAndOperationOwned(t *testing.T) {
	ctx := context.Background()
	store, operationID, taskID, workID, pins := pinFixture(t, 20)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repository.Prepare(ctx, operationID, taskID, workID, pins)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := Activation(prepared)
	if err != nil {
		t.Fatal(err)
	}
	activationConditions := append(activation.Conditions,
		Condition{Key: fixtureWorkInputKey(workID)})
	activationMutations := append(activation.Mutations,
		Mutation{Type: MutationPut, Key: fixtureWorkInputKey(workID), Value: []byte("captured input")})
	activationResult, err := store.Transact(ctx, activationConditions, activationMutations)
	activation.Clear()
	if err != nil || !activationResult.Succeeded {
		t.Fatalf("activate pins = %#v/%v", activationResult, err)
	}
	active, err := prepared.ActiveRoot(activationResult.Revision)
	if err != nil {
		t.Fatal(err)
	}
	otherOperation := ids.NewAt(ids.KindOperation, testTime(), 900)
	other := pins[0]
	other.OperationID = otherOperation
	otherValue, err := tasksecretpinrecord.Encode(other)
	if err != nil {
		t.Fatal(err)
	}
	store.put(tasksecretpinrecord.Key(other.SecretID, otherOperation), otherValue)
	if _, _, err := repository.ReleaseBatch(ctx, operationID); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("release before explicit authority = %v", err)
	}

	release, err := BeginRelease(active)
	if err != nil {
		t.Fatal(err)
	}
	if store.mustSet(t, RootKey(operationID)).Phase != phaseActive {
		t.Fatal("pure BeginRelease changed the store")
	}
	input := store.values[fixtureWorkInputKey(workID)]
	releaseConditions := append(release.Conditions, Condition{Key: input.Key, ModRevision: input.ModRevision})
	releaseMutations := append(release.Mutations, Mutation{Type: MutationDelete, Key: input.Key})
	result, err := store.Transact(ctx, releaseConditions, releaseMutations)
	release.Clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("begin release = %#v/%v", result, err)
	}
	progressed, done, err := repository.ReleaseBatch(ctx, operationID)
	if err != nil || !progressed || done {
		t.Fatalf("first release batch = %t/%t/%v", progressed, done, err)
	}
	restarted, found, err := repository.LoadActive(ctx, operationID, 0)
	if err != nil || !found || restarted.Phase() != RootPhaseReleasing ||
		store.mustSet(t, RootKey(operationID)).ReleaseCursor != releaseBatchSize {
		t.Fatalf("restarted release = %#v/%t/%v", restarted, found, err)
	}
	for !done {
		progressed, done, err = repository.ReleaseBatch(ctx, operationID)
		if err != nil || !progressed {
			t.Fatalf("resumed release = %t/%t/%v", progressed, done, err)
		}
	}
	if store.values[RootKey(operationID)] != nil ||
		store.values[tasksecretpinrecord.Key(other.SecretID, otherOperation)] == nil {
		t.Fatal("release removed another operation's pin or retained its root")
	}
	if store.maximumOperations > 96 || store.maximumBytes >= 1<<20 {
		t.Fatalf("release transaction size = %d operations/%d bytes", store.maximumOperations, store.maximumBytes)
	}
}

// Rationale: SEC-07 native input activation consumes the preparation atomically.
// Whichever side wins must fence the other, without consulting Task history.
func TestAbandonRacesAtomicWorkInputActivation(t *testing.T) {
	for _, publicationWins := range []bool{false, true} {
		t.Run(strconv.FormatBool(publicationWins), func(t *testing.T) {
			ctx := context.Background()
			store, operationID, taskID, workID, pins := pinFixture(t, 2)
			repository, err := NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := repository.Prepare(ctx, operationID, taskID, workID, pins)
			if err != nil {
				t.Fatal(err)
			}
			activation, err := Activation(prepared)
			if err != nil {
				t.Fatal(err)
			}
			defer activation.Clear()
			activate := func() (TransactionResult, error) {
				return store.Transact(
					ctx,
					append(activation.Conditions, Condition{Key: fixtureWorkInputKey(workID)}),
					append(
						activation.Mutations,
						Mutation{Type: MutationPut, Key: fixtureWorkInputKey(workID), Value: []byte("captured input")},
					),
				)
			}
			if publicationWins {
				store.beforeTransaction = func(store *pinStore, _ []Condition, mutations []Mutation) {
					for _, mutation := range mutations {
						if mutation.Key == PreparationKey(operationID) {
							store.beforeTransaction = nil
							result, err := activate()
							if err != nil || !result.Succeeded {
								t.Fatalf("racing activation: %+v/%v", result, err)
							}
							return
						}
					}
				}
			}
			restarted, err := NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			err = restarted.Abandon(ctx, operationID)
			if publicationWins {
				if !isKind(err, errs.KindStateConflict) {
					t.Fatalf("publication won but abandonment returned %v", err)
				}
				if store.values[RootKey(operationID)] == nil || store.values[fixtureWorkInputKey(workID)] == nil {
					t.Fatal("atomic publication lost root or captured input")
				}
				for _, pin := range pins {
					if store.values[tasksecretpinrecord.Key(pin.SecretID, operationID)] == nil {
						t.Fatal("abandonment removed a published input pin")
					}
				}
				if err := restarted.Abandon(ctx, operationID); !isKind(err, errs.KindStateConflict) {
					t.Fatalf("active root abandoned: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				result, err := activate()
				if err != nil || result.Succeeded {
					t.Fatalf("abandoned preparation published late: %+v/%v", result, err)
				}
				for _, pin := range pins {
					if store.values[tasksecretpinrecord.Key(pin.SecretID, operationID)] != nil {
						t.Fatal("abandoned preparation retained a pin")
					}
				}
			}
		})
	}
}

// The pin owner treats work input publication as an opaque caller-owned write;
// it must never inspect Task or diagnostic history to decide retention rights.
func fixtureWorkInputKey(workID string) string { return "/test/captured-work-input/" + workID }

type pinStore struct {
	revision          int64
	values            map[string]*KeyValue
	authorities       map[string]SecretAuthority
	digests           map[string]string
	transactionCount  int
	failTransaction   int
	beforeTransaction func(*pinStore, []Condition, []Mutation)
	maximumOperations int
	maximumBytes      int
}

func newPinStore() *pinStore {
	return &pinStore{
		revision: 1, values: make(map[string]*KeyValue),
		authorities: make(map[string]SecretAuthority), digests: make(map[string]string),
	}
}

func (store *pinStore) GetMany(
	_ context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	if revision != 0 && revision != store.revision {
		return nil, errors.New("historical reads are unavailable")
	}
	values := make([]*KeyValue, len(keys))
	for index, key := range keys {
		values[index] = cloneKeyValue(store.values[key])
	}
	return &GetManyResult{Values: values, ReadRevision: store.revision}, nil
}

func (store *pinStore) Range(_ context.Context, prefix string, limit int64) (*RangeResult, error) {
	keys := make([]string, 0)
	for key := range store.values {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := int64(len(keys)) > limit
	if more {
		keys = keys[:limit]
	}
	values := make([]KeyValue, len(keys))
	for index, key := range keys {
		values[index] = *cloneKeyValue(store.values[key])
	}
	return &RangeResult{Values: values, ReadRevision: store.revision, More: more}, nil
}

func (store *pinStore) Transact(
	_ context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.transactionCount++
	if store.beforeTransaction != nil {
		store.beforeTransaction(store, conditions, mutations)
	}
	if store.failTransaction == store.transactionCount {
		return TransactionResult{}, errors.New("injected transaction interruption")
	}
	store.measure(conditions, mutations)
	for _, condition := range conditions {
		if condition.Prefix {
			for key := range store.values {
				if len(key) >= len(condition.Key) && key[:len(condition.Key)] == condition.Key {
					return TransactionResult{Revision: store.revision}, nil
				}
			}
			continue
		}
		current := store.values[condition.Key]
		actual := int64(0)
		if current != nil {
			actual = current.ModRevision
		}
		if actual != condition.ModRevision {
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
			if mutation.Prefix {
				for key := range store.values {
					if len(key) >= len(mutation.Key) && key[:len(mutation.Key)] == mutation.Key {
						delete(store.values, key)
					}
				}
			} else {
				delete(store.values, mutation.Key)
			}
		default:
			return TransactionResult{}, errors.New("invalid mutation")
		}
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *pinStore) VerifySecret(
	_ context.Context,
	pin tasksecretpinrecord.Record,
) (SecretAuthority, error) {
	authority, found := store.authorities[pin.SecretID]
	if !found || authority.MetadataRevision != pin.MetadataRevision ||
		store.digests[pin.SecretID] != pin.CiphertextSHA256 {
		return SecretAuthority{}, errs.New(errs.KindStateConflict, "Secret pin source changed")
	}
	return authority, nil
}

func (store *pinStore) put(key string, value []byte) int64 {
	store.revision++
	store.values[key] = &KeyValue{Key: key, Value: append([]byte(nil), value...), ModRevision: store.revision}
	return store.revision
}

func (store *pinStore) delete(key string) {
	store.revision++
	delete(store.values, key)
}

func (store *pinStore) seedSecret(pin tasksecretpinrecord.Record, projectID, tenantID string) {
	metadataRevision := store.put(secretMetadataKey(pin.SecretID), []byte("metadata"))
	valueRevision := store.put(secretValueKey(pin.SecretID), []byte("encrypted"))
	store.authorities[pin.SecretID] = SecretAuthority{
		MetadataRevision: metadataRevision, ValueRevision: valueRevision,
		ProjectID: projectID, TenantID: tenantID,
	}
	store.digests[pin.SecretID] = pin.CiphertextSHA256
}

func (store *pinStore) mustSet(t *testing.T, key string) setRecord {
	t.Helper()
	value := store.values[key]
	if value == nil {
		t.Fatalf("missing set %s", key)
	}
	record, err := decodeSet(value.Value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (store *pinStore) measure(conditions []Condition, mutations []Mutation) {
	operations := len(conditions) + len(mutations)
	if operations > store.maximumOperations {
		store.maximumOperations = operations
	}
	bytes := 0
	for _, condition := range conditions {
		bytes += len(condition.Key) + 16
	}
	for _, mutation := range mutations {
		bytes += len(mutation.Key) + len(mutation.Value) + 16
	}
	if bytes > store.maximumBytes {
		store.maximumBytes = bytes
	}
}

func cloneKeyValue(value *KeyValue) *KeyValue {
	if value == nil {
		return nil
	}
	return &KeyValue{Key: value.Key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision}
}

func containsCondition(conditions []Condition, key string) bool {
	for _, condition := range conditions {
		if condition.Key == key {
			return true
		}
	}
	return false
}

func pinFixture(
	t *testing.T,
	count int,
) (*pinStore, string, string, string, []tasksecretpinrecord.Record) {
	t.Helper()
	at := testTime()
	store := newPinStore()
	operationID := ids.NewAt(ids.KindOperation, at, 1)
	taskID := ids.NewAt(ids.KindTask, at, 2)
	workID := ids.NewAt(ids.KindWorkItem, at, 5)
	projectID := ids.NewAt(ids.KindProject, at, 3)
	tenantID := ids.NewAt(ids.KindTenant, at, 4)
	pins := make([]tasksecretpinrecord.Record, count)
	for index := range count {
		pin := tasksecretpinrecord.Record{
			OperationID: operationID, SecretID: ids.NewAt(ids.KindSecret, at, int64(index+10)),
			CiphertextSHA256: digestString(string(rune(index + 1))),
		}
		store.seedSecret(pin, projectID, tenantID)
		pin.MetadataRevision = store.authorities[pin.SecretID].MetadataRevision
		pins[index] = pin
	}
	sort.Slice(pins, func(left, right int) bool { return pins[left].SecretID < pins[right].SecretID })
	return store, operationID, taskID, workID, pins
}

func digestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func testTime() time.Time { return time.Date(2026, 9, 14, 16, 0, 0, 0, time.UTC) }

func isKind(err error, kind errs.Kind) bool {
	actual, found := errs.KindOf(err)
	return found && actual == kind
}
