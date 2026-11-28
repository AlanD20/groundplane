package runnercredential

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	identity "github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumLifetime            = time.Hour
	maximumAuthorityConditions = 32
	maximumPrunedCredentials   = (keyvalue.MaximumOperations - maximumAuthorityConditions - 7) / 2
)

// IssueRequest captures a token digest minted by the authenticated transport.
// Authority contains Controller-owned compares for the approved grant or job.
// The authority owner also checks its current grant during each action's
// admission and appends the corresponding conditions to publication.
type IssueRequest struct {
	Digest      [sha256.Size]byte
	RunnerID    string
	RequestID   string
	RunnerEpoch uint64
	ExpiresAt   time.Time
	Authority   []keyvalue.Condition
}

// Issue grants one active credential for a Runner and request. Expired and
// superseded credentials are collected in the same bounded transaction.
func (repository *Repository) Issue(ctx context.Context, input IssueRequest) (identity.Binding, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return identity.Binding{}, err
	}
	now := repository.now().UTC()
	record := credentialRecord{
		Digest: hex.EncodeToString(input.Digest[:]), RunnerID: input.RunnerID, RequestID: input.RequestID,
		RunnerEpoch: input.RunnerEpoch, ExpiresAt: input.ExpiresAt.UTC(),
	}
	if !validCredentialRecord(record) || !now.Before(record.ExpiresAt) ||
		record.ExpiresAt.After(now.Add(MaximumLifetime)) || !validAuthority(input.Authority) {
		return identity.Binding{}, errs.New(errs.KindValidationFailed, "Runner credential issuance is invalid")
	}
	setKey, tokenKey := runnerKey(record.RunnerID), credentialKey(input.Digest)
	keys := append(registrationKeys(record.RunnerID), setKey, tokenKey)
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return identity.Binding{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	conditions, live, err := liveRegistration(read.Values, record.RunnerID, record.RunnerEpoch)
	if err != nil {
		return identity.Binding{}, err
	}
	if !live || read.Values[4] != nil {
		return identity.Binding{}, credentialConflict()
	}
	setValue := read.Values[3]
	indexed, err := repository.readSet(ctx, record.RunnerID, setValue, read.ReadRevision)
	if err != nil {
		return identity.Binding{}, err
	}
	conditions = append(conditions, compare(setKey, setValue), keyvalue.Condition{Key: tokenKey})
	set := credentialSet{RunnerID: record.RunnerID}
	var mutations []keyvalue.Mutation
	for _, item := range indexed {
		sameRequest := item.record.RunnerEpoch == record.RunnerEpoch && item.record.RequestID == record.RequestID
		stale := item.record.RunnerEpoch < record.RunnerEpoch || !item.record.binding(item.revision).ActiveAt(now)
		// Reserve one removal for an expired credential for this exact request.
		if stale && (sameRequest || len(mutations) < maximumPrunedCredentials-1) {
			conditions = append(conditions, keyvalue.Condition{Key: item.key, ModRevision: item.revision})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: item.key})
			continue
		}
		if sameRequest {
			return identity.Binding{}, errs.New(
				errs.KindStateConflict,
				"Runner request already has an active credential",
			)
		}
		set.Digests = append(set.Digests, item.record.Digest)
	}
	if len(set.Digests) >= maximumCredentials {
		return identity.Binding{}, errs.New(errs.KindResourceInUse, "Runner credential capacity is in use")
	}
	set.Digests = append(set.Digests, record.Digest)
	setBytes, err := recordcodec.Encode(credentialSetKind, set)
	if err != nil {
		return identity.Binding{}, err
	}
	defer clear(setBytes)
	credentialBytes, err := recordcodec.Encode(credentialKind, record)
	if err != nil {
		return identity.Binding{}, err
	}
	defer clear(credentialBytes)
	mutations = append(mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: setKey, Value: setBytes},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: tokenKey, Value: credentialBytes},
	)
	if !repository.now().Before(record.ExpiresAt) {
		return identity.Binding{}, credentialConflict()
	}
	conditions, err = mergeAuthority(conditions, input.Authority)
	if err != nil {
		return identity.Binding{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return identity.Binding{}, err
	}
	if !result.Succeeded {
		return identity.Binding{}, credentialConflict()
	}
	if result.Revision <= 0 {
		return identity.Binding{}, corrupt()
	}
	return record.binding(result.Revision), nil
}

// A captured authority includes a currently existing approved grant or job.
// Absence conditions may further fence deletion or competing authorization.
func validAuthority(conditions []keyvalue.Condition) bool {
	if len(conditions) == 0 || len(conditions) > maximumAuthorityConditions {
		return false
	}
	existing := false
	for _, condition := range conditions {
		if condition.Key == "" || condition.ModRevision < 0 || condition.Prefix && condition.ModRevision != 0 {
			return false
		}
		existing = existing || condition.ModRevision > 0
	}
	return existing
}

func mergeAuthority(base, authority []keyvalue.Condition) ([]keyvalue.Condition, error) {
	type conditionKey struct {
		key    string
		prefix bool
	}
	seen := make(map[conditionKey]int64, len(base)+len(authority))
	for _, condition := range base {
		seen[conditionKey{condition.Key, condition.Prefix}] = condition.ModRevision
	}
	for _, condition := range authority {
		key := conditionKey{condition.Key, condition.Prefix}
		if revision, existing := seen[key]; !existing {
			seen[key] = condition.ModRevision
			base = append(base, condition)
		} else if revision != condition.ModRevision {
			return nil, credentialConflict()
		}
	}
	return base, nil
}

type indexedCredential struct {
	key      string
	record   credentialRecord
	revision int64
}

func (repository *Repository) readSet(
	ctx context.Context, runnerID string, setValue *keyvalue.KeyValue, revision int64,
) ([]indexedCredential, error) {
	set, err := decodeSet(setValue, runnerID)
	if err != nil || len(set.Digests) == 0 {
		return nil, err
	}
	keys := make([]string, 0, len(set.Digests))
	for _, digest := range set.Digests {
		keys = append(keys, credentialPrefix+digest)
	}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	indexed := make([]indexedCredential, 0, len(keys))
	type requestEpoch struct {
		requestID string
		epoch     uint64
	}
	seen := make(map[requestEpoch]bool, len(keys))
	for index, value := range read.Values {
		record, err := decodeCredential(value, keys[index])
		request := requestEpoch{record.RequestID, record.RunnerEpoch}
		if err != nil || record.RunnerID != runnerID || seen[request] {
			return nil, corrupt()
		}
		seen[request] = true
		indexed = append(indexed, indexedCredential{key: keys[index], record: record, revision: value.ModRevision})
	}
	return indexed, nil
}

func compare(key string, value *keyvalue.KeyValue) keyvalue.Condition {
	condition := keyvalue.Condition{Key: key}
	if value != nil {
		condition.ModRevision = value.ModRevision
	}
	return condition
}

func credentialConflict() error {
	return errs.New(errs.KindStateConflict, "Runner credential authority changed")
}
