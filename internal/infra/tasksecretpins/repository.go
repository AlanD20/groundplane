package tasksecretpins

import (
	"bytes"
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, validation("secret pin store is missing")
	}
	return &Repository{store: store}, nil
}

func (repository *Repository) Prepare(
	ctx context.Context,
	operationID string,
	taskID string,
	pins []tasksecretpinrecord.Record,
) (Prepared, error) {
	if ctx == nil {
		return Prepared{}, validation("secret pin preparation context is missing")
	}
	canonical, digest, err := canonicalPins(operationID, taskID, pins)
	if err != nil {
		return Prepared{}, err
	}
	expected := setRecord{
		Schema: setSchema, OperationID: operationID, TaskID: taskID,
		MembershipCount: uint64(len(canonical)), MembershipSHA256: digest, Phase: phasePreparing,
	}
	descriptor, revision, err := repository.ensurePreparation(ctx, expected)
	if err != nil {
		return Prepared{}, err
	}
	for descriptor.Phase == phasePreparing && descriptor.PreparationCursor < descriptor.MembershipCount {
		pin := canonical[descriptor.PreparationCursor]
		descriptor, revision, err = repository.prepareMember(ctx, descriptor, revision, pin)
		if err != nil {
			return Prepared{}, err
		}
	}
	if descriptor.Phase == phasePreparing {
		descriptor, revision, err = repository.sealPreparation(ctx, descriptor, revision)
		if err != nil {
			return Prepared{}, err
		}
	}
	if descriptor.Phase != phaseSealed || descriptor.PreparationCursor != descriptor.MembershipCount ||
		!sameSet(descriptor, expected) {
		return Prepared{}, conflict("secret pin preparation descriptor is occupied")
	}
	return Prepared{record: descriptor, revision: revision}, nil
}

func (repository *Repository) ensurePreparation(
	ctx context.Context,
	expected setRecord,
) (setRecord, int64, error) {
	keys := []string{PreparationKey(expected.OperationID), RootKey(expected.OperationID)}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return setRecord{}, 0, err
	}
	if read.Values[1] != nil {
		return setRecord{}, 0, conflict("secret pin set is already active")
	}
	if read.Values[0] != nil {
		return matchingPreparation(read.Values[0], expected)
	}
	value, err := encodeSet(expected)
	if err != nil {
		return setRecord{}, 0, err
	}
	defer clear(value)
	result, err := repository.transact(ctx,
		[]Condition{{Key: keys[0]}, {Key: keys[1]}},
		[]Mutation{{Type: MutationPut, Key: keys[0], Value: value}},
	)
	if err != nil {
		return setRecord{}, 0, err
	}
	if result.Succeeded {
		return expected, result.Revision, nil
	}
	retry, err := repository.read(ctx, keys, 0)
	if err != nil {
		return setRecord{}, 0, err
	}
	if retry.Values[1] != nil || retry.Values[0] == nil {
		return setRecord{}, 0, conflict("secret pin preparation identity raced durable state")
	}
	return matchingPreparation(retry.Values[0], expected)
}

func matchingPreparation(value *KeyValue, expected setRecord) (setRecord, int64, error) {
	descriptor, err := decodeSet(value.Value)
	if err != nil {
		return setRecord{}, 0, err
	}
	if !sameSet(descriptor, expected) || descriptor.ReleaseCursor != 0 ||
		(descriptor.Phase != phasePreparing && descriptor.Phase != phaseSealed) {
		return setRecord{}, 0, conflict("secret pin preparation identity has different input")
	}
	return descriptor, value.ModRevision, nil
}

func sameSet(left, right setRecord) bool {
	return left.OperationID == right.OperationID && left.TaskID == right.TaskID &&
		left.MembershipCount == right.MembershipCount && left.MembershipSHA256 == right.MembershipSHA256
}

func (repository *Repository) prepareMember(
	ctx context.Context,
	descriptor setRecord,
	descriptorRevision int64,
	pin tasksecretpinrecord.Record,
) (setRecord, int64, error) {
	authority, err := repository.verifySecret(ctx, pin)
	if err != nil {
		return setRecord{}, 0, err
	}
	ordinal := descriptor.PreparationCursor + 1
	forwardKey := tasksecretpinrecord.Key(pin.SecretID, descriptor.OperationID)
	reverseKey := ReverseKey(descriptor.OperationID, ordinal)
	value, err := tasksecretpinrecord.Encode(pin)
	if err != nil {
		return setRecord{}, 0, err
	}
	defer clear(value)
	descriptor.PreparationCursor = ordinal
	descriptorValue, err := encodeSet(descriptor)
	if err != nil {
		return setRecord{}, 0, err
	}
	defer clear(descriptorValue)
	conditions := []Condition{
		{Key: PreparationKey(descriptor.OperationID), ModRevision: descriptorRevision},
		{Key: RootKey(descriptor.OperationID)},
		{Key: secretMetadataKey(pin.SecretID), ModRevision: authority.MetadataRevision},
		{Key: secretValueKey(pin.SecretID), ModRevision: authority.ValueRevision},
		{Key: secretTombstoneKey(pin.SecretID)},
		{Key: forwardKey},
		{Key: reverseKey},
	}
	if authority.ProjectID != "" {
		conditions = append(conditions, Condition{Key: projectTombstoneKey(authority.ProjectID)})
	}
	if authority.TenantID != "" {
		conditions = append(conditions, Condition{Key: tenantTombstoneKey(authority.TenantID)})
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: forwardKey, Value: value},
		{Type: MutationPut, Key: reverseKey, Value: append([]byte(nil), value...)},
		{Type: MutationPut, Key: PreparationKey(descriptor.OperationID), Value: descriptorValue},
	}
	defer clearMutations(mutations)
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return setRecord{}, 0, err
	}
	if !result.Succeeded {
		return setRecord{}, 0, conflict("secret pin member preparation raced durable state")
	}
	return descriptor, result.Revision, nil
}

func (repository *Repository) sealPreparation(
	ctx context.Context,
	descriptor setRecord,
	descriptorRevision int64,
) (setRecord, int64, error) {
	if descriptor.PreparationCursor != descriptor.MembershipCount {
		return setRecord{}, 0, corruption("secret pin preparation cursor is incomplete")
	}
	descriptor.Phase = phaseSealed
	value, err := encodeSet(descriptor)
	if err != nil {
		return setRecord{}, 0, err
	}
	defer clear(value)
	result, err := repository.transact(ctx, []Condition{
		{Key: PreparationKey(descriptor.OperationID), ModRevision: descriptorRevision},
		{Key: RootKey(descriptor.OperationID)},
	}, []Mutation{{Type: MutationPut, Key: PreparationKey(descriptor.OperationID), Value: value}})
	if err != nil {
		return setRecord{}, 0, err
	}
	if !result.Succeeded {
		return setRecord{}, 0, conflict("secret pin preparation seal raced durable state")
	}
	return descriptor, result.Revision, nil
}

func (repository *Repository) verifySecret(
	ctx context.Context,
	pin tasksecretpinrecord.Record,
) (SecretAuthority, error) {
	authority, err := repository.store.VerifySecret(ctx, pin)
	if err != nil {
		return SecretAuthority{}, err
	}
	if authority.MetadataRevision != pin.MetadataRevision || authority.ValueRevision <= 0 ||
		(authority.ProjectID == "" && authority.TenantID != "") ||
		(authority.ProjectID != "" && ids.Validate(ids.KindProject, authority.ProjectID) != nil) ||
		(authority.TenantID != "" && ids.Validate(ids.KindTenant, authority.TenantID) != nil) {
		return SecretAuthority{}, corruption("secret adapter returned invalid pin authority")
	}
	return authority, nil
}

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	result, err := repository.store.GetMany(ctx, keys, revision)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if result == nil || len(result.Values) != len(keys) || result.ReadRevision < 0 ||
		(revision > 0 && result.ReadRevision != revision) {
		return nil, corruption("secret pin store read is inconsistent")
	}
	for index, value := range result.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 ||
			value.ModRevision > result.ReadRevision) {
			return nil, corruption("secret pin store value is inconsistent")
		}
	}
	return result, nil
}

func (repository *Repository) transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return TransactionResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if result.Succeeded && result.Revision <= 0 {
		return TransactionResult{}, corruption("secret pin transaction revision is invalid")
	}
	return result, nil
}

func (repository *Repository) rangeValues(
	ctx context.Context,
	prefix string,
	limit int64,
) (*RangeResult, error) {
	result, err := repository.store.Range(ctx, prefix, limit)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if result == nil || limit <= 0 || int64(len(result.Values)) > limit || result.ReadRevision <= 0 ||
		(result.More && len(result.Values) == 0) {
		return nil, corruption("secret pin store range is inconsistent")
	}
	previous := ""
	for _, value := range result.Values {
		if !strings.HasPrefix(value.Key, prefix) || value.ModRevision <= 0 ||
			value.ModRevision > result.ReadRevision || (previous != "" && value.Key <= previous) {
			return nil, corruption("secret pin store range value is inconsistent")
		}
		previous = value.Key
	}
	return result, nil
}

func equalValue(left, right []byte) bool { return bytes.Equal(left, right) }
