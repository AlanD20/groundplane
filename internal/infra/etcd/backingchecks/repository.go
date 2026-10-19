package backingchecks

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func NewRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Backing access check store is required")
	}
	return &Repository{store: backend}, nil
}

const RecordPrefix = "/v1/private/backing-access-checks/"

func Key(taskID string) string { return RecordPrefix + taskID }
func OperationKey(operationID string) string {
	return "/v1/indexes/backing-access-checks/by-operation/" + operationID
}
func AttachReferencePrefix(attachID string) string {
	return "/v1/indexes/backing-access-checks/by-attach/" + attachID + "/"
}
func ServiceReferencePrefix(serviceID string) string {
	return "/v1/indexes/backing-access-checks/by-service/" + serviceID + "/"
}
func EnvironmentReferencePrefix(environmentID string) string {
	return "/v1/indexes/backing-access-checks/by-environment/" + environmentID + "/"
}
func ImageReferencePrefix(agentID, imageID string) string {
	return "/v1/indexes/backing-access-checks/by-image/" + agentID + "/" + strings.TrimPrefix(imageID, "sha256:") + "/"
}
func LatestKey(attachID, grantID string) string {
	if grantID == "" {
		grantID = "own"
	}
	return "/v1/indexes/backing-access-checks/latest/" + attachID + "/" + grantID
}

// References are held through uncertain execution and cleanup. Callers join
// the exact prefixes into their lifecycle/deletion/image admission fences.
func ReferenceKeys(record Record) []string {
	input := record.Source.Parameters
	prefixes := []string{AttachReferencePrefix(input.AttachID), AttachReferencePrefix(input.CredentialAttachID),
		ServiceReferencePrefix(input.ServiceID), ServiceReferencePrefix(input.BackingServiceID),
		EnvironmentReferencePrefix(input.EnvironmentID), EnvironmentReferencePrefix(record.Source.BackingEnvironmentID),
		ImageReferencePrefix(record.Source.AgentID, record.Source.HelperImageID)}
	if record.Source.GrantID != "" {
		prefixes = append(prefixes, AttachReferencePrefix(record.Source.GrantID))
	}
	slices.Sort(prefixes)
	prefixes = slices.Compact(prefixes)
	keys := make([]string, len(prefixes))
	for index, prefix := range prefixes {
		keys[index] = prefix + record.Owner.TaskID
	}
	return keys
}

func PreparePublication(record Record) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if record.Phase != Ready {
		return nil, nil, errs.New(errs.KindValidationFailed, "Backing access check publication must be fresh")
	}
	value, err := Encode(record)
	if err != nil {
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{{Key: Key(record.Owner.TaskID)}, {Key: OperationKey(record.Owner.OperationID)}}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: conditions[0].Key, Value: value},
		{Type: keyvalue.MutationPut, Key: conditions[1].Key, Value: []byte(record.Owner.TaskID)}}
	for _, key := range ReferenceKeys(record) {
		conditions = append(conditions, keyvalue.Condition{Key: key})
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(record.Owner.TaskID)},
		)
	}
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: LatestKey(record.Source.Parameters.AttachID, record.Source.GrantID), Value: []byte(record.Owner.TaskID)})
	return conditions, mutations, nil
}

func (repository *Repository) Load(ctx context.Context, taskID string) (keyvalue.Versioned[Record], error) {
	return repository.loadAt(ctx, taskID, 0)
}

func (repository *Repository) LoadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	if revision <= 0 {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed,
			"backing check snapshot revision is required",
		)
	}
	return repository.loadAt(ctx, taskID, revision)
}

func (repository *Repository) loadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed,
			"Backing access check Task id is invalid",
		)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "Backing access check read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindRequestNotFound, "Backing access check was not found")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID {
		record.Clear()
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "Backing access check owner is corrupt")
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func (repository *Repository) Latest(
	ctx context.Context,
	attachID, grantID string,
) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindAttach, attachID) != nil || grantID != "" && ids.Validate(ids.KindAttach, grantID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed,
			"Backing access check selection is invalid",
		)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{LatestKey(attachID, grantID)}})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"Backing access check latest read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindRequestNotFound,
			"this Attach has no retained access check",
		)
	}
	taskID := string(read.Values[0].Value)
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "Backing access check latest index is corrupt")
	}
	input, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{Key(taskID)}, Revision: read.ReadRevision},
	)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if input == nil || input.ReadRevision != read.ReadRevision || len(input.Values) != 1 || input.Values[0] == nil {
		if input != nil {
			keyvalue.ClearValues(input.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindStateConflict,
			"latest Backing access check is unavailable",
		)
	}
	defer keyvalue.ClearValues(input.Values)
	record, err := Decode(input.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID || record.Source.Parameters.AttachID != attachID ||
		record.Source.GrantID != grantID {
		record.Clear()
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"latest Backing access check changed selection",
		)
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     input.Values[0].ModRevision,
		ReadRevision: input.ReadRevision,
	}, nil
}
