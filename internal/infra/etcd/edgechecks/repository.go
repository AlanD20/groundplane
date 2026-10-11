package edgechecks

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
		return nil, errs.New(errs.KindInternal, "edge diagnostic store is required")
	}
	return &Repository{store: backend}, nil
}

func Key(taskID string) string { return "/v1/private/edge-connection-checks/" + taskID }
func OperationKey(operationID string) string {
	return "/v1/indexes/edge-connection-checks/by-operation/" + operationID
}
func LatestKey(routeID string) string { return "/v1/indexes/edge-connection-checks/latest/" + routeID }
func ComponentReferencePrefix(componentID string) string {
	return "/v1/indexes/edge-connection-checks/by-component/" + componentID + "/"
}
func ServiceReferencePrefix(serviceID string) string {
	return "/v1/indexes/edge-connection-checks/by-service/" + serviceID + "/"
}
func RouteReferencePrefix(routeID string) string {
	return "/v1/indexes/edge-connection-checks/by-route/" + routeID + "/"
}
func ZoneReferencePrefix(zoneID string) string {
	return "/v1/indexes/edge-connection-checks/by-zone/" + zoneID + "/"
}
func EnvironmentReferencePrefix(environmentID string) string {
	return "/v1/indexes/edge-connection-checks/by-environment/" + environmentID + "/"
}
func ImageReferencePrefix(agentID, imageID string) string {
	return "/v1/indexes/edge-connection-checks/by-image/" + agentID + "/" + strings.TrimPrefix(imageID, "sha256:") + "/"
}

// These references protect only captured resources and the matching helper
// image. They remain held through uncertain effects and cleanup-only Retry.
func ReferenceKeys(record Record) []string {
	request := record.Source.Request
	prefixes := []string{
		ComponentReferencePrefix(request.ComponentID),
		ServiceReferencePrefix(
			request.ServiceID,
		),
		EnvironmentReferencePrefix(request.EnvironmentID),
		ImageReferencePrefix(record.Source.AgentID, record.Source.HelperImageID),
	}
	if request.RouteID != "" {
		prefixes = append(
			prefixes,
			ComponentReferencePrefix(record.Source.RouterComponentID),
			RouteReferencePrefix(request.RouteID),
		)
	} else {
		prefixes = append(prefixes, ServiceReferencePrefix(request.OriginServiceID))
	}
	for _, zone := range record.Source.Zones {
		prefixes = append(prefixes, ZoneReferencePrefix(zone.ID))
	}
	slices.Sort(prefixes)
	prefixes = slices.Compact(prefixes)
	keys := make([]string, len(prefixes))
	for index, prefix := range prefixes {
		keys[index] = prefix + record.Owner.TaskID
	}
	return keys
}

// Source comparisons, the exact latest-index revision, native Task publication
// and idempotency must join these operations in one transaction at admission.
func PreparePublication(record Record) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if record.Phase != Ready {
		return nil, nil, errs.New(errs.KindValidationFailed, "edge diagnostic publication must be fresh")
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
	mutations = append(
		mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   LatestKey(record.Source.Request.TargetID()),
			Value: []byte(record.Owner.TaskID),
		},
	)
	return conditions, mutations, nil
}

func (repository *Repository) Load(ctx context.Context, taskID string) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, invalidRecord()
	}
	return repository.loadAt(ctx, taskID, 0)
}

// LoadAt joins the native Task coordinator's fixed MVCC snapshot rather than
// reading a later result or cleanup owner while preparing its transaction.
func (repository *Repository) LoadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision <= 0 {
		return keyvalue.Versioned[Record]{}, invalidRecord()
	}
	return repository.loadAt(ctx, taskID, revision)
}

func (repository *Repository) loadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision != 0 && read.ReadRevision != revision ||
		len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "edge diagnostic record read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindRequestNotFound, "edge diagnostic was not found")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "edge diagnostic owner is corrupt")
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func (repository *Repository) Latest(ctx context.Context, targetID string) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindRoute, targetID) != nil && ids.Validate(ids.KindComponent, targetID) != nil {
		return keyvalue.Versioned[Record]{}, invalidRecord()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{LatestKey(targetID)}})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest edge diagnostic read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindRequestNotFound,
			"this Route or Tunnel has no retained connection check",
		)
	}
	taskID := string(read.Values[0].Value)
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest edge diagnostic index is corrupt")
	}
	current, err := repository.loadAt(ctx, taskID, read.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if current.Record.Source.Request.TargetID() != targetID {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"latest edge diagnostic changed resource ownership",
		)
	}
	return current, nil
}
