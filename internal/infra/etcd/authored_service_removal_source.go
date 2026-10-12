package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AuthoredServiceRemovalSource captures actual applied state when available,
// otherwise the exact last declared definition. A direct Deploy can acknowledge
// runtime without applying that Environment projection. Its separate runtime
// capture remains mandatory; a declaration is never runtime or absence proof.
type AuthoredServiceRemovalSource struct {
	Projection                keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]
	Service                   keyvalue.Versioned[servicerecord.ServiceRecord]
	Kind                      environmentchanges.ServiceRemovalProjectionSource
	AppliedProjectionRevision int64
}

func (repository *ServiceRepository) GetAuthoredServiceRemovalSource(
	ctx context.Context, environmentID, declaredRevisionID, serviceID string,
) (AuthoredServiceRemovalSource, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, declaredRevisionID) != nil || ids.Validate(ids.KindService, serviceID) != nil {
		return AuthoredServiceRemovalSource{}, errs.New(
			errs.KindValidationFailed,
			"Service removal source identity is invalid",
		)
	}
	keys := []string{
		projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID),
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(environmentID, declaredRevisionID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return AuthoredServiceRemovalSource{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return AuthoredServiceRemovalSource{}, errs.New(errs.KindInternal, "Service removal source read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	source := AuthoredServiceRemovalSource{AppliedProjectionRevision: keyvalue.RevisionOf(read.Values[0])}
	index := 0
	if read.Values[0] != nil {
		source.Projection.Record, err = projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
		if err != nil || source.Projection.Record.EnvironmentID != environmentID {
			return AuthoredServiceRemovalSource{}, errs.New(errs.KindInternal, "Service applied projection is corrupt")
		}
	}
	if read.Values[0] != nil && projectionContainsService(source.Projection.Record, serviceID) {
		source.Kind = environmentchanges.ServiceRemovalAppliedProjection
	} else {
		if read.Values[1] == nil {
			return AuthoredServiceRemovalSource{}, errs.New(errs.KindStateConflict, "Service declared removal source is unavailable")
		}
		index = 1
		source.Kind = environmentchanges.ServiceRemovalDeclaredProjection
		source.Projection.Record, err = projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[1].Value)
		if err != nil || source.Projection.Record.EnvironmentID != environmentID ||
			source.Projection.Record.RevisionID != declaredRevisionID ||
			!projectionContainsService(source.Projection.Record, serviceID) {
			return AuthoredServiceRemovalSource{}, errs.New(errs.KindStateConflict, "Service retained declaration changed")
		}
	}
	source.Projection.Revision, source.Projection.ReadRevision = read.Values[index].ModRevision, read.ReadRevision
	source.Service, err = servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{
		Services: source.Projection.Record.DesiredServices, Revision: source.Projection.Revision, ReadRevision: read.ReadRevision,
	}, serviceID, keys[index])
	return source, err
}

func projectionContainsService(projection projectionrecord.EnvironmentComposeProjection, serviceID string) bool {
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == serviceID {
			return true
		}
	}
	return false
}
