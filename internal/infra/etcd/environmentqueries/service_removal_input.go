package environmentqueries

import (
	"context"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceReader) GetServiceRemovalRuntime(
	ctx context.Context, environmentID, serviceID string, revision int64,
) (etcdstore.Versioned[serviceruntimerecord.Record], error) {
	if ctx == nil || revision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return etcdstore.Versioned[serviceruntimerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Service removal runtime request is invalid",
		)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceruntimerecord.Key(serviceID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[serviceruntimerecord.Record]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != revision {
		return etcdstore.Versioned[serviceruntimerecord.Record]{}, errs.New(
			errs.KindInternal,
			"Service removal runtime read is incomplete",
		)
	}
	result := etcdstore.Versioned[serviceruntimerecord.Record]{ReadRevision: revision}
	if read.Values[0] == nil {
		return result, nil
	}
	defer etcdstore.ClearValues(read.Values)
	result.Record, err = releases.DecodeReleaseRecord[serviceruntimerecord.Record](
		read.Values[0].Value,
		"service-acknowledged-runtime",
	)
	if err != nil || result.Record.EnvironmentID != environmentID || result.Record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(result.Record) != nil {
		return etcdstore.Versioned[serviceruntimerecord.Record]{}, errs.New(
			errs.KindInternal,
			"Service removal runtime is corrupt",
		)
	}
	result.Revision = read.Values[0].ModRevision
	return result, nil
}

func (repository *ServiceReader) GetServiceRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.ServiceRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service removal Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.ServiceRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Service removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	intent, err := environmentchanges.DecodeServiceRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, environmentchanges.CorruptServiceRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
