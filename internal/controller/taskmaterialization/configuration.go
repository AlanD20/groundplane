package taskmaterialization

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type configurationSnapshotReader interface {
	LoadRetained(context.Context, runtimeconfiguration.Reference) (runtimeconfiguration.Snapshot, error)
}

func (resolver *TaskMaterializationResolver) EnableConfigurationRecovery(snapshots configurationSnapshotReader) error {
	if resolver == nil {
		return errs.New(errs.KindInternal, "configuration recovery sources are required")
	}
	sources, err := configurationrecovery.NewSources(snapshots)
	if err != nil {
		return err
	}
	resolver.configurationRecovery = sources
	return nil
}
