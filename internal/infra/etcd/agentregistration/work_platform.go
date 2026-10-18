package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecordWorkPlatform accepts only the authenticated current host process. The
// immutable record survives process replacement without borrowing Controller OS.
func (repository *Repository) RecordWorkPlatform(ctx context.Context, authority WorkAuthorization, operatingSystem, architecture, variant string) error {
	guards, err := authority.Conditions()
	if err != nil {
		return err
	}
	platform := hostbindings.Platform{HostID: authority.Identity().HostID, OS: operatingSystem, Architecture: architecture, Variant: variant}
	value, err := hostbindings.EncodePlatform(platform)
	if err != nil {
		return err
	}
	defer clear(value)
	previous, err := hostbindings.ReadPlatform(ctx, repository.store, platform.HostID, 0)
	if err != nil {
		return err
	}
	if previous.Revision > 0 {
		if previous.Record != platform {
			return errs.New(errs.KindStateConflict, "Agent platform differs from its enrolled host")
		}
		return nil
	}
	key := hostbindings.PlatformKey(platform.HostID)
	result, err := repository.store.Transact(ctx, append(guards, keyvalue.Condition{Key: key}),
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "host ownership changed before platform registration; reconnect")
	}
	return nil
}
