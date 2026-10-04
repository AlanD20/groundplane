package serviceobservation

import (
	"context"
	"errors"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureLogSource uses the same acknowledged runtime authority as health reads.
// A Service with no applied runtime has no log source; malformed runtime fails closed.
func CaptureLogSource(
	ctx context.Context,
	releases Releases,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (*agentpb.ServiceObservationTarget, error) {
	if captured, ok := capture(ctx, releases, service); ok {
		return captured.target, nil
	}
	if service.Record.BackingNetworkID != "" {
		_, found, err := releases.GetAppliedProjectionAt(ctx, service.Record.EnvironmentID, service.ReadRevision)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
	} else {
		_, err := releases.ResolveServing(ctx, service.Record.EnvironmentID, service.Record.Desired.ID, service.ReadRevision)
		if errors.Is(err, errs.New(errs.KindReleaseNotFound, "")) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, errs.New(errs.KindStorageUnavailable, "Service runtime could not be selected for logs")
}
