package remotehosts

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *Repository) ListHosts(
	ctx context.Context,
	request keyvalue.PageRequest,
) (keyvalue.Page[remotehost.Host], error) {
	reader, ok := repository.store.(interface {
		Store
		Get(context.Context, string) (*keyvalue.GetResult, error)
		Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	})
	if !ok {
		return keyvalue.Page[remotehost.Host]{}, recordcodec.CorruptRecord()
	}
	return recordquery.ListPrimary(
		ctx, reader, "remote-hosts", "platform", "-", Key(""), ids.KindHost, request,
		func(body []byte) (remotehost.Host, error) {
			host, err := recordcodec.Decode[remotehost.Host](body, recordKind)
			if err == nil {
				err = host.Validate()
			}
			return host, err
		},
		func(host remotehost.Host) string { return host.ID },
		func(remotehost.Host) bool { return true },
	)
}

// PublishOperatorChange atomically records an exact enrollment transition and
// the replay receipt. Detaching delegates to the ordinary executor fences.
func (repository *Repository) PublishOperatorChange(
	ctx context.Context, previous remotehost.VersionedHost, next remotehost.Host, publication workqueue.Change,
) (bool, error) {
	defer keyvalue.ClearMutationValues(publication.Mutations)
	if previous.Revision == 0 {
		if next.Generation != 0 || next.Agent != nil || next.Pending != nil {
			return false, invalidTransition()
		}
		return repository.write(ctx, 0, next, publication)
	}
	if previous.Host.ID != next.ID || previous.Host.Name != next.Name {
		return false, invalidTransition()
	}
	if previous.Host.Agent != nil && next.Agent == nil {
		return repository.detachAgent(
			ctx,
			previous.Revision,
			previous.Host.Identity(previous.Host.Agent.AgentID),
			next,
			publication,
		)
	}
	// The offline CSR ceremony stages a signed certificate in the same write as
	// allocating the next Agent generation. It cannot activate that generation.
	initial := previous.Host.Agent == nil && previous.Host.Generation < math.MaxUint64 &&
		next.Generation == previous.Host.Generation+1 && next.Agent == nil && next.Pending != nil &&
		next.Pending.Certificate != nil
	noChange := previous.Host.Generation == next.Generation && previous.Host.Pending == nil &&
		previous.Host.Agent == nil && next.Pending == nil && next.Agent == nil
	if !initial && !noChange && !credentialUpdateAllowed(previous.Host, next) {
		return false, invalidTransition()
	}
	return repository.write(ctx, previous.Revision, next, publication)
}
