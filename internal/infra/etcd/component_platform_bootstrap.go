package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	platformCoreDNSComponentID       = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	platformComponentBootstrapPrefix = "/v1/records/platform-component-bootstrap/"
)

func platformComponentBootstrapKey(componentID string) string {
	return platformComponentBootstrapPrefix + componentID
}

// DefaultPlatformComponents returns the stable platform singleton catalog.
func DefaultPlatformComponents(tailnetDelegation bool) ([]componentrecord.Record, error) {
	components := []core.Component{
		{
			ID:      platformCoreDNSComponentID,
			Owner:   core.ComponentOwnerPlatform,
			Kind:    core.ComponentKindCoreDNS,
			Enabled: true,
			Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
				CorefileTemplate:  ". {\n    {groundplane}\n    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n",
				UpstreamAuto:      true,
				TailnetDelegation: tailnetDelegation,
			}},
		},
	}
	records := make([]componentrecord.Record, len(components))
	for index, component := range components {
		record, err := componentrecord.NewRecord(component)
		if err != nil {
			return nil, err
		}
		records[index] = record
	}
	return records, nil
}

// EnsurePlatformComponents creates missing singleton records without replacing
// existing desired configuration or runtime state.
func (repository *ComponentRepository) EnsurePlatformComponents(
	ctx context.Context,
	records []componentrecord.Record,
) ([]etcdstore.Versioned[componentrecord.Record], error) {
	result := make([]etcdstore.Versioned[componentrecord.Record], 0, len(records))
	for _, record := range records {
		if err := validatePlatformComponentRecord(record); err != nil {
			return nil, err
		}
		current, err := repository.GetComponent(ctx, record.Desired.ID)
		if err == nil {
			if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
				current.Record.Desired.Kind != record.Desired.Kind {
				return nil, errs.New(errs.KindStateConflict, "platform Component identity is already in use")
			}
			result = append(result, current)
			continue
		}
		if kind, ok := errs.KindOf(err); !ok || kind != errs.KindComponentNotFound {
			return nil, err
		}
		created, err := repository.createPlatformComponent(ctx, record, true)
		if err != nil {
			return nil, err
		}
		result = append(result, created)
	}
	return result, nil
}

// HasPlatformComponentBootstrapProvenance proves that the current record is
// still the exact singleton created by the bootstrap repository transaction.
func (repository *ComponentRepository) HasPlatformComponentBootstrapProvenance(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
) (bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return false, err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return false, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return false, errs.New(errs.KindValidationFailed, "platform Component version metadata is invalid")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{platformComponentBootstrapKey(current.Record.Desired.ID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if state == nil || len(state.Values) != 1 || state.ReadRevision != current.ReadRevision {
		return false, errs.New(errs.KindInternal, "platform Component bootstrap provenance read is invalid")
	}
	if state.Values[0] == nil {
		return false, nil
	}
	if state.Values[0].ModRevision != current.Revision ||
		string(state.Values[0].Value) != current.Record.Desired.ID {
		return false, errs.New(errs.KindStateConflict, "platform Component bootstrap provenance is stale")
	}
	return true, nil
}
