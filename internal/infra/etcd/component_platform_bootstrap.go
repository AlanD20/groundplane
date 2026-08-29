package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	platformCoreDNSComponentID    = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

// DefaultPlatformComponents returns the stable platform singleton catalog.
func DefaultPlatformComponents(tailnetDelegation bool) ([]ComponentRecord, error) {
	components := []core.Component{
		{
			ID: platformCoreDNSComponentID,
			Owner: core.ComponentOwnerPlatform,
			Kind: core.ComponentKindCoreDNS,
			Enabled: true,
			Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
				UpstreamAuto: true,
				TailnetDelegation: tailnetDelegation,
			}},
		},
	}
	records := make([]ComponentRecord, len(components))
	for index, component := range components {
		record, err := NewComponentRecord(component)
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
	records []ComponentRecord,
) ([]Versioned[ComponentRecord], error) {
	result := make([]Versioned[ComponentRecord], 0, len(records))
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
		created, err := repository.CreatePlatformComponent(ctx, record)
		if err != nil {
			return nil, err
		}
		result = append(result, created)
	}
	return result, nil
}
