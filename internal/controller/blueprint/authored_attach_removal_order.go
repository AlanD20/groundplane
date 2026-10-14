package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Credential reuse and grants must retire before the referenced owner. Capture
// this ordering in the durable plan; child publication still fences topology.
func (service *Service) orderAuthoredAttachRemovals(
	ctx context.Context,
	environmentID string,
	units []blueprintunits.Unit,
	revision int64,
) ([]blueprintunits.Unit, error) {
	removals := make(map[string]int)
	for index, unit := range units {
		if unit.Removal && unit.Target.Kind == ids.KindAttach {
			removals[unit.Target.ID] = index
		}
	}
	if len(removals) == 0 {
		return units, nil
	}
	cursor := ""
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, keyvalue.PageRequest{
			Limit: keyvalue.MaximumPageLimit, Cursor: cursor, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page.Revision != revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal topology changed")
		}
		for _, source := range page.Items {
			record := source.Record
			references := append([]string(nil), record.GrantAttachIDs...)
			if !record.OwnsCredential() {
				references = append(references, record.CredentialAttachID)
			}
			for _, referencedID := range references {
				index, removing := removals[referencedID]
				if !removing {
					continue
				}
				if _, removing := removals[record.ID]; !removing {
					return nil, errs.New(errs.KindResourceInUse,
						"Blueprint omits an Attach still used by a retained credential or grant binding")
				}
				units[index].After = canonicalAuthoredUnitKeys(append(units[index].After,
					blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: record.ID}))
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return units, nil
}
