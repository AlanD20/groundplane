package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The effective Compose projection uses only the Attach records selected by
// this authored revision. Their exact revisions are fenced at seal, so a
// concurrent Detach or changed ready record cannot change network authority.
func (service *Service) loadAuthoredRuntimeAttaches(
	ctx context.Context,
	environmentID string,
	identities projectionrecord.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	selected := make(map[string]projectionrecord.OwnedIdentity, len(identities.Attaches))
	for _, identity := range identities.Attaches {
		selected[identity.ID] = identity
	}
	if len(selected) == 0 {
		return nil, nil
	}
	applied := make(map[string]blueprintunits.AppliedRecord, len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		if versioned.Record.Target.Kind == ids.KindAttach {
			applied[versioned.Record.Target.ID] = versioned.Record
		}
	}
	result := make([]etcdstore.Versioned[attachrecord.Record], 0, len(selected))
	cursor := ""
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, etcdstore.PageRequest{
			Limit: etcdstore.MaximumPageLimit, Cursor: cursor, Revision: snapshot.ReadRevision,
		})
		if err != nil {
			return nil, err
		}
		if page.Revision != snapshot.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach read revision changed")
		}
		for _, versioned := range page.Items {
			identity, owned := selected[versioned.Record.ID]
			if !owned {
				continue
			}
			receipt, acknowledged := applied[versioned.Record.ID]
			if versioned.Record.EnvironmentID != environmentID ||
				versioned.Record.Name != identity.Name ||
				versioned.Record.Status != core.AttachReady ||
				versioned.Record.Operation != attachrecord.AttachOperationProvision ||
				!acknowledged || receipt.State != blueprintunits.Applied ||
				receipt.SourceTaskID != versioned.Record.TaskID ||
				versioned.Revision <= 0 {
				return nil, errs.New(errs.KindStateConflict, "Blueprint applied Attach topology changed")
			}
			result = append(result, versioned)
			delete(selected, versioned.Record.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(selected) != 0 {
		return nil, errs.New(errs.KindStateConflict, "Blueprint applied Attach is missing")
	}
	return result, nil
}
