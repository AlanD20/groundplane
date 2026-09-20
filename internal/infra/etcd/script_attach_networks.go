package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ScriptAttachSources struct {
	Networks []Versioned[ZoneRecord]
	Heads    []Versioned[EnvironmentBlueprintHead]
	Attaches []Versioned[AttachRecord]
}

func loadEnvironmentAttachesAtRevision(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	revision int64,
) ([]Versioned[AttachRecord], error) {
	var result []Versioned[AttachRecord]
	request := PageRequest{Limit: 200}
	for {
		page, err := listIndexPageAtRevision(ctx, store, "attaches", "environment", environmentID,
			attachOwnerPrefix(environmentID), attachKey, ids.KindAttach, request, decodeAttachRecord,
			func(record AttachRecord) string { return record.ID },
			func(record AttachRecord) bool { return record.EnvironmentID == environmentID }, revision)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.NextCursor == "" {
			return result, nil
		}
		request.Cursor = page.NextCursor
	}
}

// Bind only the intended consumer's Attach networks. Backing Zones are existing
// resources in another Environment, never staged candidate-owned Zones.
func resolveScriptAttachNetworks(
	ctx context.Context,
	store hierarchyStore,
	environmentID, serviceID string,
	attaches []Versioned[AttachRecord],
	revision int64,
) (ScriptAttachSources, error) {
	result := ScriptAttachSources{}
	resolved := make(map[string]Versioned[ZoneRecord])
	heads := make(map[string]Versioned[EnvironmentBlueprintHead])
	for _, value := range attaches {
		attach := value.Record
		if attach.EnvironmentID != environmentID {
			return ScriptAttachSources{}, errs.New(
				errs.KindStateConflict,
				"Script Attach belongs to another Environment",
			)
		}
		if attach.ServiceID != serviceID {
			continue
		}
		if validateAttachRecord(attach) != nil || attach.Operation == AttachOperationDetach {
			return ScriptAttachSources{}, errs.New(errs.KindStateConflict, "Script Attach network authority is invalid")
		}
		result.Attaches = append(result.Attaches, value)
		if previous, found := resolved[attach.BackingNetworkID]; found {
			if previous.Record.EnvironmentID != attach.BackingEnvironmentID {
				return ScriptAttachSources{}, errs.New(errs.KindStateConflict, "Script Attach network owner changed")
			}
			continue
		}
		head, projection, err := loadScriptExecutionDesiredProjection(
			ctx,
			store,
			attach.BackingEnvironmentID,
			"",
			revision,
		)
		if err != nil {
			return ScriptAttachSources{}, err
		}
		if projection.ReadRevision != revision {
			return ScriptAttachSources{}, errs.New(
				errs.KindStateConflict,
				"Script backing Environment source is missing",
			)
		}
		heads[attach.BackingEnvironmentID] = head
		for _, zone := range projection.Record.DesiredZones {
			if zone.Desired.ID != attach.BackingNetworkID {
				continue
			}
			network, err := joinEnvironmentZone(projection, zone)
			if err != nil {
				return ScriptAttachSources{}, err
			}
			resolved[attach.BackingNetworkID] = network
		}
		if _, found := resolved[attach.BackingNetworkID]; !found {
			return ScriptAttachSources{}, errs.New(errs.KindStateConflict, "Script Attach backing network is missing")
		}
	}
	for _, network := range resolved {
		result.Networks = append(result.Networks, network)
	}
	for _, head := range heads {
		result.Heads = append(result.Heads, head)
	}
	sort.Slice(result.Networks, func(i, j int) bool {
		return result.Networks[i].Record.Desired.ID < result.Networks[j].Record.Desired.ID
	})
	sort.Slice(result.Heads, func(i, j int) bool {
		return result.Heads[i].Record.EnvironmentID < result.Heads[j].Record.EnvironmentID
	})
	return result, nil
}

func scriptAttachSourceConditions(sources ScriptAttachSources) []etcdstore.Condition {
	byKey := make(map[string]etcdstore.Condition)
	for _, attach := range sources.Attaches {
		key := attachKey(attach.Record.ID)
		byKey[key] = etcdstore.Condition{Key: key, ModRevision: attach.Revision}
	}
	for _, head := range sources.Heads {
		key := environmentBlueprintHeadKey(head.Record.EnvironmentID)
		byKey[key] = etcdstore.Condition{Key: key, ModRevision: head.Revision}
		for _, network := range sources.Networks {
			if network.Record.EnvironmentID != head.Record.EnvironmentID {
				continue
			}
			root := environmentBlueprintRootKey(head.Record.EnvironmentID, head.Record.RevisionID)
			byKey[root] = etcdstore.Condition{Key: root, ModRevision: network.Revision}
			deleted := deletionTombstoneKey(string(DeletionTargetZone), network.Record.Desired.ID)
			byKey[deleted] = etcdstore.Condition{Key: deleted}
		}
	}
	result := make([]etcdstore.Condition, 0, len(byKey))
	for _, condition := range byKey {
		result = append(result, condition)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}
