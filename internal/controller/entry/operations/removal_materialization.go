package operations

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func PlanEntryRemovals(
	environmentID string,
	removed []entryrecord.Record,
	next []entryrecord.Record,
	services []composeidentity.Resource,
) ([]taskplanning.EnvironmentEntryMaterialization, error) {
	nextFiles := make(map[string]struct{})
	nextScopes := make(map[string]struct{})
	for _, record := range next {
		if record.Entry.Kind == core.EntryKindFile {
			nextFiles[record.Entry.Path] = struct{}{}
			continue
		}
		if record.Entry.Kind == core.EntryKindEnv && !record.Entry.ExposesAll() {
			for _, scope := range record.Entry.Exposure {
				nextScopes[scope] = struct{}{}
			}
		}
	}
	serviceByName := make(map[string]composeidentity.Resource, len(services))
	for _, service := range services {
		serviceByName[service.Name] = service
	}
	seen := make(map[string]struct{})
	result := make([]taskplanning.EnvironmentEntryMaterialization, 0, len(removed))
	for _, record := range removed {
		entry := record.Entry
		if entry.Kind == core.EntryKindFile {
			if _, retained := nextFiles[entry.Path]; retained {
				continue
			}
			if entry.UID == nil || entry.GID == nil {
				return nil, errs.New(errs.KindInternal, "removed Blueprint file Entry ownership is missing")
			}
			output := etcd.TaskMaterializationOutputRemovePlainFile
			mode := entrymaterialization.ModeReadOnly
			if entry.Secret {
				output = etcd.TaskMaterializationOutputRemoveSecretFile
				mode = entrymaterialization.ModePrivate
			}
			if _, duplicate := seen[entry.Path]; !duplicate {
				seen[entry.Path] = struct{}{}
				result = append(result, taskplanning.EnvironmentEntryMaterialization{
					Destination: entry.Path, OutputKind: output, UID: *entry.UID, GID: *entry.GID, Mode: mode,
					Source: etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
				})
			}
			continue
		}
		if entry.Kind != core.EntryKindEnv || entry.ExposesAll() {
			continue
		}
		for _, scope := range entry.Exposure {
			if _, retained := nextScopes[scope]; retained {
				continue
			}
			destination := environmentfile.ServiceEnvFileName(environmentID, scope)
			if _, duplicate := seen[destination]; duplicate {
				continue
			}
			identity, found := serviceByName[scope]
			if !found {
				return nil, errs.New(errs.KindInternal, "removed Blueprint Entry exposure Service is missing")
			}
			seen[destination] = struct{}{}
			result = append(result, taskplanning.EnvironmentEntryMaterialization{
				Destination: destination, ServiceID: identity.ID, ServiceName: identity.Name,
				OutputKind: etcd.TaskMaterializationOutputRemoveGeneratedEnv,
				Mode:       entrymaterialization.ModePrivate,
				Source:     etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
			})
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Destination < result[right].Destination })
	return result, nil
}
