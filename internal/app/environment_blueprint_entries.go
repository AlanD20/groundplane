package app

import (
	"context"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *environmentBlueprintService) prepareBlueprintEntryValues(
	ctx context.Context,
	generationService *entrygeneration.EntryGenerationService,
	projectID string,
	environmentID string,
	reconciliation controller.BlueprintEntryReconciliation,
	createdAt time.Time,
) error {
	for _, candidate := range reconciliation.Values {
		found, err := service.repository.BlueprintEntryValueGenerationExists(ctx, candidate.Record)
		if err != nil {
			return err
		}
		if !found {
			generation, err := generationService.Generate(
				ctx, projectID, environmentID, candidate.Desired,
				candidate.Record.CurrentValueGenerationID, createdAt,
			)
			if err != nil {
				return err
			}
			createErr := service.repository.CreateBlueprintEntryValueGeneration(ctx, generation)
			entrygeneration.ClearEntryValueGeneration(&generation)
			if createErr != nil {
				return createErr
			}
		}
	}
	for _, record := range reconciliation.Current {
		if record.BlueprintKey == "" {
			continue
		}
		if err := service.repository.BindBlueprintEntryEnvironment(ctx, environmentID, record.Entry.ID); err != nil {
			return err
		}
	}
	return nil
}

func environmentBlueprintEntryRemovals(
	environmentID string,
	removed []etcd.EntryRecord,
	next []etcd.EntryRecord,
	services []controller.ComposeResourceIdentity,
) ([]controller.EnvironmentEntryMaterialization, error) {
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
	serviceByName := make(map[string]controller.ComposeResourceIdentity, len(services))
	for _, service := range services {
		serviceByName[service.Name] = service
	}
	seen := make(map[string]struct{})
	result := make([]controller.EnvironmentEntryMaterialization, 0, len(removed))
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
				result = append(result, controller.EnvironmentEntryMaterialization{
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
			destination := controller.ServiceEnvFileName(environmentID, scope)
			if _, duplicate := seen[destination]; duplicate {
				continue
			}
			identity, found := serviceByName[scope]
			if !found {
				return nil, errs.New(errs.KindInternal, "removed Blueprint Entry exposure Service is missing")
			}
			seen[destination] = struct{}{}
			result = append(result, controller.EnvironmentEntryMaterialization{
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
