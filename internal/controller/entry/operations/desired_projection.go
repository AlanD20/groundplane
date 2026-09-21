package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/core"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func entryDesiredServiceIdentities(
	projection projectionrecord.EnvironmentComposeProjection,
) ([]composeidentity.Resource, error) {
	result := make([]composeidentity.Resource, len(projection.DesiredServices))
	seenIDs := make(map[string]string, len(projection.DesiredServices))
	seenNames := make(map[string]string, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID ||
			ids.Validate(ids.KindService, service.Desired.ID) != nil ||
			service.Desired.Name == "" {
			return nil, errs.New(errs.KindInternal, "Environment desired projection has an invalid Service identity")
		}
		if name, duplicate := seenIDs[service.Desired.ID]; duplicate && name != service.Desired.Name {
			return nil, errs.New(errs.KindInternal, "Environment desired projection repeats a Service id")
		}
		if serviceID, duplicate := seenNames[service.Desired.Name]; duplicate && serviceID != service.Desired.ID {
			return nil, errs.New(errs.KindInternal, "Environment desired projection repeats a Service name")
		}
		seenIDs[service.Desired.ID] = service.Desired.Name
		seenNames[service.Desired.Name] = service.Desired.ID
		result[index] = composeidentity.Resource{ID: service.Desired.ID, Name: service.Desired.Name}
	}
	return result, nil
}

func entryDesiredCandidateRecord(
	current projectionrecord.EnvironmentComposeProjection,
	request entryDesiredMutationRequest,
	revisionID string,
) (*entryrecord.Record, *entryrecord.Record, error) {
	desired := request.desired
	var previous *entryrecord.Record
	if request.action == entryDesiredMutationCreate {
		desired.ID = entryStableIDFromRevision(ids.KindEnvEntry, revisionID)
	} else {
		for index := range current.Entries {
			if current.Entries[index].Entry.ID == request.entryID {
				value := current.Entries[index]
				previous = &value
				break
			}
		}
		if previous == nil {
			return nil, nil, errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		if request.action == entryDesiredMutationRemove {
			return nil, previous, nil
		}
		desired.ID = previous.Entry.ID
	}
	persisted := desired
	if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
		persisted.Source.Literal = ""
	}
	record, err := entryrecord.NewRecord(
		current.EnvironmentID, persisted, entryStableIDFromRevision(ids.KindConfig, revisionID),
	)
	if err != nil {
		return nil, nil, err
	}
	if previous != nil {
		record.BlueprintKey = previous.BlueprintKey
	}
	return &record, previous, nil
}

func replaceProjectedEntry(
	current []entryrecord.Record,
	previous *entryrecord.Record,
	next *entryrecord.Record,
) []entryrecord.Record {
	result := make([]entryrecord.Record, 0, len(current)+1)
	for _, record := range current {
		if previous == nil || record.Entry.ID != previous.Entry.ID {
			result = append(result, record)
		}
	}
	if next != nil {
		result = append(result, *next)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Entry.ID < result[right].Entry.ID })
	return result
}

func (service *entryDesiredMutationService) resolveCurrentEntry(
	ctx context.Context,
	entryID string,
) (string, entryrecord.Record, error) {
	environmentID, found, err := service.repository.ResolveBlueprintEntryEnvironment(ctx, entryID)
	if err != nil {
		return "", entryrecord.Record{}, err
	}
	if !found {
		return "", entryrecord.Record{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return "", entryrecord.Record{}, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			if record.Entry.ID == entryID {
				return environmentID, record, nil
			}
		}
	}
	return "", entryrecord.Record{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
}

func (service *entryDesiredMutationService) validateExposure(
	ctx context.Context,
	environmentID string,
	exposure []string,
) error {
	if len(exposure) == 1 && exposure[0] == "all" {
		return nil
	}
	missing := make(map[string]struct{}, len(exposure))
	for _, name := range exposure {
		missing[name] = struct{}{}
	}
	cursor := ""
	for {
		page, err := service.repository.ListServices(ctx, environmentID, etcdstore.PageRequest{Limit: 200, Cursor: cursor})
		if err != nil {
			return err
		}
		for _, item := range page.Items {
			delete(missing, item.Record.Desired.Name)
		}
		if len(missing) == 0 {
			return nil
		}
		if page.NextCursor == "" {
			return errs.New(errs.KindServiceNotFound, "Entry exposure Service was not found")
		}
		cursor = page.NextCursor
	}
}

func entryStableIDFromRevision(kind ids.Kind, revisionID string) string {
	if ids.Validate(ids.KindTask, revisionID) != nil {
		return ""
	}
	return string(kind) + "_" + revisionID[len("task_"):]
}
