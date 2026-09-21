package blueprint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"sort"
	"time"
)

func componentTaskPreparationIsZeroForBlueprint(preparation componentplanning.ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == ""
}

func (service *Service) prepareBlueprintComponents(
	ctx context.Context,
	environmentID string,
	taskID string,
	createdAt time.Time,
	allocate func(ids.Kind) string,
	specs map[string]core.ComponentSpec,
	current []etcdstore.Versioned[componentrecord.Record],
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
) (componentplanning.ComponentTaskPreparation, []componentrecord.Record, []core.Component, error) {
	currentComponents := make([]core.Component, len(current))
	currentByID := make(map[string]etcdstore.Versioned[componentrecord.Record], len(current))
	for index, versioned := range current {
		component, err := componentrecord.ProjectRecord(versioned.Record)
		if err != nil {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, err
		}
		if _, duplicate := currentByID[component.ID]; duplicate {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Environment Component singleton set repeats an id",
			)
		}
		currentComponents[index] = component
		currentByID[component.ID] = versioned
	}
	changes, err := taskplanning.ReconcileBlueprintComponents(specs, currentComponents, allocate)
	if err != nil {
		return componentplanning.ComponentTaskPreparation{}, nil, nil, err
	}
	inputs := make([]componentplanning.EnvironmentComponentCandidateInput, len(changes.Candidates))
	for index, candidate := range changes.Candidates {
		versioned, exists := currentByID[candidate.Current.ID]
		if !exists {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Blueprint Component candidate lost its active record",
			)
		}
		inputs[index] = componentplanning.EnvironmentComponentCandidateInput{
			Current: versioned, Candidate: candidate.Candidate,
		}
	}
	preparation := componentplanning.ComponentTaskPreparation{}
	if len(inputs) != 0 {
		preparation, err = service.repository.PrepareEnvironmentComponentTask(
			ctx,
			taskID,
			environmentID,
			zoneChanges,
			inputs,
			createdAt,
		)
		if err != nil {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	recordsByID := make(map[string]componentrecord.Record, len(changes.Effective))
	for _, component := range changes.Effective {
		record, recordErr := componentrecord.NewRecord(component)
		if recordErr != nil {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, recordErr
		}
		recordsByID[component.ID] = record
	}
	if len(preparation.Intent.Candidates) != len(changes.Candidates) {
		return componentplanning.ComponentTaskPreparation{}, nil, nil, errs.New(
			errs.KindInternal,
			"prepared Blueprint Component candidate count changed",
		)
	}
	for _, candidate := range preparation.Intent.Candidates {
		if _, exists := recordsByID[candidate.Candidate.Desired.ID]; !exists {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"prepared Blueprint Component candidate is unknown",
			)
		}
		recordsByID[candidate.Candidate.Desired.ID] = candidate.Candidate
	}
	records := make([]componentrecord.Record, 0, len(recordsByID))
	for _, record := range recordsByID {
		records = append(records, record)
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Desired.Kind < records[right].Desired.Kind
	})
	effective := make([]core.Component, len(records))
	for index, record := range records {
		effective[index], err = componentrecord.ProjectRecord(record)
		if err != nil {
			return componentplanning.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	return preparation, records, effective, nil
}

func blueprintComponentEnvironment(
	environment hierarchyrecord.EnvironmentRecord,
	zones []core.Zone,
	services []core.Service,
	routes []core.Route,
	components []core.Component,
	entries []entryrecord.Record,
) core.Environment {
	projected := core.Environment{
		ID: environment.ID, ProjectID: environment.ProjectID, Name: environment.Name,
		NetworkPool: environment.NetworkPool, VolumeDir: environment.VolumeDir,
		Zones: make(map[string]core.Zone, len(zones)), Services: make(map[string]core.Service, len(services)),
		Routes: append([]core.Route(nil), routes...), Components: append([]core.Component(nil), components...),
		Entries: projectedEnvironmentEntries(entries),
	}
	for _, zone := range zones {
		projected.Zones[zone.Name] = zone
	}
	for _, service := range services {
		projected.Services[service.Name] = service
	}
	return projected
}

func projectedEnvironmentEntries(records []entryrecord.Record) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}

func environmentComponentComposeIdentities(
	authored composeidentity.Snapshot,
	generated []composeidentity.Resource,
) composeidentity.Snapshot {
	result := composeidentity.Snapshot{
		Services: append([]composeidentity.Resource(nil), authored.Services...),
		Networks: append([]composeidentity.Resource(nil), authored.Networks...),
		Volumes:  append([]composeidentity.Resource(nil), authored.Volumes...),
	}
	result.Services = append(result.Services, generated...)
	sort.Slice(result.Services, func(left int, right int) bool {
		return result.Services[left].Name < result.Services[right].Name
	})
	return result
}

type environmentComponentMaterializationInput struct {
	destination string
	serviceID   string
	serviceName string
	outputKind  materializationrecord.OutputKind
	uid         uint32
	gid         uint32
	mode        entrymaterialization.Mode
	source      materializationrecord.Source
	content     []byte
	resolve     bool
}

func (service *Service) environmentComponentMaterializations(
	ctx context.Context,
	environmentID string,
	projectID string,
	revisionID string,
	artifactID string,
	generation uint64,
	allocate func(ids.Kind, string) string,
	runtimeFiles []core.BlueprintFile,
	projection composerender.EnvironmentComponentComposeProjection,
	entryMaterializations []composerender.EnvironmentEntryMaterialization,
	entries []entryrecord.Record,
) ([]materializationrecord.Record, []*agentpb.ExecutionStep, error) {
	inputs := make([]environmentComponentMaterializationInput, 0,
		len(runtimeFiles)+len(projection.PlainFiles)+len(projection.EnvironmentFiles)+len(entryMaterializations))
	for _, materialization := range entryMaterializations {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: materialization.Destination,
			serviceID:   materialization.ServiceID,
			serviceName: materialization.ServiceName,
			outputKind:  materialization.OutputKind,
			uid:         materialization.UID,
			gid:         materialization.GID,
			mode:        materialization.Mode,
			source:      materialization.Source,
			resolve:     true,
		})
	}
	for _, file := range runtimeFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: materializationrecord.OutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: materializationrecord.Source{
				Kind: materializationrecord.SourceBlueprintFile,
				BlueprintFile: &materializationrecord.BlueprintFileValueReference{
					RevisionID: revisionID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.PlainFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: materializationrecord.OutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: materializationrecord.Source{
				Kind: materializationrecord.SourceComponentFile,
				ComponentFile: &materializationrecord.ComponentFileValueReference{
					RevisionID: revisionID, ComponentID: file.ComponentID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.EnvironmentFiles {
		values := make([]materializationrecord.GeneratedEnvironmentEntryReference, len(file.Values))
		for index, binding := range file.Values {
			secret, err := service.materials.PinSecretValue(ctx, projectID, binding.SecretID)
			if err != nil {
				return nil, nil, err
			}
			values[index] = materializationrecord.GeneratedEnvironmentEntryReference{
				Name: binding.Name, Secret: &secret,
			}
		}
		sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Destination, serviceID: file.ServiceID, serviceName: file.ServiceName,
			outputKind: materializationrecord.OutputGeneratedEnvironment,
			mode:       entrymaterialization.ModePrivate, resolve: true,
			source: materializationrecord.Source{
				Kind: materializationrecord.SourceGeneratedEnvironment,
				GeneratedEnvironment: &materializationrecord.GeneratedEnvironmentValueReference{
					FormatVersion: 1, Values: values,
				},
			},
		})
	}
	sort.Slice(inputs, func(left int, right int) bool { return inputs[left].destination < inputs[right].destination })
	references := make([]materializationrecord.Record, 0, len(inputs))
	steps := make([]*agentpb.ExecutionStep, 0, len(inputs))
	previousDestination := ""
	for _, input := range inputs {
		if input.destination == previousDestination {
			return nil, nil, errs.New(errs.KindNameConflict, "Environment materialization destination is duplicated")
		}
		content := input.content
		var err error
		if input.resolve {
			content, err = service.materials.ResolveTaskMaterializationSource(ctx, environmentID, input.source)
			if err != nil {
				clear(content)
				return nil, nil, err
			}
		}
		digest := sha256.Sum256(content)
		reference := materializationrecord.Record{
			StepID:            allocate(ids.KindStep, "materialization-step/"+input.destination),
			MaterializationID: allocate(ids.KindConfig, "materialization/"+input.destination),
			EnvironmentID:     environmentID, Destination: input.destination,
			ServiceID: input.serviceID, ServiceName: input.serviceName,
			OutputKind: input.outputKind, UID: input.uid, GID: input.gid, Mode: uint32(input.mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.source,
		}
		if input.source.Kind == materializationrecord.SourceComponentFile {
			err = service.materials.RetainComponentFile(ctx, reference, generation, content)
		}
		clear(content)
		if err != nil {
			return nil, nil, err
		}
		step, err := taskmaterialization.BuildTaskMaterializationStep(
			reference,
			artifactID,
			uint32(desiredrevision.TaskTimeoutSeconds),
		)
		if err != nil {
			return nil, nil, err
		}
		references = append(references, reference)
		steps = append(steps, step)
		previousDestination = input.destination
	}
	sort.Slice(references, func(left int, right int) bool {
		return references[left].StepID < references[right].StepID
	})
	return references, steps, nil
}
