package blueprint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"sort"
	"time"
)

func componentTaskPreparationIsZeroForBlueprint(preparation etcd.ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == ""
}

func (service *Service) prepareBlueprintComponents(
	ctx context.Context,
	environmentID string,
	taskID string,
	createdAt time.Time,
	allocate func(ids.Kind) string,
	specs map[string]core.ComponentSpec,
	current []etcd.Versioned[etcd.ComponentRecord],
	zoneChanges []etcd.EnvironmentBlueprintZoneChange,
) (etcd.ComponentTaskPreparation, []etcd.ComponentRecord, []core.Component, error) {
	currentComponents := make([]core.Component, len(current))
	currentByID := make(map[string]etcd.Versioned[etcd.ComponentRecord], len(current))
	for index, versioned := range current {
		component, err := etcd.ProjectComponentRecord(versioned.Record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
		if _, duplicate := currentByID[component.ID]; duplicate {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Environment Component singleton set repeats an id",
			)
		}
		currentComponents[index] = component
		currentByID[component.ID] = versioned
	}
	changes, err := controller.ReconcileBlueprintComponents(specs, currentComponents, allocate)
	if err != nil {
		return etcd.ComponentTaskPreparation{}, nil, nil, err
	}
	inputs := make([]etcd.EnvironmentComponentCandidateInput, len(changes.Candidates))
	for index, candidate := range changes.Candidates {
		versioned, exists := currentByID[candidate.Current.ID]
		if !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Blueprint Component candidate lost its active record",
			)
		}
		inputs[index] = etcd.EnvironmentComponentCandidateInput{
			Current: versioned, Candidate: candidate.Candidate,
		}
	}
	preparation := etcd.ComponentTaskPreparation{}
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
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	recordsByID := make(map[string]etcd.ComponentRecord, len(changes.Effective))
	for _, component := range changes.Effective {
		record, recordErr := etcd.NewComponentRecord(component)
		if recordErr != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, recordErr
		}
		recordsByID[component.ID] = record
	}
	if len(preparation.Intent.Candidates) != len(changes.Candidates) {
		return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
			errs.KindInternal,
			"prepared Blueprint Component candidate count changed",
		)
	}
	for _, candidate := range preparation.Intent.Candidates {
		if _, exists := recordsByID[candidate.Candidate.Desired.ID]; !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"prepared Blueprint Component candidate is unknown",
			)
		}
		recordsByID[candidate.Candidate.Desired.ID] = candidate.Candidate
	}
	records := make([]etcd.ComponentRecord, 0, len(recordsByID))
	for _, record := range recordsByID {
		records = append(records, record)
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Desired.Kind < records[right].Desired.Kind
	})
	effective := make([]core.Component, len(records))
	for index, record := range records {
		effective[index], err = etcd.ProjectComponentRecord(record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
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
	authored controller.ComposeIdentitySnapshot,
	generated []controller.ComposeResourceIdentity,
) controller.ComposeIdentitySnapshot {
	result := controller.ComposeIdentitySnapshot{
		Services: append([]controller.ComposeResourceIdentity(nil), authored.Services...),
		Networks: append([]controller.ComposeResourceIdentity(nil), authored.Networks...),
		Volumes:  append([]controller.ComposeResourceIdentity(nil), authored.Volumes...),
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
	outputKind  etcd.TaskMaterializationOutputKind
	uid         uint32
	gid         uint32
	mode        entrymaterialization.Mode
	source      etcd.TaskMaterializationSource
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
	projection controller.EnvironmentComponentComposeProjection,
	entryMaterializations []controller.EnvironmentEntryMaterialization,
	entries []entryrecord.Record,
) ([]etcd.TaskMaterializationRecord, []*agentpb.ExecutionStep, error) {
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
			destination: file.Path, outputKind: etcd.TaskMaterializationOutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceBlueprintFile,
				BlueprintFile: &etcd.TaskBlueprintFileValueReference{
					RevisionID: revisionID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.PlainFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: etcd.TaskMaterializationOutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceComponentFile,
				ComponentFile: &etcd.TaskComponentFileValueReference{
					RevisionID: revisionID, ComponentID: file.ComponentID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.EnvironmentFiles {
		values := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(file.Values))
		for index, binding := range file.Values {
			secret, err := service.materials.PinSecretValue(ctx, projectID, binding.SecretID)
			if err != nil {
				return nil, nil, err
			}
			values[index] = etcd.TaskGeneratedEnvironmentEntryReference{
				Name: binding.Name, Secret: &secret,
			}
		}
		sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Destination, serviceID: file.ServiceID, serviceName: file.ServiceName,
			outputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
			mode:       entrymaterialization.ModePrivate, resolve: true,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
				GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
					FormatVersion: 1, Values: values,
				},
			},
		})
	}
	sort.Slice(inputs, func(left int, right int) bool { return inputs[left].destination < inputs[right].destination })
	references := make([]etcd.TaskMaterializationRecord, 0, len(inputs))
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
		reference := etcd.TaskMaterializationRecord{
			StepID:            allocate(ids.KindStep, "materialization-step/"+input.destination),
			MaterializationID: allocate(ids.KindConfig, "materialization/"+input.destination),
			EnvironmentID:     environmentID, Destination: input.destination,
			ServiceID: input.serviceID, ServiceName: input.serviceName,
			OutputKind: input.outputKind, UID: input.uid, GID: input.gid, Mode: uint32(input.mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.source,
		}
		if input.source.Kind == etcd.TaskMaterializationSourceComponentFile {
			err = service.materials.RetainComponentFile(ctx, reference, generation, content)
		}
		clear(content)
		if err != nil {
			return nil, nil, err
		}
		step, err := controller.BuildTaskMaterializationStep(
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
