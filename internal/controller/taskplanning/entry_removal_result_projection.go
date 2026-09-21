package taskplanning

import (
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func entryRemovalTaskPlan(task etcd.TaskRecord) (entrycapability.RemovalTaskPlan, error) {
	plan := entrycapability.RemovalTaskPlan{
		Executor: entrycapability.RemovalExecutorAgent, PlanHash: task.PlanHash,
		RenderGeneration:    task.RenderGeneration,
		EnvironmentID:       task.Params[taskjournal.TaskEntryEnvironmentParam],
		BlueprintRevisionID: task.Params[blueprints.EnvironmentDesiredRevisionParam],
		ArtifactID:          task.Params[taskjournal.TaskComposeArtifactParam], TimeoutSeconds: task.TimeoutSeconds,
		Identity: entrycapability.RemovalEnvironmentIdentity{
			TenantID: task.Owner.TenantID, TenantSlug: task.Params[taskjournal.TaskEntryTenantSlugParam],
			ProjectID: task.Owner.ProjectID, ProjectSlug: task.Params[taskjournal.TaskEntryProjectSlugParam],
			EnvironmentID:       task.Owner.EnvironmentID,
			EnvironmentName:     task.Params[taskjournal.TaskEntryEnvironmentNameParam],
			AuthorizedVolumeDir: task.Params[taskjournal.TaskEntryAuthorizedVolumeDirParam],
		},
		Steps:            make([]entrycapability.RemovalStep, len(task.Steps)),
		Materializations: make([]entrycapability.RemovalMaterialization, len(task.Materializations)),
	}
	for index, step := range task.Steps {
		plan.Steps[index] = entrycapability.RemovalStep{ID: step.ID}
	}
	for index, materialization := range task.Materializations {
		converted, err := entryRemovalMaterialization(materialization)
		if err != nil {
			return entrycapability.RemovalTaskPlan{}, err
		}
		plan.Materializations[index] = converted
	}
	return plan, nil
}

func entryRemovalMaterialization(
	input materializationrecord.Record,
) (entrycapability.RemovalMaterialization, error) {
	source := entrycapability.RemovalSource{Kind: entrycapability.RemovalSourceKind(input.Source.Kind)}
	if input.Source.Kind == materializationrecord.SourceGeneratedEnvironment {
		if input.Source.GeneratedEnvironment == nil {
			return entrycapability.RemovalMaterialization{}, errs.New(
				errs.KindInternal, "entry removal generated source is incomplete",
			)
		}
		generated := &entrycapability.RemovalGeneratedEnvironment{
			FormatVersion: input.Source.GeneratedEnvironment.FormatVersion,
			Values:        make([]entrycapability.RemovalGeneratedValue, len(input.Source.GeneratedEnvironment.Values)),
		}
		for index, value := range input.Source.GeneratedEnvironment.Values {
			storage, err := entryRemovalValueStorage(value.Value.Storage)
			if err != nil {
				return entrycapability.RemovalMaterialization{}, err
			}
			generated.Values[index] = entrycapability.RemovalGeneratedValue{
				Name: value.Name, EntryID: value.Value.EntryID,
				ValueGenerationID: value.Value.ValueGenerationID,
				Storage:           storage,
			}
		}
		source.GeneratedEnvironment = generated
	} else if input.Source.Kind != materializationrecord.SourceRemoval {
		return entrycapability.RemovalMaterialization{}, errs.New(
			errs.KindInternal, "entry removal source kind is invalid",
		)
	}
	outputKind, err := entryRemovalOutputKind(input.OutputKind)
	if err != nil {
		return entrycapability.RemovalMaterialization{}, err
	}
	return entrycapability.RemovalMaterialization{
		StepID: input.StepID, MaterializationID: input.MaterializationID,
		EnvironmentID: input.EnvironmentID, Destination: input.Destination,
		ServiceID: input.ServiceID, ServiceName: input.ServiceName,
		OutputKind: outputKind, UID: input.UID, GID: input.GID,
		Mode: input.Mode, Length: input.Length, SHA256: input.SHA256, Source: source,
	}, nil
}

func entryRemovalValueStorage(input materializationrecord.EntryValueStorage) (entrycapability.RemovalValueStorage, error) {
	switch input {
	case materializationrecord.EntryValueStoragePlain:
		return entrycapability.RemovalValueStoragePlain, nil
	case materializationrecord.EntryValueStorageSecret:
		return entrycapability.RemovalValueStorageSecret, nil
	default:
		return "", errs.New(errs.KindInternal, "durable Entry removal value storage is invalid")
	}
}

func entryRemovalOutputKind(
	input materializationrecord.OutputKind,
) (entrycapability.RemovalOutputKind, error) {
	switch input {
	case materializationrecord.OutputGeneratedEnvironment:
		return entrycapability.RemovalOutputGeneratedEnvironment, nil
	case materializationrecord.OutputPlainFile:
		return entrycapability.RemovalOutputPlainFile, nil
	case materializationrecord.OutputSecretFile:
		return entrycapability.RemovalOutputSecretFile, nil
	case materializationrecord.OutputRemoveGeneratedEnv:
		return entrycapability.RemovalOutputRemoveGeneratedEnv, nil
	case materializationrecord.OutputRemovePlainFile:
		return entrycapability.RemovalOutputRemovePlainFile, nil
	case materializationrecord.OutputRemoveSecretFile:
		return entrycapability.RemovalOutputRemoveSecretFile, nil
	default:
		return "", errs.New(errs.KindInternal, "durable Entry removal output kind is invalid")
	}
}
