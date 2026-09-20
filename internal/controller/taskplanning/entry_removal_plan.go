package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"math"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type entryRemovalPlanStateReader interface {
	GetEntryRemovalIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EntryRemovalIntent], bool, error)
}

// EntryRemovalMaterializationResolver resolves one closed durable source while
// the application prepares immutable length and digest metadata.
type EntryRemovalMaterializationResolver interface {
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		etcd.TaskMaterializationSource,
	) ([]byte, error)
}

type entryRemovalTaskProcedureIDs struct {
	ArtifactID string
}

func (planner *EntryRemovalPlanner) PrepareEntryRemoval(
	ctx context.Context,
	request entrycapability.RemovalPlanRequest,
) (entrycapability.RemovalTaskPlan, error) {
	if planner == nil || planner.plans == nil || planner.plans.blueprints == nil || planner.hierarchy == nil ||
		ids.Validate(ids.KindTask, request.TaskID) != nil || ids.Validate(ids.KindPlan, request.PlanID) != nil ||
		ids.Validate(ids.KindEnvEntry, request.EntryID) != nil ||
		ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, request.ArtifactID) != nil || request.EntryRevision <= 0 ||
		request.ProjectionRevision <= 0 || request.CreatedAt.IsZero() ||
		request.Identity.EnvironmentID != request.EnvironmentID ||
		request.Identity.ProjectID == "" || request.Identity.ProjectSlug == "" ||
		request.Identity.EnvironmentName == "" || request.Identity.AuthorizedVolumeDir == "" {
		return entrycapability.RemovalTaskPlan{}, errs.New(errs.KindInternal, "entry removal plan request is invalid")
	}
	projection, found, err := planner.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, request.EnvironmentID)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	if !found || projection.Revision != request.ProjectionRevision {
		return entrycapability.RemovalTaskPlan{}, errs.New(errs.KindStateConflict, "entry removal projection changed")
	}
	intent, err := etcd.NewEntryRemovalIntent(
		request.TaskID, request.EnvironmentID, request.EntryID, request.EntryRevision, &projection, request.CreatedAt,
	)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	owner := etcd.TaskOwner{
		WorkspaceType: etcd.TaskWorkspacePlatform,
		ProjectID:     request.Identity.ProjectID, EnvironmentID: request.Identity.EnvironmentID,
	}
	if request.Identity.TenantID != "" {
		owner.WorkspaceType = etcd.TaskWorkspaceTenant
		owner.TenantID = request.Identity.TenantID
	}
	task := etcd.TaskRecord{
		ID: request.TaskID, PlanID: request.PlanID, Executor: etcd.TaskExecutorAgent,
		Type: etcd.TaskRemove, Target: request.EntryID, TimeoutSeconds: 120,
		Status: etcd.TaskStatusPending, CreatedAt: request.CreatedAt, Owner: owner,
	}
	prepared, err := planner.plans.prepareEntryRemovalTask(
		ctx, task, intent, entryRemovalTaskProcedureIDs{ArtifactID: request.ArtifactID},
		planner.materials, request.Identity,
	)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	return entryRemovalTaskPlan(prepared)
}

func (resolver *TaskPlanResolver) prepareEntryRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
	procedure entryRemovalTaskProcedureIDs,
	materials EntryRemovalMaterializationResolver,
	identity entrycapability.RemovalEnvironmentIdentity,
) (etcd.TaskRecord, error) {
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil || intent.CandidateProjection == nil ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal procedure ids are invalid")
	}
	task.Params = map[string]string{
		etcd.TaskEntryEnvironmentParam:           intent.EnvironmentID,
		etcd.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:     intent.CandidateProjection.RevisionID,
		etcd.TaskComposeArtifactParam:            procedure.ArtifactID,
		etcd.TaskEntryTenantSlugParam:            identity.TenantSlug,
		etcd.TaskEntryProjectSlugParam:           identity.ProjectSlug,
		etcd.TaskEntryEnvironmentNameParam:       identity.EnvironmentName,
		etcd.TaskEntryAuthorizedVolumeDirParam:   identity.AuthorizedVolumeDir,
	}
	task.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	task.Steps = make([]etcd.TaskStepRecord, len(templates))
	task.Materializations = make([]etcd.TaskMaterializationRecord, len(templates))
	seenMaterializations := make(map[string]struct{}, len(templates))
	seenSteps := make(map[string]struct{}, len(templates))
	for index, template := range templates {
		materializationID := ids.New(ids.KindConfig)
		stepID := ids.New(ids.KindStep)
		if ids.Validate(ids.KindConfig, materializationID) != nil || ids.Validate(ids.KindStep, stepID) != nil {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal procedure ids are invalid")
		}
		if _, duplicate := seenMaterializations[materializationID]; duplicate {
			return etcd.TaskRecord{}, errs.New(
				errs.KindValidationFailed,
				"entry removal materialization id is duplicated",
			)
		}
		if _, duplicate := seenSteps[stepID]; duplicate {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal step id is duplicated")
		}
		seenMaterializations[materializationID] = struct{}{}
		seenSteps[stepID] = struct{}{}
		reference := template
		reference.MaterializationID = materializationID
		reference.StepID = stepID
		content := []byte(nil)
		if !entryRemovalOutputRemoves(reference.OutputKind) {
			if materials == nil {
				return etcd.TaskRecord{}, errs.New(
					errs.KindInternal,
					"entry removal materialization resolver is unavailable",
				)
			}
			content, err = materials.ResolveTaskMaterializationSource(ctx, intent.EnvironmentID, reference.Source)
			if err != nil {
				clear(content)
				return etcd.TaskRecord{}, err
			}
		}
		digest := sha256.Sum256(content)
		reference.Length = uint64(len(content))
		reference.SHA256 = hex.EncodeToString(digest[:])
		clear(content)
		task.Steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: stepID}
		task.Materializations[index] = reference
	}
	sort.Slice(task.Materializations, func(left int, right int) bool {
		return task.Materializations[left].StepID < task.Materializations[right].StepID
	})
	plan, err := resolver.buildEntryRemovalPlan(ctx, task, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return task, nil
}

func entryRemovalTaskPlan(task etcd.TaskRecord) (entrycapability.RemovalTaskPlan, error) {
	plan := entrycapability.RemovalTaskPlan{
		Executor: entrycapability.RemovalExecutorAgent, PlanHash: task.PlanHash,
		RenderGeneration:    task.RenderGeneration,
		EnvironmentID:       task.Params[etcd.TaskEntryEnvironmentParam],
		BlueprintRevisionID: task.Params[etcd.EnvironmentDesiredRevisionParam],
		ArtifactID:          task.Params[etcd.TaskComposeArtifactParam], TimeoutSeconds: task.TimeoutSeconds,
		Identity: entrycapability.RemovalEnvironmentIdentity{
			TenantID: task.Owner.TenantID, TenantSlug: task.Params[etcd.TaskEntryTenantSlugParam],
			ProjectID: task.Owner.ProjectID, ProjectSlug: task.Params[etcd.TaskEntryProjectSlugParam],
			EnvironmentID:       task.Owner.EnvironmentID,
			EnvironmentName:     task.Params[etcd.TaskEntryEnvironmentNameParam],
			AuthorizedVolumeDir: task.Params[etcd.TaskEntryAuthorizedVolumeDirParam],
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
	input etcd.TaskMaterializationRecord,
) (entrycapability.RemovalMaterialization, error) {
	source := entrycapability.RemovalSource{Kind: entrycapability.RemovalSourceKind(input.Source.Kind)}
	if input.Source.Kind == etcd.TaskMaterializationSourceGeneratedEnvironment {
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
	} else if input.Source.Kind != etcd.TaskMaterializationSourceRemoval {
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

func entryRemovalValueStorage(input etcd.TaskEntryValueStorage) (entrycapability.RemovalValueStorage, error) {
	switch input {
	case etcd.TaskEntryValueStoragePlain:
		return entrycapability.RemovalValueStoragePlain, nil
	case etcd.TaskEntryValueStorageSecret:
		return entrycapability.RemovalValueStorageSecret, nil
	default:
		return "", errs.New(errs.KindInternal, "durable Entry removal value storage is invalid")
	}
}

func entryRemovalOutputKind(
	input etcd.TaskMaterializationOutputKind,
) (entrycapability.RemovalOutputKind, error) {
	switch input {
	case etcd.TaskMaterializationOutputGeneratedEnvironment:
		return entrycapability.RemovalOutputGeneratedEnvironment, nil
	case etcd.TaskMaterializationOutputPlainFile:
		return entrycapability.RemovalOutputPlainFile, nil
	case etcd.TaskMaterializationOutputSecretFile:
		return entrycapability.RemovalOutputSecretFile, nil
	case etcd.TaskMaterializationOutputRemoveGeneratedEnv:
		return entrycapability.RemovalOutputRemoveGeneratedEnv, nil
	case etcd.TaskMaterializationOutputRemovePlainFile:
		return entrycapability.RemovalOutputRemovePlainFile, nil
	case etcd.TaskMaterializationOutputRemoveSecretFile:
		return entrycapability.RemovalOutputRemoveSecretFile, nil
	default:
		return "", errs.New(errs.KindInternal, "durable Entry removal output kind is invalid")
	}
}

func (resolver *TaskPlanResolver) resolveEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(entryRemovalPlanStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "entry removal plan state reader is unavailable")
	}
	stored, found, err := reader.GetEntryRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "entry removal intent is missing")
	}
	return resolver.buildEntryRemovalPlan(ctx, task, stored.Record)
}

func (resolver *TaskPlanResolver) buildEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		task.Type != etcd.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil ||
		task.ID != intent.TaskID || task.Target != intent.EntryID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != etcd.TaskStatusPending || intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 8 {
		return nil, errs.New(errs.KindInternal, "durable Entry removal Task shape is invalid")
	}
	candidate := *intent.CandidateProjection
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[etcd.TaskComposeArtifactParam]
	if task.Params[etcd.TaskEntryEnvironmentParam] != intent.EnvironmentID ||
		task.Params[etcd.TaskMaterializationEnvironmentParam] != intent.EnvironmentID ||
		revisionID != candidate.RevisionID || ids.Validate(ids.KindTask, revisionID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || uint64(task.RenderGeneration) != candidate.RenderGeneration {
		return nil, errs.New(errs.KindInternal, "durable Entry removal Task parameters are invalid")
	}
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil || len(task.Steps) != len(templates) || len(task.Materializations) != len(templates) {
		return nil, errs.New(errs.KindInternal, "durable Entry removal materialization count changed")
	}
	references := make(map[string]etcd.TaskMaterializationRecord, len(task.Materializations))
	for _, reference := range task.Materializations {
		if _, duplicate := references[reference.StepID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Entry removal step binding is duplicated")
		}
		references[reference.StepID] = reference
	}
	steps := make([]*agentpb.ExecutionStep, len(task.Steps))
	for index, step := range task.Steps {
		reference, found := references[step.ID]
		if !found || !sameEntryRemovalMaterializationTemplate(reference, templates[index]) {
			return nil, errs.New(errs.KindInternal, "durable Entry removal materialization changed")
		}
		steps[index], err = taskmaterialization.BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
		if err != nil {
			return nil, err
		}
	}

	identity, err := pinnedEntryRemovalIdentity(task, intent)
	if err != nil {
		return nil, err
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx,
		task,
		identity,
		revisionID,
		artifactID,
		candidate,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}

func pinnedEntryRemovalIdentity(
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
) (pinnedEnvironmentIdentity, error) {
	identity := pinnedEnvironmentIdentity{
		TenantID: task.Owner.TenantID, TenantSlug: task.Params[etcd.TaskEntryTenantSlugParam],
		ProjectID: task.Owner.ProjectID, ProjectSlug: task.Params[etcd.TaskEntryProjectSlugParam],
		EnvironmentID:       task.Owner.EnvironmentID,
		EnvironmentName:     task.Params[etcd.TaskEntryEnvironmentNameParam],
		AuthorizedVolumeDir: task.Params[etcd.TaskEntryAuthorizedVolumeDirParam],
	}
	validWorkspace := task.Owner.WorkspaceType == etcd.TaskWorkspaceTenant &&
		ids.Validate(ids.KindTenant, identity.TenantID) == nil && identity.TenantSlug != ""
	if identity.TenantID == "" {
		validWorkspace = task.Owner.WorkspaceType == etcd.TaskWorkspacePlatform && identity.TenantSlug == ""
	}
	if !validWorkspace || ids.Validate(ids.KindProject, identity.ProjectID) != nil ||
		ids.Validate(
			ids.KindEnvironment,
			identity.EnvironmentID,
		) != nil || identity.EnvironmentID != intent.EnvironmentID ||
		identity.ProjectSlug == "" || identity.EnvironmentName == "" || identity.AuthorizedVolumeDir == "" {
		return pinnedEnvironmentIdentity{}, errs.New(errs.KindInternal, "durable Entry removal identity is invalid")
	}
	return identity, nil
}

func entryRemovalMaterializationTemplates(
	intent etcd.EntryRemovalIntent,
) ([]etcd.TaskMaterializationRecord, error) {
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "entry removal projection is unavailable")
	}
	var removed *entryrecord.Record
	for index := range intent.CurrentProjection.Entries {
		if intent.CurrentProjection.Entries[index].Entry.ID == intent.EntryID {
			value := intent.CurrentProjection.Entries[index]
			removed = &value
			break
		}
	}
	if removed == nil || removed.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "entry removal target is absent from its pinned projection")
	}
	for _, record := range intent.CandidateProjection.Entries {
		if record.Entry.ID == intent.EntryID {
			return nil, errs.New(errs.KindInternal, "entry removal candidate retained its target")
		}
	}
	if removed.Entry.Kind == core.EntryKindFile {
		if removed.Entry.UID == nil || removed.Entry.GID == nil {
			return nil, errs.New(errs.KindInternal, "file Entry removal metadata is incomplete")
		}
		kind := etcd.TaskMaterializationOutputRemovePlainFile
		mode := entrymaterialization.ModeReadOnly
		if removed.Entry.Secret {
			kind = etcd.TaskMaterializationOutputRemoveSecretFile
			mode = entrymaterialization.ModePrivate
		}
		return []etcd.TaskMaterializationRecord{{
			EnvironmentID: intent.EnvironmentID, Destination: removed.Entry.Path,
			OutputKind: kind, UID: *removed.Entry.UID, GID: *removed.Entry.GID, Mode: uint32(mode),
			SHA256: hex.EncodeToString(sha256.New().Sum(nil)),
			Source: etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
		}}, nil
	}
	if removed.Entry.Kind != core.EntryKindEnv {
		return nil, errs.New(errs.KindInternal, "entry removal target kind is invalid")
	}
	scopes := append([]string(nil), removed.Entry.Exposure...)
	if removed.Entry.ExposesAll() {
		scopes = []string{"all"}
	} else {
		sort.Strings(scopes)
		for index, scope := range scopes {
			if scope == "all" || scope == "" || index > 0 && scope == scopes[index-1] {
				return nil, errs.New(errs.KindInternal, "entry removal exposure is invalid")
			}
		}
	}
	result := make([]etcd.TaskMaterializationRecord, len(scopes))
	for index, scope := range scopes {
		template, err := entryRemovalEnvironmentTemplate(intent, scope)
		if err != nil {
			return nil, err
		}
		result[index] = template
	}
	sort.Slice(result, func(left int, right int) bool {
		return result[left].Destination < result[right].Destination
	})
	return result, nil
}

func entryRemovalEnvironmentTemplate(
	intent etcd.EntryRemovalIntent,
	scope string,
) (etcd.TaskMaterializationRecord, error) {
	reference := etcd.TaskMaterializationRecord{
		EnvironmentID: intent.EnvironmentID, OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
		Mode: uint32(entrymaterialization.ModePrivate),
	}
	if scope == "all" {
		reference.Destination = environmentfile.EnvFileName(intent.EnvironmentID)
	} else {
		services, err := entryRemovalEnvironmentServiceIdentities(*intent.CandidateProjection)
		if err != nil {
			return etcd.TaskMaterializationRecord{}, err
		}
		for _, service := range services {
			if service.Name == scope {
				reference.ServiceID = service.ID
				reference.ServiceName = service.Name
				break
			}
		}
		if reference.ServiceID == "" {
			return etcd.TaskMaterializationRecord{}, errs.New(
				errs.KindInternal,
				"entry removal exposure Service is absent from its pinned projection",
			)
		}
		reference.Destination = environmentfile.ServiceEnvFileName(intent.EnvironmentID, scope)
	}
	values := make([]etcd.TaskGeneratedEnvironmentEntryReference, 0)
	for _, record := range intent.CandidateProjection.Entries {
		entry := record.Entry
		if entry.Kind != core.EntryKindEnv || scope == "all" && !entry.ExposesAll() ||
			scope != "all" && (entry.ExposesAll() || !entryExposesService(entry, scope)) {
			continue
		}
		storage := etcd.TaskEntryValueStoragePlain
		if entry.Secret {
			storage = etcd.TaskEntryValueStorageSecret
		}
		values = append(values, etcd.TaskGeneratedEnvironmentEntryReference{
			Name: entry.Key,
			Value: etcd.TaskEntryValueReference{
				EntryID: entry.ID, ValueGenerationID: record.CurrentValueGenerationID, Storage: storage,
			},
		})
	}
	sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
	for index := 1; index < len(values); index++ {
		if values[index].Name == values[index-1].Name {
			return etcd.TaskMaterializationRecord{}, errs.New(
				errs.KindInternal,
				"entry removal generated Environment contains a duplicate key",
			)
		}
	}
	if scope != "all" && len(values) == 0 {
		reference.OutputKind = etcd.TaskMaterializationOutputRemoveGeneratedEnv
		reference.SHA256 = hex.EncodeToString(sha256.New().Sum(nil))
		reference.Source = etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval}
		return reference, nil
	}
	reference.Source = etcd.TaskMaterializationSource{
		Kind:                 etcd.TaskMaterializationSourceGeneratedEnvironment,
		GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{FormatVersion: 1, Values: values},
	}
	return reference, nil
}

type entryRemovalServiceIdentity struct {
	ID   string
	Name string
}

func entryRemovalEnvironmentServiceIdentities(
	projection etcd.EnvironmentComposeProjection,
) ([]entryRemovalServiceIdentity, error) {
	identities := make([]entryRemovalServiceIdentity, 0, len(projection.DesiredServices))
	seenIDs := make(map[string]struct{}, len(projection.DesiredServices))
	seenNames := make(map[string]struct{}, len(projection.DesiredServices))
	add := func(identity entryRemovalServiceIdentity) error {
		if identity.ID == "" || identity.Name == "" {
			return errs.New(errs.KindInternal, "entry removal service identity is incomplete")
		}
		if _, duplicate := seenIDs[identity.ID]; duplicate {
			return errs.New(errs.KindInternal, "entry removal service identity id is duplicated")
		}
		if _, duplicate := seenNames[identity.Name]; duplicate {
			return errs.New(errs.KindInternal, "entry removal service identity name is duplicated")
		}
		seenIDs[identity.ID] = struct{}{}
		seenNames[identity.Name] = struct{}{}
		identities = append(identities, identity)
		return nil
	}
	generatedIDs := make(map[string]string)
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if serviceID == "" {
				return nil, errs.New(errs.KindInternal, "entry removal generated service identity is incomplete")
			}
			if _, duplicate := generatedIDs[serviceID]; duplicate {
				return nil, errs.New(errs.KindInternal, "entry removal generated service identity is duplicated")
			}
			generatedIDs[serviceID] = component.Desired.ID
		}
	}
	for _, service := range projection.DesiredServices {
		if _, generated := generatedIDs[service.Desired.ID]; generated {
			continue
		}
		if err := add(entryRemovalServiceIdentity{ID: service.Desired.ID, Name: service.Desired.Name}); err != nil {
			return nil, err
		}
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "entry removal Compose artifact is corrupt")
	}
	artifactServices := make(map[string]*agentpb.ComposeService, len(generatedIDs))
	for _, service := range artifact.GetServices() {
		if service == nil {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is invalid")
		}
		if _, generated := generatedIDs[service.GetServiceId()]; !generated {
			continue
		}
		if service.GetComposeName() == "" || service.GetOwnerComponentId() == "" ||
			service.GetOwnerComponentId() != generatedIDs[service.GetServiceId()] {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is invalid")
		}
		if _, duplicate := artifactServices[service.GetServiceId()]; duplicate {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is duplicated")
		}
		artifactServices[service.GetServiceId()] = service
	}
	generatedServiceIDs := make([]string, 0, len(generatedIDs))
	for serviceID := range generatedIDs {
		generatedServiceIDs = append(generatedServiceIDs, serviceID)
	}
	sort.Strings(generatedServiceIDs)
	for _, serviceID := range generatedServiceIDs {
		service, found := artifactServices[serviceID]
		if !found {
			return nil, errs.New(errs.KindInternal, "entry removal generated service metadata is missing")
		}
		if err := add(entryRemovalServiceIdentity{ID: service.GetServiceId(), Name: service.GetComposeName()}); err != nil {
			return nil, err
		}
	}
	sort.Slice(identities, func(left, right int) bool {
		if identities[left].Name == identities[right].Name {
			return identities[left].ID < identities[right].ID
		}
		return identities[left].Name < identities[right].Name
	})
	return identities, nil
}

func entryExposesService(entry core.EnvEntry, serviceName string) bool {
	for _, exposure := range entry.Exposure {
		if exposure == serviceName {
			return true
		}
	}
	return false
}

func sameEntryRemovalMaterializationTemplate(
	reference etcd.TaskMaterializationRecord,
	template etcd.TaskMaterializationRecord,
) bool {
	if reference.EnvironmentID != template.EnvironmentID || reference.Destination != template.Destination ||
		reference.ServiceID != template.ServiceID || reference.ServiceName != template.ServiceName ||
		reference.OutputKind != template.OutputKind || reference.UID != template.UID || reference.GID != template.GID ||
		reference.Mode != template.Mode || !sameEntryRemovalMaterializationSource(reference.Source, template.Source) {
		return false
	}
	if entryRemovalOutputRemoves(reference.OutputKind) {
		emptyDigest := sha256.Sum256(nil)
		return reference.Length == 0 && reference.SHA256 == hex.EncodeToString(emptyDigest[:])
	}
	return true
}

func sameEntryRemovalMaterializationSource(
	left etcd.TaskMaterializationSource,
	right etcd.TaskMaterializationSource,
) bool {
	if left.Kind != right.Kind {
		return false
	}
	if left.Kind == etcd.TaskMaterializationSourceRemoval {
		return left.BlueprintFile == nil && left.ComponentFile == nil && left.EntryValue == nil &&
			left.GeneratedEnvironment == nil && right.BlueprintFile == nil && right.ComponentFile == nil &&
			right.EntryValue == nil && right.GeneratedEnvironment == nil
	}
	if left.GeneratedEnvironment == nil || right.GeneratedEnvironment == nil ||
		left.GeneratedEnvironment.FormatVersion != right.GeneratedEnvironment.FormatVersion ||
		len(left.GeneratedEnvironment.Values) != len(right.GeneratedEnvironment.Values) {
		return false
	}
	for index := range left.GeneratedEnvironment.Values {
		if left.GeneratedEnvironment.Values[index] != right.GeneratedEnvironment.Values[index] {
			return false
		}
	}
	return true
}

func entryRemovalOutputRemoves(kind etcd.TaskMaterializationOutputKind) bool {
	return kind == etcd.TaskMaterializationOutputRemoveGeneratedEnv ||
		kind == etcd.TaskMaterializationOutputRemovePlainFile ||
		kind == etcd.TaskMaterializationOutputRemoveSecretFile
}
