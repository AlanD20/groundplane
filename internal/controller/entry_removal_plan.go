package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type entryRemovalPlanStateReader interface {
	GetEntryRemovalIntent(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EntryRemovalIntent], bool, error)
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

// EntryRemovalTaskProcedureIDs are allocated once before durable publication.
// Materialization and step ids correspond by index to the canonical,
// destination-sorted removal procedure derived from the pinned intent.
type EntryRemovalTaskProcedureIDs struct {
	ArtifactID string
}

// PrepareEntryRemovalTask resolves generated Environment bytes only long
// enough to pin their metadata. Removal outputs always pin the empty digest.
// The resulting Task contains every input needed for restart reconstruction.
func (resolver *TaskPlanResolver) PrepareEntryRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
	procedure EntryRemovalTaskProcedureIDs,
	materials EntryRemovalMaterializationResolver,
) (etcd.TaskRecord, error) {
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil || intent.CandidateProjection == nil ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Entry removal procedure ids are invalid")
	}
	task.Params = map[string]string{
		etcd.TaskEntryEnvironmentParam:           intent.EnvironmentID,
		etcd.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:   intent.CandidateProjection.RevisionID,
		etcd.TaskComposeArtifactParam:            procedure.ArtifactID,
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
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Entry removal procedure ids are invalid")
		}
		if _, duplicate := seenMaterializations[materializationID]; duplicate {
			return etcd.TaskRecord{}, errs.New(
				errs.KindValidationFailed,
				"Entry removal materialization id is duplicated",
			)
		}
		if _, duplicate := seenSteps[stepID]; duplicate {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Entry removal step id is duplicated")
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
					"Entry removal materialization resolver is unavailable",
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
		task.Steps[index] = etcd.TaskStepRecord{ID: stepID}
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

func (resolver *TaskPlanResolver) resolveEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(entryRemovalPlanStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Entry removal plan state reader is unavailable")
	}
	stored, found, err := reader.GetEntryRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Entry removal intent is missing")
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
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 4 {
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
		steps[index], err = BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
		if err != nil {
			return nil, err
		}
	}

	environment, err := resolver.blueprints.GetEnvironment(ctx, intent.EnvironmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return nil, err
	}
	if environment.Record.ID != intent.EnvironmentID ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
		project.Record.ID != environment.Record.ProjectID || project.Record.Kind != etcd.ProjectKindTenant ||
		tenant.Record.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "Entry removal Environment hierarchy is invalid")
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx,
		task,
		pinnedEnvironmentIdentity{
			TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug,
			ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
			EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
			AuthorizedVolumeDir: environment.Record.VolumeDir,
		},
		revisionID,
		artifactID,
		candidate,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}

func entryRemovalMaterializationTemplates(
	intent etcd.EntryRemovalIntent,
) ([]etcd.TaskMaterializationRecord, error) {
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "Entry removal projection is unavailable")
	}
	var removed *etcd.EntryRecord
	for index := range intent.CurrentProjection.Entries {
		if intent.CurrentProjection.Entries[index].Entry.ID == intent.EntryID {
			value := intent.CurrentProjection.Entries[index]
			removed = &value
			break
		}
	}
	if removed == nil || removed.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "Entry removal target is absent from its pinned projection")
	}
	for _, record := range intent.CandidateProjection.Entries {
		if record.Entry.ID == intent.EntryID {
			return nil, errs.New(errs.KindInternal, "Entry removal candidate retained its target")
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
			Source: etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
		}}, nil
	}
	if removed.Entry.Kind != core.EntryKindEnv {
		return nil, errs.New(errs.KindInternal, "Entry removal target kind is invalid")
	}
	scopes := append([]string(nil), removed.Entry.Exposure...)
	if removed.Entry.ExposesAll() {
		scopes = []string{"all"}
	} else {
		sort.Strings(scopes)
		for index, scope := range scopes {
			if scope == "all" || scope == "" || index > 0 && scope == scopes[index-1] {
				return nil, errs.New(errs.KindInternal, "Entry removal exposure is invalid")
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
		reference.Destination = EnvFileName(intent.EnvironmentID)
	} else {
		for _, service := range intent.CandidateProjection.Services {
			if service.Name == scope {
				reference.ServiceID = service.ID
				reference.ServiceName = service.Name
				break
			}
		}
		if reference.ServiceID == "" {
			return etcd.TaskMaterializationRecord{}, errs.New(
				errs.KindInternal,
				"Entry removal exposure Service is absent from its pinned projection",
			)
		}
		reference.Destination = ServiceEnvFileName(intent.EnvironmentID, scope)
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
				"Entry removal generated Environment contains a duplicate key",
			)
		}
	}
	if scope != "all" && len(values) == 0 {
		reference.OutputKind = etcd.TaskMaterializationOutputRemoveGeneratedEnv
		reference.Source = etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval}
		return reference, nil
	}
	reference.Source = etcd.TaskMaterializationSource{
		Kind:                 etcd.TaskMaterializationSourceGeneratedEnvironment,
		GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{FormatVersion: 1, Values: values},
	}
	return reference, nil
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
