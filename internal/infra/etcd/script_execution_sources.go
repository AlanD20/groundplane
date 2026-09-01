package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptExecutionSources is the complete public source authority for one
// Script runner snapshot. Every Versioned value was read from Revision.
// Secret/plain Entry generation bytes are deliberately resolved through the
// private assignment-artifact path instead of entering this aggregate.
type ScriptExecutionSources struct {
	Revision          int64
	Tenant            Versioned[TenantRecord]
	Project           Versioned[ProjectRecord]
	Environment       Versioned[EnvironmentRecord]
	Service           Versioned[ServiceRecord]
	ScriptSet         Versioned[ScriptSetGenerationRecord]
	Script            Versioned[ScriptRecord]
	BodyGeneration    Versioned[ScriptBodyGenerationRecord]
	Release           CurrentSuccessfulRelease
	RenderInput       Versioned[ReleaseRenderInput]
	DesiredHead       Versioned[EnvironmentBlueprintHead]
	DesiredProjection Versioned[EnvironmentComposeProjection]
	Networks          []Versioned[ZoneRecord]
}

// LoadExecutionSources captures one Script execution from one fixed MVCC
// revision. The successful Release, rather than mutable Service desired state,
// is the image and service-definition authority.
func (repository *ScriptRepository) LoadExecutionSources(
	ctx context.Context,
	ledger *ReleaseLedger,
	scriptID string,
) (ScriptExecutionSources, error) {
	return repository.loadExecutionSources(ctx, ledger, scriptID, "", 0)
}

// LoadReleaseHookExecutionSources binds a Script to one policy-selected sealed
// Release at one exact revision. It never substitutes mutable serving state or
// desired image text for the supplied Release identity.
func (repository *ScriptRepository) LoadReleaseHookExecutionSources(
	ctx context.Context,
	ledger *ReleaseLedger,
	scriptID string,
	releaseID string,
	revision int64,
) (ScriptExecutionSources, error) {
	if ids.Validate(ids.KindDeployment, releaseID) != nil || revision <= 0 {
		return ScriptExecutionSources{}, errs.New(errs.KindValidationFailed, "release hook source request is invalid")
	}
	return repository.loadExecutionSources(ctx, ledger, scriptID, releaseID, revision)
}

func (repository *ScriptRepository) loadExecutionSources(
	ctx context.Context,
	ledger *ReleaseLedger,
	scriptID string,
	releaseID string,
	revision int64,
) (ScriptExecutionSources, error) {
	if ctx == nil || repository == nil || repository.store == nil || ledger == nil ||
		ids.Validate(ids.KindScript, scriptID) != nil {
		return ScriptExecutionSources{}, errs.New(
			errs.KindValidationFailed,
			"Script execution source request is invalid",
		)
	}
	stored, err := readActiveScriptStorage(ctx, repository.store, scriptID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	revision = stored.Script.ReadRevision
	metadata := stored.Script.Record
	bodyValue, err := scriptExecutionValueAt(
		ctx,
		repository.store,
		scriptSetBodyGenerationKey(metadata.EnvironmentID, metadata.ScriptSetGeneration, scriptID, metadata.ActiveGeneration),
		revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	body, err := decodeScriptBodyGeneration(bodyValue.Value)
	if err != nil || body.ScriptID != scriptID || body.Generation != metadata.ActiveGeneration {
		return ScriptExecutionSources{}, corruptRecord()
	}
	metadata.Desired.Body = body.Body
	if err := metadata.Desired.Validate(); err != nil {
		return ScriptExecutionSources{}, corruptRecord()
	}

	environmentValue, err := scriptExecutionValueAt(ctx, repository.store, environmentKey(metadata.EnvironmentID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	environment, err := decodeEnvironment(environmentValue.Value)
	if err != nil || environment.ID != metadata.EnvironmentID || environment.DeletionTaskID != "" {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script Environment is not runnable")
	}
	projectValue, err := scriptExecutionValueAt(ctx, repository.store, projectKey(environment.ProjectID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	project, err := decodeProject(projectValue.Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != ProjectKindTenant ||
		project.DeletionTaskID != "" || ids.Validate(ids.KindTenant, project.TenantID) != nil {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script Project is not runnable")
	}
	tenantValue, err := scriptExecutionValueAt(ctx, repository.store, tenantKey(project.TenantID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	tenant, err := decodeTenant(tenantValue.Value)
	if err != nil || tenant.ID != project.TenantID || tenant.DeletionTaskID != "" {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script Tenant is not runnable")
	}
	service, err := findServiceAtRevision(ctx, repository.store, metadata.ServiceID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	target := service.Record
	if target.EnvironmentID != environment.ID {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script target Service is not runnable")
	}

	var release CurrentSuccessfulRelease
	if releaseID == "" {
		release, err = ledger.ResolveCurrentSuccessful(ctx, environment.ID, target.Desired.ID, revision)
		if err != nil {
			return ScriptExecutionSources{}, err
		}
	} else {
		intentValue, readErr := scriptExecutionValueAt(
			ctx, repository.store, releaseIntentStagingKey("", releaseID), revision,
		)
		if readErr != nil {
			return ScriptExecutionSources{}, readErr
		}
		intent, decodeErr := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if decodeErr != nil || domain.ValidateIntent(intent) != nil || intent.ID != releaseID ||
			intent.EnvironmentID != environment.ID || intent.ServiceID != target.Desired.ID {
			return ScriptExecutionSources{}, corruptReleaseRecord()
		}
		release = CurrentSuccessfulRelease{
			Intent: intent, IntentRevision: intentValue.ModRevision, Revision: revision,
		}
	}
	renderInput, err := ledger.GetReleaseRenderInputAt(ctx, release.Intent.ID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if renderInput.Record.EnvironmentID != environment.ID || renderInput.Record.ServiceID != target.Desired.ID ||
		renderInput.Record.Projection.RevisionID == "" || renderInput.Record.Projection.RenderGeneration == 0 {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	pinnedRevisionID := ""
	if releaseID != "" {
		pinnedRevisionID = renderInput.Record.Projection.RevisionID
	}
	desiredHead, desiredProjection, err := loadScriptExecutionDesiredProjection(
		ctx, repository.store, environment.ID, pinnedRevisionID, revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if releaseID != "" && !sameServiceRemovalProjection(desiredProjection.Record, renderInput.Record.Projection) {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	networks, err := resolveScriptExecutionNetworks(desiredProjection)
	if err != nil {
		return ScriptExecutionSources{}, err
	}

	return ScriptExecutionSources{
		Revision: revision,
		Tenant:   Versioned[TenantRecord]{Record: tenant, Revision: tenantValue.ModRevision, ReadRevision: revision},
		Project:  Versioned[ProjectRecord]{Record: project, Revision: projectValue.ModRevision, ReadRevision: revision},
		Environment: Versioned[EnvironmentRecord]{
			Record: environment, Revision: environmentValue.ModRevision, ReadRevision: revision,
		},
		Service:   service,
		ScriptSet: stored.Active,
		Script:    Versioned[ScriptRecord]{Record: metadata, Revision: stored.Script.Revision, ReadRevision: revision},
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{
			Record: body, Revision: bodyValue.ModRevision, ReadRevision: revision,
		},
		Release: release, RenderInput: renderInput,
		DesiredHead: desiredHead, DesiredProjection: desiredProjection,
		Networks: networks,
	}, nil
}

func loadScriptExecutionDesiredProjection(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	pinnedRevisionID string,
	revision int64,
) (Versioned[EnvironmentBlueprintHead], Versioned[EnvironmentComposeProjection], error) {
	headValue, err := scriptExecutionValueAt(ctx, store, environmentBlueprintHeadKey(environmentID), revision)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{}, err
	}
	headRevisionID, err := decodeTaskReference(headValue.Value)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{},
			corruptEnvironmentComposeProjection()
	}
	selectedRevisionID := pinnedRevisionID
	if selectedRevisionID == "" {
		selectedRevisionID = headRevisionID
	}
	rootValue, err := scriptExecutionValueAt(
		ctx, store, environmentBlueprintRootKey(environmentID, selectedRevisionID), revision,
	)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{}, err
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootValue.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != selectedRevisionID {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{},
			corruptEnvironmentComposeProjection()
	}
	keys := make([]string, int(seal.ProjectionChunks))
	for index := range keys {
		keys[index] = environmentBlueprintChunkKeyFor(
			environmentID, selectedRevisionID, EnvironmentBlueprintChunkProjection, uint32(index),
		)
	}
	stream, readRevision, err := (&HierarchyRepository{store: store}).readEnvironmentBlueprintStreamAtRevision(
		ctx, seal, "projection", keys, revision,
	)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{}, err
	}
	defer clear(stream)
	projection, err := decodeEnvironmentComposeProjection(stream)
	if err != nil || readRevision != revision || projection.EnvironmentID != environmentID ||
		projection.RevisionID != selectedRevisionID {
		return Versioned[EnvironmentBlueprintHead]{}, Versioned[EnvironmentComposeProjection]{},
			corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentBlueprintHead]{
		Record:   EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: headRevisionID},
		Revision: headValue.ModRevision, ReadRevision: revision,
	}, Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: rootValue.ModRevision, ReadRevision: revision,
	}, nil
}

func resolveScriptExecutionNetworks(
	projection Versioned[EnvironmentComposeProjection],
) ([]Versioned[ZoneRecord], error) {
	networks := make([]Versioned[ZoneRecord], len(projection.Record.DesiredZones))
	for index, desired := range projection.Record.DesiredZones {
		joined, err := joinEnvironmentZone(projection, desired)
		if err != nil {
			return nil, errs.New(errs.KindStateConflict, "Script network source changed")
		}
		networks[index] = joined
	}
	return networks, nil
}

func scriptExecutionValueAt(
	ctx context.Context,
	store hierarchyStore,
	key string,
	revision int64,
) (*KeyValue, error) {
	read, err := store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution source is missing at its fixed revision")
	}
	return read.Values[0], nil
}
