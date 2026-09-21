package etcd

import (
	"context"
	"encoding/json"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

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
	Tenant            etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project           etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment       etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Service           etcdstore.Versioned[servicerecord.ServiceRecord]
	ScriptSet         etcdstore.Versioned[scriptrecord.SetGenerationRecord]
	Script            etcdstore.Versioned[scriptrecord.Record]
	BodyGeneration    etcdstore.Versioned[scriptrecord.BodyGenerationRecord]
	Release           ServingRelease
	RenderInput       etcdstore.Versioned[ReleaseRenderInput]
	DesiredHead       etcdstore.Versioned[EnvironmentBlueprintHead]
	DesiredProjection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	Networks          []etcdstore.Versioned[zonerecord.Record]
	AttachSources     ScriptAttachSources
}

// LoadBlueprintReleaseHookExecutionSources loads exact prepublished Script/body
// and candidate Release records while the same-Blueprint Service and projection
// remain visible only through their sealed candidate.
func (repository *ScriptRepository) LoadBlueprintReleaseHookExecutionSources(
	ctx context.Context,
	publicationID string,
	script scriptrecord.Record,
	service servicerecord.ServiceRecord,
	member ReleaseTaskRenderMember,
	tenant etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	projection projectionrecord.EnvironmentComposeProjection,
	intendedAttaches []etcdstore.Versioned[attachrecord.Record],
	revision int64,
) (ScriptExecutionSources, error) {
	if ctx == nil || repository == nil || repository.store == nil ||
		validatePublicationID(publicationID) != nil || revision <= 0 ||
		projection.EnvironmentID != environment.Record.ID ||
		projection.RevisionID == "" || projection.RenderGeneration == 0 ||
		service.EnvironmentID != environment.Record.ID ||
		script.EnvironmentID != environment.Record.ID ||
		service.Desired.ID != member.Intent.ServiceID ||
		script.ServiceID != service.Desired.ID {
		return ScriptExecutionSources{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Script candidate source request is invalid",
		)
	}
	script.ScriptSetGeneration = projection.RevisionID
	keys := []string{
		scriptrecord.ScriptSetScriptKey(script.EnvironmentID, script.ScriptSetGeneration, script.Desired.ID),
		scriptrecord.ScriptSetBodyGenerationKey(
			script.EnvironmentID,
			script.ScriptSetGeneration,
			script.Desired.ID,
			script.ActiveGeneration,
		),
		releaseIntentStagingKey(publicationID, member.Intent.ID),
		releaseRenderInputStagingKey(publicationID, member.Intent.ID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return ScriptExecutionSources{}, errs.New(
			errs.KindInternal,
			"Blueprint Script candidate source read is incomplete",
		)
	}
	for _, value := range read.Values {
		if value == nil {
			return ScriptExecutionSources{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Script candidate source is missing",
			)
		}
	}
	storedScript, err := scriptrecord.DecodeRecord(read.Values[0].Value)
	if err != nil || storedScript.Desired.ID != script.Desired.ID ||
		storedScript.ScriptSetGeneration != projection.RevisionID ||
		storedScript.ActiveGeneration != script.ActiveGeneration {
		return ScriptExecutionSources{}, recordcodec.CorruptRecord()
	}
	body, err := scriptrecord.DecodeScriptBodyGeneration(read.Values[1].Value)
	if err != nil || body.ScriptID != script.Desired.ID ||
		body.Generation != script.ActiveGeneration {
		return ScriptExecutionSources{}, recordcodec.CorruptRecord()
	}
	intent, err := decodeReleaseRecord[domain.Intent](
		read.Values[2].Value,
		"release-intent",
	)
	if err != nil || intent.ID != member.Intent.ID ||
		intent.OperationID != member.Intent.OperationID ||
		intent.ServiceID != member.Intent.ServiceID {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	raw, err := decodeReleaseRecord[json.RawMessage](
		read.Values[3].Value,
		"release-render-input",
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	render, err := decodeReleaseRenderInput(raw)
	if err != nil || render.ReleaseID != member.Render.ReleaseID ||
		render.ServiceID != member.Render.ServiceID ||
		render.Projection.RevisionID != projection.RevisionID {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	attachNetworks, err := resolveScriptAttachNetworks(
		ctx,
		repository.store,
		environment.Record.ID,
		service.Desired.ID,
		intendedAttaches,
		revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	return ScriptExecutionSources{
		AttachSources: attachNetworks,
		Revision:      revision,
		Tenant:        tenant,
		Project:       project,
		Environment:   environment,
		Service: etcdstore.Versioned[servicerecord.ServiceRecord]{
			Record:       service,
			ReadRevision: revision,
		},
		ScriptSet: etcdstore.Versioned[scriptrecord.SetGenerationRecord]{
			Record: scriptrecord.SetGenerationRecord{
				EnvironmentID: environment.Record.ID,
				GenerationID:  projection.RevisionID,
			},
			ReadRevision: revision,
		},
		Script: etcdstore.Versioned[scriptrecord.Record]{
			Record:       storedScript,
			Revision:     read.Values[0].ModRevision,
			ReadRevision: revision,
		},
		BodyGeneration: etcdstore.Versioned[scriptrecord.BodyGenerationRecord]{
			Record:       body,
			Revision:     read.Values[1].ModRevision,
			ReadRevision: revision,
		},
		Release: ServingRelease{
			Intent:         intent,
			IntentRevision: read.Values[2].ModRevision,
			Revision:       revision,
		},
		RenderInput: etcdstore.Versioned[ReleaseRenderInput]{
			Record:       render,
			Revision:     read.Values[3].ModRevision,
			ReadRevision: revision,
		},
		DesiredHead: etcdstore.Versioned[EnvironmentBlueprintHead]{
			Record: EnvironmentBlueprintHead{
				EnvironmentID: environment.Record.ID,
				RevisionID:    projection.RevisionID,
			},
			ReadRevision: revision,
		},
		DesiredProjection: etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
			Record:       projection,
			ReadRevision: revision,
		},
	}, nil
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
		scriptrecord.ScriptSetBodyGenerationKey(
			metadata.EnvironmentID,
			metadata.ScriptSetGeneration,
			scriptID,
			metadata.ActiveGeneration,
		),
		revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	body, err := scriptrecord.DecodeScriptBodyGeneration(bodyValue.Value)
	if err != nil || body.ScriptID != scriptID || body.Generation != metadata.ActiveGeneration {
		return ScriptExecutionSources{}, recordcodec.CorruptRecord()
	}
	metadata.Desired.Body = body.Body
	if err := metadata.Desired.Validate(); err != nil {
		return ScriptExecutionSources{}, recordcodec.CorruptRecord()
	}

	environmentValue, err := scriptExecutionValueAt(
		ctx,
		repository.store,
		hierarchyrecord.EnvironmentKey(metadata.EnvironmentID),
		revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil || environment.ID != metadata.EnvironmentID || environment.DeletionTaskID != "" {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script Environment is not runnable")
	}
	projectValue, err := scriptExecutionValueAt(ctx, repository.store, hierarchyrecord.ProjectKey(environment.ProjectID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	project, err := hierarchyrecord.DecodeProject(projectValue.Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant ||
		project.DeletionTaskID != "" || ids.Validate(ids.KindTenant, project.TenantID) != nil {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script Project is not runnable")
	}
	tenantValue, err := scriptExecutionValueAt(ctx, repository.store, hierarchyrecord.TenantKey(project.TenantID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	tenant, err := hierarchyrecord.DecodeTenant(tenantValue.Value)
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

	var release ServingRelease
	if releaseID == "" {
		release, err = ledger.ResolveServing(ctx, environment.ID, target.Desired.ID, revision)
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
		// Hook policy selects a sealed candidate before it can have a terminal
		// success record. Manual execution above still requires ResolveServing.
		release = ServingRelease{
			Intent: intent, IntentRevision: intentValue.ModRevision, Revision: revision,
		}
	}
	renderInput, err := ledger.GetReleaseRenderInputAt(ctx, release.Intent.ID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if releaseID != "" {
		value, encodeErr := EncodeReleaseRenderInput(renderInput.Record)
		digest, digestErr := domain.Digest(json.RawMessage(value))
		clear(value)
		if encodeErr != nil || digestErr != nil || digest != release.Intent.RenderInputDigest {
			return ScriptExecutionSources{}, corruptReleaseRecord()
		}
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
	if releaseID != "" && !sameReleaseHookAuthoredInputs(desiredProjection.Record, renderInput.Record.Projection) {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	networks, err := resolveScriptExecutionNetworks(desiredProjection)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	intendedAttaches, err := loadEnvironmentAttachesAtRevision(ctx, repository.store, environment.ID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	attachNetworks, err := resolveScriptAttachNetworks(
		ctx,
		repository.store,
		environment.ID,
		service.Record.Desired.ID,
		intendedAttaches,
		revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}

	return ScriptExecutionSources{
		Revision: revision,
		Tenant:   etcdstore.Versioned[hierarchyrecord.TenantRecord]{Record: tenant, Revision: tenantValue.ModRevision, ReadRevision: revision},
		Project:  etcdstore.Versioned[hierarchyrecord.ProjectRecord]{Record: project, Revision: projectValue.ModRevision, ReadRevision: revision},
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record: environment, Revision: environmentValue.ModRevision, ReadRevision: revision,
		},
		Service:   service,
		ScriptSet: stored.Active,
		Script:    etcdstore.Versioned[scriptrecord.Record]{Record: metadata, Revision: stored.Script.Revision, ReadRevision: revision},
		BodyGeneration: etcdstore.Versioned[scriptrecord.BodyGenerationRecord]{
			Record: body, Revision: bodyValue.ModRevision, ReadRevision: revision,
		},
		Release: release, RenderInput: renderInput,
		DesiredHead: desiredHead, DesiredProjection: desiredProjection,
		Networks: networks, AttachSources: attachNetworks,
	}, nil
}

// A Release can recapture Attach memberships in its generated artifact without
// changing the pinned authored inputs. Hooks consume normalized Compose and typed
// sources, not that artifact. Its bytes remain validated and bound by the Release
// render digest above; neither this comparison nor hook loading rewrites them.
func sameReleaseHookAuthoredInputs(root, captured projectionrecord.EnvironmentComposeProjection) bool {
	captured.ComposeArtifact = root.ComposeArtifact
	return sameServiceRemovalProjection(root, captured)
}

func loadScriptExecutionDesiredProjection(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	pinnedRevisionID string,
	revision int64,
) (etcdstore.Versioned[EnvironmentBlueprintHead], etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], error) {
	headValue, err := scriptExecutionValueAt(ctx, store, environmentBlueprintHeadKey(environmentID), revision)
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	headRevisionID, err := idempotencyrecord.DecodeTaskReference(headValue.Value)
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{},
			projectionrecord.CorruptEnvironmentComposeProjection()
	}
	selectedRevisionID := pinnedRevisionID
	if selectedRevisionID == "" {
		selectedRevisionID = headRevisionID
	}
	rootValue, err := scriptExecutionValueAt(
		ctx, store, environmentBlueprintRootKey(environmentID, selectedRevisionID), revision,
	)
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootValue.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != selectedRevisionID {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{},
			projectionrecord.CorruptEnvironmentComposeProjection()
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
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	defer clear(stream)
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(stream)
	if err != nil || readRevision != revision || projection.EnvironmentID != environmentID ||
		projection.RevisionID != selectedRevisionID {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{},
			projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[EnvironmentBlueprintHead]{
		Record:   EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: headRevisionID},
		Revision: headValue.ModRevision, ReadRevision: revision,
	}, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: rootValue.ModRevision, ReadRevision: revision,
	}, nil
}

func resolveScriptExecutionNetworks(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) ([]etcdstore.Versioned[zonerecord.Record], error) {
	networks := make([]etcdstore.Versioned[zonerecord.Record], len(projection.Record.DesiredZones))
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
) (*etcdstore.KeyValue, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution source is missing at its fixed revision")
	}
	return read.Values[0], nil
}
