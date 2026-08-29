package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
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
	Script            Versioned[ScriptRecord]
	BodyGeneration    Versioned[ScriptBodyGenerationRecord]
	Release           CurrentSuccessfulRelease
	RenderInput       Versioned[ReleaseRenderInput]
	AppliedProjection Versioned[EnvironmentComposeProjection]
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
	if ctx == nil || repository == nil || repository.store == nil || ledger == nil ||
		ids.Validate(ids.KindScript, scriptID) != nil {
		return ScriptExecutionSources{}, errs.New(
			errs.KindValidationFailed,
			"Script execution source request is invalid",
		)
	}
	primary, err := repository.store.Get(ctx, scriptKey(scriptID))
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if primary == nil || primary.Entry == nil || primary.ReadRevision <= 0 {
		return ScriptExecutionSources{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	revision := primary.ReadRevision
	metadata, err := decodeScriptRecord(primary.Entry.Value)
	if err != nil || metadata.Desired.ID != scriptID || metadata.ActiveGeneration == 0 {
		return ScriptExecutionSources{}, corruptRecord()
	}
	bodyValue, err := scriptExecutionValueAt(
		ctx,
		repository.store,
		scriptBodyGenerationKey(scriptID, metadata.ActiveGeneration),
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
	serviceValue, err := scriptExecutionValueAt(ctx, repository.store, serviceKey(metadata.ServiceID), revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	target, err := decodeServiceRecord(serviceValue.Value)
	if err != nil || target.Desired.ID != metadata.ServiceID || target.EnvironmentID != environment.ID {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script target Service is not runnable")
	}

	release, err := ledger.ResolveCurrentSuccessful(ctx, environment.ID, target.Desired.ID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	renderInput, err := ledger.GetReleaseRenderInputAt(ctx, release.Intent.ID, revision)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	if renderInput.Record.EnvironmentID != environment.ID || renderInput.Record.ServiceID != target.Desired.ID ||
		renderInput.Record.Projection.RevisionID == "" || renderInput.Record.Projection.RenderGeneration == 0 {
		return ScriptExecutionSources{}, corruptReleaseRecord()
	}
	appliedValue, err := scriptExecutionValueAt(
		ctx, repository.store, environmentComposeProjectionKey(environment.ID), revision,
	)
	if err != nil {
		return ScriptExecutionSources{}, err
	}
	applied, err := decodeEnvironmentComposeProjection(appliedValue.Value)
	if err != nil || applied.EnvironmentID != environment.ID || applied.RevisionID == "" ||
		applied.RenderGeneration == 0 {
		return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script applied Environment projection is unavailable")
	}

	networks := make([]Versioned[ZoneRecord], len(applied.Networks))
	for index, identity := range applied.Networks {
		value, err := scriptExecutionValueAt(ctx, repository.store, zoneKey(identity.ID), revision)
		if err != nil {
			return ScriptExecutionSources{}, err
		}
		network, err := decodeZoneRecord(value.Value)
		if err != nil || network.Desired.ID != identity.ID || network.EnvironmentID != environment.ID {
			return ScriptExecutionSources{}, errs.New(errs.KindStateConflict, "Script network source changed")
		}
		networks[index] = Versioned[ZoneRecord]{
			Record: network, Revision: value.ModRevision, ReadRevision: revision,
		}
	}

	return ScriptExecutionSources{
		Revision: revision,
		Tenant:   Versioned[TenantRecord]{Record: tenant, Revision: tenantValue.ModRevision, ReadRevision: revision},
		Project:  Versioned[ProjectRecord]{Record: project, Revision: projectValue.ModRevision, ReadRevision: revision},
		Environment: Versioned[EnvironmentRecord]{
			Record: environment, Revision: environmentValue.ModRevision, ReadRevision: revision,
		},
		Service: Versioned[ServiceRecord]{Record: target, Revision: serviceValue.ModRevision, ReadRevision: revision},
		Script:  Versioned[ScriptRecord]{Record: metadata, Revision: primary.Entry.ModRevision, ReadRevision: revision},
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{
			Record: body, Revision: bodyValue.ModRevision, ReadRevision: revision,
		},
		Release: release, RenderInput: renderInput,
		AppliedProjection: Versioned[EnvironmentComposeProjection]{
			Record: applied, Revision: appliedValue.ModRevision, ReadRevision: revision,
		},
		Networks: networks,
	}, nil
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
