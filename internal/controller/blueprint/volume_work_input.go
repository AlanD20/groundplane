package blueprint

import (
	"context"
	"encoding/hex"
	"maps"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Volume preparation does not wait for the Environment runtime projection:
// that projection may itself depend on this Volume. Render only the captured
// Volume, with no Services, hooks or mutable dependency facts.
func (service *Service) prepareAuthoredVolumeInput(ctx context.Context, work workqueue.View, create bool) (*agentpb.VolumeWorkInput, []keyvalue.Condition, error) {
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, work.Task.Task)
	if err != nil {
		return nil, nil, err
	}
	for _, revision := range []int64{source.desired.Revision, source.identities.Revision,
		source.environment.Revision, source.project.Revision} {
		if revision <= 0 || revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume source changed after its preparation snapshot")
		}
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return nil, nil, err
	}
	var identity projectionrecord.OwnedIdentity
	for _, candidate := range source.identities.Record.Volumes {
		if candidate.ID == work.Item.Item.TargetID {
			if identity.ID != "" {
				return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume identity is duplicated")
			}
			identity = candidate
		}
	}
	volume, found := project.Volumes[identity.Name]
	slug, slugged := volume.Extensions[composerender.ComposeVolumeSlugExtension].(string)
	if identity.ID == "" || !found || volume.Name != "" || bool(volume.External) ||
		(volume.Driver != "" && volume.Driver != "local") || len(volume.DriverOpts) != 0 ||
		len(volume.CustomLabels) != 0 || len(volume.Extensions) != 1 || !slugged || slug != identity.Slug {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume requires captured managed local storage")
	}
	if !create {
		birth, found, err := service.repository.GetEnvironmentDesiredInputRevision(ctx, source.environment.Record.ID, identity.BirthRevisionID)
		if err != nil {
			return nil, nil, err
		}
		if !found || birth.Revision <= 0 || birth.Revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume creation input is unavailable")
		}
		birthIdentities, found, err := service.repository.GetEnvironmentOwnedIdentitiesRevision(ctx, source.environment.Record.ID, identity.BirthRevisionID)
		if err != nil {
			return nil, nil, err
		}
		if !found || birthIdentities.Revision <= 0 || birthIdentities.Revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume creation identity is unavailable")
		}
		matched := false
		for _, original := range birthIdentities.Record.Volumes {
			if original.ID == identity.ID {
				if matched || original.Name != identity.Name {
					return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume creation ownership changed")
				}
				matched = true
			}
		}
		if !matched {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume is absent from its creation revision")
		}
		birthProject, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, birth.Record, birthIdentities.Record)
		if err != nil {
			return nil, nil, err
		}
		original, found := birthProject.Volumes[identity.Name]
		if !found || !maps.Equal(original.Labels, volume.Labels) {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint cannot change physical labels of an existing Volume")
		}
	}
	ownerKind := composerender.ComposeProjectOwnerBacking
	guards := []keyvalue.Condition{
		{Key: hierarchy.EnvironmentKey(source.environment.Record.ID), ModRevision: source.environment.Revision},
		{Key: hierarchy.ProjectKey(source.project.Record.ID), ModRevision: source.project.Revision},
	}
	if source.project.Record.Kind == hierarchy.ProjectKindTenant {
		if source.tenant.Revision <= 0 || source.tenant.Revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume Tenant changed after its preparation snapshot")
		}
		ownerKind = composerender.ComposeProjectOwnerTenant
		guards = append(guards, keyvalue.Condition{Key: hierarchy.TenantKey(source.tenant.Record.ID), ModRevision: source.tenant.Revision})
	}
	item := work.Item.Item
	planID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "volume-input")
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project:          &composetypes.Project{Name: project.Name, Volumes: composetypes.Volumes{identity.Name: volume}},
		ArtifactID:       ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "volume-artifact"),
		ProjectOwnerKind: ownerKind, TenantID: work.Task.Task.Scope.TenantID,
		ProjectID: source.project.Record.ID, EnvironmentID: source.environment.Record.ID,
		PlanID: planID, RenderGeneration: source.desired.Record.RenderGeneration,
		AuthorizedVolumeDir: source.environment.Record.VolumeDir,
		Identities:          composeidentity.Snapshot{Volumes: []composeidentity.Resource{{ID: identity.ID, Name: identity.Name}}},
	})
	if err != nil {
		return nil, nil, err
	}
	intent, err := hex.DecodeString(work.Task.Task.Intent.SHA256)
	if err != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Volume intent digest is invalid")
	}
	action := agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY
	if create {
		action = agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE
	}
	input := &agentpb.VolumeWorkInput{Action: action, EnvironmentId: source.environment.Record.ID,
		VolumeId: identity.ID, ComposeKey: identity.Name, IntentSha256: intent, Candidate: artifact}
	if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
		return nil, nil, err
	}
	return input, guards, nil
}
