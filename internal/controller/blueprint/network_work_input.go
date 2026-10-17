package blueprint

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// A bridge must be prepared before Services can render their runtime. Its input
// therefore depends on the captured authored Zone, not the runtime projection.
func (service *Service) prepareAuthoredNetworkInput(ctx context.Context, work workqueue.View) (*agentpb.NetworkWorkInput, []keyvalue.Condition, error) {
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, work.Task.Task)
	if err != nil {
		return nil, nil, err
	}
	for _, revision := range []int64{source.desired.Revision, source.identities.Revision,
		source.environment.Revision, source.project.Revision} {
		if revision <= 0 || revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone source changed after its preparation snapshot")
		}
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return nil, nil, err
	}
	var identity composeidentity.Resource
	for _, candidate := range source.identities.Record.Networks {
		if candidate.ID != work.Item.Item.TargetID {
			continue
		}
		if identity.ID != "" {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone identity is duplicated")
		}
		identity = composeidentity.Resource{ID: candidate.ID, Name: candidate.Name}
	}
	network, found := project.Networks[identity.Name]
	if identity.ID == "" || !found || bool(network.External) {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone requires a captured managed network")
	}
	ownerKind := composerender.ComposeProjectOwnerBacking
	guards := []keyvalue.Condition{
		{Key: hierarchy.EnvironmentKey(source.environment.Record.ID), ModRevision: source.environment.Revision},
		{Key: hierarchy.ProjectKey(source.project.Record.ID), ModRevision: source.project.Revision},
	}
	if source.project.Record.Kind == hierarchy.ProjectKindTenant {
		if source.tenant.Revision <= 0 || source.tenant.Revision > work.ReadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone Tenant changed after its preparation snapshot")
		}
		ownerKind = composerender.ComposeProjectOwnerTenant
		guards = append(guards, keyvalue.Condition{Key: hierarchy.TenantKey(source.tenant.Record.ID), ModRevision: source.tenant.Revision})
	}
	item := work.Item.Item
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project:          &composetypes.Project{Name: project.Name, Networks: composetypes.Networks{identity.Name: network}},
		ArtifactID:       ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "network-artifact"),
		ProjectOwnerKind: ownerKind, TenantID: work.Task.Task.Scope.TenantID,
		ProjectID: source.project.Record.ID, EnvironmentID: source.environment.Record.ID,
		PlanID:              ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "network-input"),
		RenderGeneration:    source.desired.Record.RenderGeneration,
		AuthorizedVolumeDir: source.environment.Record.VolumeDir,
		Identities:          composeidentity.Snapshot{Networks: []composeidentity.Resource{identity}},
	})
	if err != nil {
		return nil, nil, err
	}
	intent, err := hex.DecodeString(work.Task.Task.Intent.SHA256)
	if err != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone intent digest is invalid")
	}
	dockerName, err := networkname.New(identity.ID)
	if err != nil {
		return nil, nil, err
	}
	input := &agentpb.NetworkWorkInput{Action: agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE,
		EnvironmentId: source.environment.Record.ID, NetworkId: identity.ID,
		DockerName: dockerName, IntentSha256: intent, Candidate: artifact}
	if err := executionplan.ValidateNetworkWorkInput(input); err != nil {
		return nil, nil, err
	}
	return input, guards, nil
}
