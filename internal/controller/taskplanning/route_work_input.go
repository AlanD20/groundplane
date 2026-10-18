package taskplanning

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RouteWorkRequest supplies already captured inputs. A Route edit or removal
// reconciles the selected router; removing a Route never removes that Component.
// Resource publication and Route settlement remain the caller's responsibility.
type RouteWorkRequest struct {
	OperationID        string
	RevisionID         string
	Provider           environmentchanges.RouteProviderPin
	Component          componentrecord.Record
	Artifact           *agentpb.ComposeArtifact
	File               taskmaterialization.Record
	Configuration      runtimeconfiguration.WorkCapture
	PriorConfiguration runtimeconfiguration.Snapshot
}

// PrepareRouteWork emits the same complete Component operation used by native
// Component work. It does not manufacture generic steps or executable Tasks.
func (capture *ComponentConfigurationCapture) PrepareRouteWork(ctx context.Context,
	request RouteWorkRequest,
) (*agentpb.ComponentWorkInput, error) {
	pin, configuration := request.Provider, request.Configuration
	component := request.Component
	if capture == nil || ctx == nil || environmentchanges.ValidateRouteProviderPin(&pin) != nil ||
		pin.Runtime == nil || componentrecord.ValidateRecord(component) != nil || !component.Desired.Enabled ||
		component.Desired.ID != pin.ComponentID || component.Desired.OwnerID != pin.Runtime.EnvironmentID ||
		len(component.Runtime.GeneratedServices) != 1 || component.Runtime.GeneratedServices[0] != pin.ServiceID ||
		configuration.Validate() != nil || configuration.Current.EnvironmentID != pin.Runtime.EnvironmentID ||
		configuration.Generation != pin.InputGeneration || configuration.ReadRevision < pin.InputRevision ||
		request.Artifact == nil || request.File.EnvironmentID != pin.Runtime.EnvironmentID ||
		request.File.Destination != pin.Destination || request.File.ServiceID != "" || request.File.ServiceName != "" ||
		request.File.OutputKind != taskmaterialization.OutputPlainFile ||
		!componentWorkFileOwner(request.File, pin.Runtime.EnvironmentID, pin.ComponentID, pin.Destination) ||
		len(configuration.Destinations) != 1 || configuration.Destinations[0] != pin.Destination {
		return nil, errs.New(errs.KindStateConflict, "Route work requires its captured router, file and configuration")
	}
	prior, err := environmentchanges.DecodeRouteProviderRuntime(&pin)
	if err != nil {
		return nil, err
	}
	predecessor := &agentpb.ComponentWorkPredecessor{Absent: prior == nil, Runtime: prior}
	predecessorKind := core.ComponentKind("")
	if prior != nil {
		if prior.ArtifactId == request.Artifact.ArtifactId {
			return nil, errs.New(errs.KindStateConflict, "Route candidate reuses the retained router artifact")
		}
		predecessorKind = component.Desired.Kind
		predecessor.Configuration, err = capture.PrepareComponentWorkPredecessorConfiguration(ctx,
			pin.ComponentID, predecessorKind, prior, configuration, request.PriorConfiguration)
		if err != nil {
			return nil, err
		}
	}
	action, err := capture.PrepareComponentWorkAction(ctx, EnvironmentManagedConfigApplyInput{
		RevisionID: request.RevisionID, RenderGeneration: configuration.Generation,
		Components: []componentrecord.Record{component}, ComponentCatalog: capture.componentCatalog,
		Materializations: []taskmaterialization.Record{request.File}, Artifact: request.Artifact,
	}, configuration, request.PriorConfiguration, predecessorKind)
	if err != nil {
		return nil, err
	}
	if action == nil || action.ActionId != pin.ActionID ||
		hex.EncodeToString(action.DefinitionDigest) != pin.DefinitionDigest ||
		hex.EncodeToString(action.CatalogDigest) != pin.CatalogDigest {
		return nil, errs.New(errs.KindStateConflict, "Route activation differs from its captured provider")
	}
	current, err := componentConfigurationReference(configuration.Current)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(request.File.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, err)
	}
	file := request.File
	input := &agentpb.ComponentWorkInput{
		OperationId: request.OperationID, EnvironmentId: pin.Runtime.EnvironmentID, ComponentId: pin.ComponentID,
		RenderGeneration: configuration.Generation, Candidate: proto.CloneOf(request.Artifact),
		Predecessor: predecessor, ConfigurationAction: action, Configuration: current,
		Files: []*agentpb.MaterializeFile{{ArtifactId: request.Artifact.ArtifactId,
			MaterializationId: file.MaterializationID, EnvironmentId: file.EnvironmentID,
			Destination: file.Destination, ServiceId: file.ServiceID, ServiceName: file.ServiceName,
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
			Uid:        file.UID, Gid: file.GID, Mode: file.Mode, Length: file.Length, Sha256: digest}},
	}
	if configuration.Prior != nil {
		input.PriorConfiguration, err = componentConfigurationReference(*configuration.Prior)
		if err != nil {
			return nil, err
		}
	}
	if err := executionplan.ValidateComponentWorkInput(input); err != nil {
		return nil, err
	}
	return input, nil
}

func componentConfigurationReference(reference runtimeconfiguration.Reference) (*agentpb.WorkConfigurationReference, error) {
	if err := runtimeconfiguration.ValidateReference(reference); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
		EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}, nil
}
