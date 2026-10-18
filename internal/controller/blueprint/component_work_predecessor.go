package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) prepareComponentWorkPredecessor(ctx context.Context, placement componentWorkPlacement,
	capture runtimeconfiguration.WorkCapture,
) (*agentpb.ComponentWorkPredecessor, error) {
	current := placement.Selection.Current
	if !current.RuntimePresent || !current.Runtime.Record.Component.Desired.Enabled {
		return &agentpb.ComponentWorkPredecessor{Absent: true}, nil
	}
	runtime := current.Runtime.Record
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(runtime.Artifact, artifact); err != nil {
		return nil, errs.New(errs.KindStateConflict, "Component acknowledged runtime cannot be decoded")
	}
	if artifact.OwnerId != capture.Current.EnvironmentID || len(artifact.Services) != 1 ||
		artifact.Services[0].GetOwnerComponentId() != current.Component.Record.Desired.ID {
		return nil, errs.New(errs.KindStateConflict, "Component predecessor contains unrelated workloads")
	}
	var prior runtimeconfiguration.Snapshot
	var err error
	if capture.Prior != nil {
		prior, err = service.repository.LoadComponentWorkConfiguration(ctx, *capture.Prior)
		if err != nil {
			return nil, err
		}
	}
	configuration, err := service.configuration.PrepareComponentWorkPredecessorConfiguration(ctx,
		current.Component.Record.Desired.ID, runtime.Component.Desired.Kind, artifact, capture, prior)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkPredecessor{Runtime: artifact, Configuration: configuration}, nil
}
