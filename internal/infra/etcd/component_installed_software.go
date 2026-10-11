package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// GetComponentInstalledSoftware uses the retained acknowledgement at the same
// MVCC view as the desired read. The current catalog or candidate cannot relabel
// an older installation; no runtime receipt means no installed identity.
func (repository *ComponentRepository) GetComponentInstalledSoftware(
	ctx context.Context,
	componentID string,
	revision int64,
) (*buildversion.ContainerIdentity, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Component software read requires its id and storage revision")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			components.RecordKey(componentID),
			platformcomponents.ComponentObservationKey(componentID),
			componentplanning.RuntimeKey(componentID),
		},
		Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil || component.Desired.ID != componentID {
		return nil, recordcodec.CorruptRecord()
	}
	var artifact *agentpb.ComposeArtifact
	switch component.Desired.Owner {
	case core.ComponentOwnerPlatform:
		if read.Values[1] == nil {
			return nil, nil
		}
		observed, err := platformcomponents.DecodeComponentObservation(read.Values[1].Value)
		if err != nil || observed.ComponentID != componentID {
			return nil, recordcodec.CorruptRecord()
		}
		if !observed.Enabled {
			return nil, nil
		}
		artifact = observed.ComposeArtifact
	case core.ComponentOwnerEnvironment:
		if read.Values[2] == nil {
			return nil, nil
		}
		acknowledged, err := recordcodec.Decode[componentplanning.Runtime](
			read.Values[2].Value,
			"blueprint-component-runtime",
		)
		if err != nil || componentplanning.ValidateRuntime(acknowledged) != nil ||
			acknowledged.Component.Desired.ID != componentID || acknowledged.EnvironmentID != component.Desired.OwnerID {
			return nil, recordcodec.CorruptRecord()
		}
		if !acknowledged.Component.Desired.Enabled {
			return nil, nil
		}
		artifact = &agentpb.ComposeArtifact{}
		if err := proto.Unmarshal(acknowledged.Artifact, artifact); err != nil {
			return nil, recordcodec.CorruptRecord()
		}
	default:
		return nil, recordcodec.CorruptRecord()
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() != componentID {
			continue
		}
		if selected != nil {
			return nil, recordcodec.CorruptRecord()
		}
		selected = service
	}
	if selected == nil {
		return nil, recordcodec.CorruptRecord()
	}
	identity := &buildversion.ContainerIdentity{
		Image:        selected.GetImageReference(),
		ConfigDigest: "sha256:" + hex.EncodeToString(selected.GetImageConfigDigest()),
		SelectedTag:  selected.GetImageSelectedTag(),
	}
	if err := identity.Validate(); err != nil {
		return nil, recordcodec.CorruptRecord()
	}
	return identity, nil
}
