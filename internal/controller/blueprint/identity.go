package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

func environmentBlueprintState(
	environmentID string,
	head etcdstore.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, composeidentity.Snapshot, uint64, error) {
	if hasHead != hasProjection {
		return 0, composeidentity.Snapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are inconsistent",
		)
	}
	if !hasHead {
		return 0, composeidentity.Snapshot{}, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, composeidentity.Snapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are corrupt",
		)
	}
	snapshot, err := authoredComposeIdentitySnapshot(projection.Record)
	if err != nil {
		return 0, composeidentity.Snapshot{}, 0, err
	}
	return head.Revision, snapshot, projection.Record.RenderGeneration + 1, nil
}

func authoredComposeIdentitySnapshot(
	projection projectionrecord.EnvironmentComposeProjection,
) (composeidentity.Snapshot, error) {
	project, err := composerender.LoadNormalizedEnvironmentProject(context.Background(), projection)
	if err != nil {
		return composeidentity.Snapshot{}, err
	}
	authoredNames := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		authoredNames[name] = struct{}{}
	}
	for name := range project.DisabledServices {
		authoredNames[name] = struct{}{}
	}
	snapshot := composeidentity.Snapshot{
		Services: make([]composeidentity.Resource, len(projection.DesiredServices)),
		Networks: make([]composeidentity.Resource, len(projection.DesiredZones)),
		Volumes:  make([]composeidentity.Resource, len(projection.Volumes)),
	}
	seenServiceIDs := make(map[string]string, len(projection.DesiredServices))
	seenServiceNames := make(map[string]string, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID ||
			ids.Validate(ids.KindService, service.Desired.ID) != nil ||
			service.Desired.Name == "" {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid authored Service identity",
			)
		}
		if name, duplicate := seenServiceIDs[service.Desired.ID]; duplicate && name != service.Desired.Name {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection repeats an authored Service id",
			)
		}
		if serviceID, duplicate := seenServiceNames[service.Desired.Name]; duplicate &&
			serviceID != service.Desired.ID {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection repeats an authored Service name",
			)
		}
		if _, authored := authoredNames[service.Desired.Name]; !authored {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired Service is absent from normalized Compose",
			)
		}
		delete(authoredNames, service.Desired.Name)
		seenServiceIDs[service.Desired.ID] = service.Desired.Name
		seenServiceNames[service.Desired.Name] = service.Desired.ID
		snapshot.Services[index] = composeidentity.Resource{
			ID:   service.Desired.ID,
			Name: service.Desired.Name,
		}
	}
	if len(authoredNames) != 0 {
		return composeidentity.Snapshot{}, errs.New(
			errs.KindInternal,
			"Environment normalized Compose has no desired Service identity",
		)
	}
	for index, zone := range projection.DesiredZones {
		if zone.EnvironmentID != projection.EnvironmentID || ids.Validate(ids.KindNetwork, zone.Desired.ID) != nil ||
			zone.Desired.Name == "" {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid Zone identity",
			)
		}
		snapshot.Networks[index] = composeidentity.Resource{ID: zone.Desired.ID, Name: zone.Desired.Name}
	}
	for index, volume := range projection.Volumes {
		if ids.Validate(ids.KindVolume, volume.ID) != nil || volume.Key == "" {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid Volume identity",
			)
		}
		snapshot.Volumes[index] = composeidentity.Resource{ID: volume.ID, Name: volume.Key}
	}
	return snapshot, nil
}
