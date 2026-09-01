package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestClassifyBackingServiceCreationUsesCurrentConditionLayout(t *testing.T) {
	publication := environmentBlueprintPublicationEvidence{
		rootRevision: 11, descriptorRevision: 12, locatorRevision: 13,
	}
	values := make([]*KeyValue, 30)
	values[5] = &KeyValue{ModRevision: publication.rootRevision}
	values[6] = &KeyValue{ModRevision: publication.descriptorRevision}
	values[7] = &KeyValue{ModRevision: publication.locatorRevision}

	err := classifyBackingServiceCreation(BackingServiceCreation{}, publication, len(values))(1, values)
	if err == nil || !strings.Contains(err.Error(), "Backing-service creation raced") {
		t.Fatalf("classification error = %v, want final race classification", err)
	}
}

func TestValidateBackingServiceComponentsRejectsNonEmpty(t *testing.T) {
	t.Parallel()
	err := validateBackingServiceComponents([]ComponentRecord{{}})
	if err == nil {
		t.Fatal("validateBackingServiceComponents() accepted non-empty Components")
	}
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed ||
		!strings.Contains(err.Error(), "zero Environment Components") {
		t.Fatalf("validateBackingServiceComponents() error = %v, want validation rejection", err)
	}
}

func TestBackingServiceCreationValidatesDesiredTopology(t *testing.T) {
	t.Parallel()
	const (
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		networkID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		volumeID      = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	zone := core.Zone{
		ID: networkID, Name: "database", Subnet: "10.40.1.0/24",
		OwnerKind: core.ZoneOwnerBackingProject, OwnerID: projectID,
	}
	service := core.Service{
		ID: serviceID, Name: "postgres", Image: "postgres:16-alpine",
		Strategy: core.StrategyRecreate, Adapter: "postgres:16", Command: []string{"postgres"},
	}
	creation := BackingServiceCreation{
		Project:     ProjectRecord{ID: projectID},
		Environment: EnvironmentRecord{ID: environmentID},
		Zone:        ZoneRecord{EnvironmentID: environmentID, Desired: zone},
		Service: ServiceRecord{
			EnvironmentID: environmentID, BackingNetworkID: networkID, Desired: service,
		},
		Claim: EnvironmentBlueprintStageClaim{
			EnvironmentID: environmentID, RevisionID: taskID, TaskID: taskID,
			SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1,
		},
		Revision: EnvironmentDesiredRevisionIdentity{EnvironmentID: environmentID, RevisionID: taskID},
		Task:     TaskRecord{ID: taskID},
		Projection: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 1,
			ComposeArtifact: []byte{1},
			DesiredZones:    []EnvironmentZoneProjection{{EnvironmentID: environmentID, Desired: zone}},
			DesiredServices: []EnvironmentServiceProjection{{
				EnvironmentID: environmentID, BackingNetworkID: networkID, Desired: service,
			}},
			Volumes: []EnvironmentVolumeIdentity{{ID: volumeID}},
			VolumeMounts: []EnvironmentServiceVolumeMount{{
				ServiceID: serviceID, VolumeID: volumeID, Target: "/var/lib/postgresql/data",
			}},
		},
	}
	if err := validateBackingServiceProjection(creation); err != nil {
		t.Fatalf("validateBackingServiceProjection() error = %v", err)
	}

	changedService := creation
	changedService.Projection.DesiredServices = append(
		[]EnvironmentServiceProjection(nil), creation.Projection.DesiredServices...,
	)
	changedService.Projection.DesiredServices[0].Desired.Image = "postgres:17-alpine"
	if err := validateBackingServiceProjection(changedService); err == nil {
		t.Fatal("validateBackingServiceProjection() accepted a changed desired Service")
	}

	changedZone := creation
	changedZone.Projection.DesiredZones = append(
		[]EnvironmentZoneProjection(nil), creation.Projection.DesiredZones...,
	)
	changedZone.Projection.DesiredZones[0].Desired.Internal = true
	if err := validateBackingServiceProjection(changedZone); err == nil {
		t.Fatal("validateBackingServiceProjection() accepted a changed desired Zone")
	}
}
