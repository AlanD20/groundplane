package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: every full Environment render must preserve the union of active
// Attach memberships; otherwise a later Blueprint apply disconnects healthy
// consumers from their isolated backing-service networks.
func TestProjectAttachNetworksAddsExternalConsumerMemberships(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		apiID         = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		workerID      = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		postgresID    = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		valkeyID      = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	project := &composetypes.Project{Services: composetypes.Services{
		"api":    {Name: "api", Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}}},
		"worker": {Name: "worker"},
	}, Networks: composetypes.Networks{"frontend": {}}}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		DesiredServices: []etcd.EnvironmentServiceProjection{
			{EnvironmentID: environmentID, Desired: core.Service{ID: apiID, Name: "api"}},
			{EnvironmentID: environmentID, Desired: core.Service{ID: workerID, Name: "worker"}},
		},
		DesiredZones: []etcd.EnvironmentZoneProjection{{EnvironmentID: environmentID, Desired: core.Zone{
			ID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAX", Name: "frontend",
		}}},
	}
	external, err := ProjectAttachNetworks(project, projection, []etcd.AttachTaskNetworkJoin{
		{NetworkID: postgresID, ServiceIDs: []string{apiID}},
		{NetworkID: valkeyID, ServiceIDs: []string{apiID, workerID}},
	})
	if err != nil {
		t.Fatalf("ProjectAttachNetworks() error = %v", err)
	}
	postgresName := "gp_attach_net_01arz3ndektsv4rrffq69g5fav"
	valkeyName := "gp_attach_net_01arz3ndektsv4rrffq69g5faw"
	if len(external) != 2 || external[0] != (ComposeResourceIdentity{ID: postgresID, Name: postgresName}) ||
		external[1] != (ComposeResourceIdentity{ID: valkeyID, Name: valkeyName}) {
		t.Fatalf("external identities = %#v", external)
	}
	if !project.Networks[postgresName].External || !project.Networks[valkeyName].External {
		t.Fatalf("external networks = %#v", project.Networks)
	}
	api := project.Services["api"]
	worker := project.Services["worker"]
	if api.Networks[postgresName] == nil || api.Networks[valkeyName] == nil || api.Networks["frontend"] == nil ||
		worker.Networks[valkeyName] == nil || worker.Networks[postgresName] != nil {
		t.Fatalf("projected memberships: api=%#v worker=%#v", api.Networks, worker.Networks)
	}
}
