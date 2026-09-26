package blueprint

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale (BP-01): direct credential reuse and grants are durable Attach
// relationships. Export and Validate must preserve their names and reject a
// changed topology instead of previewing a duplicate create or losing facts.
func TestDirectAttachTopologyExportsAndValidates(t *testing.T) {
	at := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	backingProjectID := ids.NewAt(ids.KindProject, at, 2)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	backingServiceID := ids.NewAt(ids.KindService, at, 4)
	backingNetworkID := ids.NewAt(ids.KindNetwork, at, 5)
	ownerID := ids.NewAt(ids.KindAttach, at, 6)
	dependentID := ids.NewAt(ids.KindAttach, at, 7)
	grantID := ids.NewAt(ids.KindAttach, at, 8)
	ownerServiceID := ids.NewAt(ids.KindService, at, 9)
	dependentServiceID := ids.NewAt(ids.KindService, at, 10)
	grantServiceID := ids.NewAt(ids.KindService, at, 11)
	repository := &authoringRoundtripRepository{
		blueprintPreflightRepository: &blueprintPreflightRepository{},
		extraProjects: map[string]hierarchyrecord.ProjectRecord{
			backingProjectID: {ID: backingProjectID, Slug: "data", Kind: hierarchyrecord.ProjectKindBacking},
		},
		extraEnvironments: map[string]hierarchyrecord.EnvironmentRecord{
			backingEnvironmentID: {ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main"},
		},
		services: map[string]servicerecord.ServiceRecord{
			backingServiceID: {EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
				Desired: core.Service{ID: backingServiceID, Name: "postgres"}},
			ownerServiceID: {EnvironmentID: environmentID, Desired: core.Service{ID: ownerServiceID, Name: "api"}},
			dependentServiceID: {EnvironmentID: environmentID,
				Desired: core.Service{ID: dependentServiceID, Name: "worker"}},
			grantServiceID: {EnvironmentID: environmentID,
				Desired: core.Service{ID: grantServiceID, Name: "reports"}},
		},
	}
	newAttach := func(id, name, serviceID, credentialID string, grants []string) attachrecord.Record {
		return attachrecord.Record{
			ID: id, EnvironmentID: environmentID, Name: name,
			BackingProjectID: backingProjectID, BackingEnvironmentID: backingEnvironmentID,
			BackingServiceID: backingServiceID, BackingNetworkID: backingNetworkID,
			ServiceID: serviceID, CredentialAttachID: credentialID, GrantAttachIDs: grants,
			Status: core.AttachReady, Operation: attachrecord.AttachOperationProvision,
		}
	}
	current := []etcdstore.Versioned[attachrecord.Record]{
		{Record: newAttach(ownerID, "api-db", ownerServiceID, ownerID, []string{grantID})},
		{Record: newAttach(dependentID, "worker-db", dependentServiceID, ownerID, nil)},
		{Record: newAttach(grantID, "reporting", grantServiceID, grantID, nil)},
	}
	service := &Service{repository: repository}
	specs, err := service.authoringAttachmentSpecs(t.Context(), environmentID, current)
	if err != nil {
		t.Fatal(err)
	}
	if specs["api-db"].BackingProject != "data" || specs["api-db"].BackingService != "postgres" ||
		specs["api-db"].Service != "api" || specs["api-db"].Credential.Mode != "new" ||
		len(specs["api-db"].Grants) != 1 || specs["api-db"].Grants[0] != "reporting" ||
		specs["worker-db"].Credential != (core.AttachmentCredentialSpec{Mode: "existing", Attach: "api-db"}) {
		t.Fatalf("durable relationships were not exported: %#v", specs)
	}
	if err := validateCurrentAttachmentDeclarations(specs, specs, current); err != nil {
		t.Fatalf("unchanged direct Attaches were rejected: %v", err)
	}
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{Attachments: specs},
		blueprintparser.Result{Project: &composetypes.Project{},
			Extensions: blueprintparser.Extensions{Attachments: specs}}, true, nil, current,
	)
	attachCount := 0
	for _, change := range changes {
		if change.Resource == "attach" {
			attachCount++
			if change.Action != apiTypes.BlueprintChangeRetain {
				t.Fatalf("unchanged direct Attach preview = %#v", change)
			}
		}
	}
	if attachCount != 3 {
		t.Fatalf("preview contains %d Attaches, want 3", attachCount)
	}
	changed := make(map[string]core.AttachmentSpec, len(specs))
	for name, spec := range specs {
		changed[name] = spec
	}
	modified := changed["api-db"]
	modified.BackingService = "other"
	changed["api-db"] = modified
	if err := validateCurrentAttachmentDeclarations(changed, specs, current); err == nil {
		t.Fatal("changed direct Attach topology passed Validate")
	}
}
