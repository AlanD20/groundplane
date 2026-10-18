package attachplan

import (
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters/mysql"
	"github.com/AlanD20/groundplane/internal/adapters/postgres"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

var nativeDatabaseAdapters sync.Once

// ATT-08/10: captured grants follow credential creation and precede deletion;
// a resolver cannot silently substitute another binding's grant.
func TestNativeAttachPinsGrantIdentityAndCleanupOrder(t *testing.T) {
	nativeDatabaseAdapters.Do(func() {
		postgres.Register()
		mysql.Register()
	})
	for _, database := range []struct{ family, version, image string }{
		{"postgres", "16", "postgres:16-alpine"},
		{"mysql", "8.4", "mysql:8.4"},
	} {
		t.Run(database.family, func(t *testing.T) {
			checkNativeGrantDelivery(t, database.family, database.version, database.image)
		})
	}
}

func checkNativeGrantDelivery(t *testing.T, family, version, image string) {
	t.Helper()
	fixture := newNativeAuthenticationFixture(t, core.BackingAuthenticationUsernamePassword)
	generation := &fixture.generation
	generation.AdapterKey = family
	generation.Authentication, fixture.facts.mode = "", ""
	grantID := ids.New(ids.KindAttach)
	owner := &generation.Provision.Candidate
	owner.GrantAttachIDs = []string{grantID}
	grant, err := attachments.NewPendingAttachRecord(grantID, owner.EnvironmentID, "reporting",
		owner.BackingProjectID, owner.BackingEnvironmentID, owner.BackingServiceID, owner.BackingNetworkID,
		owner.ServiceID, grantID, nil, owner.FactSets, owner.TaskID, owner.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	generation.FactSets = append(generation.FactSets, attachments.FactSetMetadata{GrantAttachID: grantID,
		Facts: []attachments.FactDefinition{{Key: "reporting_DATABASE"}}})
	owner.FactSets = generation.FactSets
	generation.Provision.Credential.Record = *owner
	generation.Provision.Grants = []blueprintattachinputs.BindingRecord{{Record: grant}}
	fixture.facts.grants = []GrantIdentity{{AttachID: grantID, Database: "reporting_db"}}
	reference, err := blueprintattachinputs.WorkInput(*generation)
	if err != nil {
		t.Fatal(err)
	}
	fixture.item.Input, fixture.task.Intent = &reference, reference
	planner, err := NewProvisionPlanner(fixture.facts)
	if err != nil {
		t.Fatal(err)
	}
	input, err := planner.BuildCaptured(t.Context(), *generation, fixture.task, fixture.item)
	if err != nil {
		t.Fatal(err)
	}
	defer attachexecution.ClearProvision(input)
	commands, err := attachexecution.ProvisionProcedures(input)
	if err != nil || len(commands) != 2 ||
		commands[0].Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION ||
		commands[0].Authentication != agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD ||
		commands[1].Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT ||
		commands[1].GrantOn != "reporting_db" ||
		len(commands[1].Password) != 0 {
		t.Fatalf("credential/grant sequence = %v, %v", commands, err)
	}
	clear(commands[0].Password)
	fixture.removal.Attach.GrantAttachIDs, fixture.removal.Attach.FactSets = []string{grantID}, generation.FactSets
	backing := &fixture.removal.Backing.Desired
	backing.Adapter, backing.AdapterVersion, backing.Image = family, version, image
	backing.Authentication = ""
	removal, err := NewRemovalPlanner(fixture.facts)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := removal.Build(t.Context(), fixture.removal, input.CapturedSource, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer attachexecution.ClearRemoval(cleanup)
	commands, err = attachexecution.CredentialProcedures(cleanup)
	if err != nil || len(commands) != 2 ||
		commands[0].Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE ||
		commands[0].GrantOn != "reporting_db" ||
		commands[1].Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH ||
		len(commands[1].Password) != 0 {
		t.Fatalf("grant/credential removal sequence = %v, %v", commands, err)
	}
	fixture.facts.grants[0].AttachID = ids.New(ids.KindAttach)
	if delivery, err := planner.BuildCaptured(t.Context(), *generation, fixture.task, fixture.item); err == nil ||
		delivery != nil {
		t.Fatal("substituted grant identity acquired provisioning authority")
	}
}
