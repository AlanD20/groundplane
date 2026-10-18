package attachplan

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/valkey"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
)

var nativeAuthAdapter sync.Once

type nativeAuthenticationFixture struct {
	generation blueprintattachinputs.Generation
	task       workexecution.Task
	item       workexecution.Item
	removal    attachremoval.Input
	facts      *nativeAuthenticationFacts
}

func newNativeAuthenticationFixture(t *testing.T, mode core.BackingAuthentication) nativeAuthenticationFixture {
	t.Helper()
	nativeAuthAdapter.Do(valkey.Register)
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	taskID, attachID := ids.New(ids.KindTask), ids.New(ids.KindAttach)
	environment, serviceID := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	backingProject, backingEnv := ids.New(ids.KindProject), ids.New(ids.KindEnvironment)
	backingService, network := ids.New(ids.KindService), ids.New(ids.KindNetwork)
	var sets []attachments.FactSetMetadata
	if mode != core.BackingAuthenticationNone {
		sets = []attachments.FactSetMetadata{
			{Facts: []attachments.FactDefinition{{Key: "cache_PASSWORD", Secret: true}}},
		}
	}
	record, err := attachments.NewPendingAttachRecord(attachID, environment, "cache", backingProject,
		backingEnv, backingService, network, serviceID, attachID, nil, sets, taskID, at)
	if err != nil {
		t.Fatal(err)
	}
	operation := ids.New(ids.KindOperation)
	generation := blueprintattachinputs.Generation{
		ID: operation, OperationID: operation, EnvironmentID: environment, ParentTaskID: taskID, RevisionID: taskID,
		AttachID: attachID, AttachName: "cache", AuthoredSpecSHA256: strings.Repeat("b", 64),
		ConsumerServiceID: serviceID, CredentialOwnerID: attachID, BackingProjectID: backingProject,
		BackingEnvironmentID: backingEnv, BackingServiceID: backingService, BackingNetworkID: network,
		AdapterKey: "valkey", Authentication: mode, FactSets: sets, WorkItemID: ids.New(ids.KindWorkItem),
		Provision: &blueprintattachinputs.Provision{RequiresProvisioning: mode != core.BackingAuthenticationNone,
			Candidate: record, Credential: blueprintattachinputs.BindingRecord{Record: record}},
	}
	if len(sets) != 0 {
		// The fact port below substitutes decryption; generation validation still
		// checks the real envelope digest and captured owner before opening it.
		generation.GeneratedInputs, err = attachments.NewAttachEncryptedFacts(
			attachID,
			1,
			"age-x25519",
			"sha256",
			[]byte("opaque-test-envelope"),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	reference, err := blueprintattachinputs.WorkInput(generation)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionAttach,
		TargetID: attachID, Intent: reference, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute), ExpectedItems: 1,
		Scope: workexecution.Scope{
			TenantID:      ids.New(ids.KindTenant),
			ProjectID:     ids.New(ids.KindProject),
			EnvironmentID: environment,
		}}
	host := ids.New(ids.KindHost)
	item := workexecution.Item{ID: generation.WorkItemID, TaskID: taskID, Operation: workexecution.Attach,
		TargetID: attachID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: host},
		Claims: []workexecution.Claim{{HostID: host, ScopeID: attachID}}, Input: &reference,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	backing, err := services.NewServiceRecord(backingEnv, core.Service{ID: backingService, Name: "cache",
		Image: "valkey/valkey:9-alpine", Adapter: "valkey", AdapterVersion: "9", Authentication: mode}, network)
	if err != nil {
		t.Fatal(err)
	}
	source := attachremoval.Input{Schema: 1, SnapshotRevision: 10, Attach: record, AttachRevision: 8,
		Backing: backing, BackingRevision: 7, BackingDesiredRevision: 6,
		TenantID: task.Scope.TenantID, ProjectID: task.Scope.ProjectID}
	source.Attach.Status = core.AttachReady
	if len(sets) != 0 {
		envelope := generation.GeneratedInputs
		source.Facts, source.FactsRevision = &envelope, 9
	}
	if err := attachremoval.Validate(source); err != nil {
		t.Fatal(err)
	}
	return nativeAuthenticationFixture{generation: generation, task: task, item: item, removal: source,
		facts: &nativeAuthenticationFacts{t: t, mode: mode}}
}

type nativeAuthenticationFacts struct {
	t          *testing.T
	mode       core.BackingAuthentication
	wrongMode  bool
	grants     []GrantIdentity
	provisions int
	removals   int
}

func (facts *nativeAuthenticationFacts) consume(consume IdentityConsumer) error {
	mode := facts.mode
	if facts.wrongMode {
		mode = core.BackingAuthenticationUsernamePassword
	}
	identity := Identity{Authentication: mode, Role: "default", Password: []byte("captured-password")}
	if facts.mode == core.BackingAuthenticationUsernamePassword || facts.mode == "" {
		identity.Role, identity.Database = "app_role", "app_db"
	}
	identity.Grants = append([]GrantIdentity(nil), facts.grants...)
	defer identity.Clear()
	return consume(identity)
}

func (facts *nativeAuthenticationFacts) ResolveCapturedProvisionIdentity(
	_ context.Context,
	_ blueprintattachinputs.Generation,
	consume IdentityConsumer,
) error {
	facts.provisions++
	return facts.consume(consume)
}

func (facts *nativeAuthenticationFacts) ResolveDeletionIdentity(
	_ context.Context,
	_ attachremoval.Input,
	consume IdentityConsumer,
) error {
	facts.removals++
	return facts.consume(consume)
}

func (facts *nativeAuthenticationFacts) ResolveCapturedProvisionHook(
	context.Context,
	blueprintattachinputs.Generation,
	workexecution.Scope,
	HookInputConsumer,
) error {
	facts.t.Fatal("credential Attach attempted Custom hook resolution")
	return nil
}

func (facts *nativeAuthenticationFacts) ResolveCapturedDetachHook(
	context.Context,
	attachremoval.Input,
	HookInputConsumer,
) error {
	facts.t.Fatal("credential Detach attempted Custom hook resolution")
	return nil
}

func (facts *nativeAuthenticationFacts) SealBackingHookInputsAtRevision(
	context.Context,
	string,
	string,
	backinghook.Configuration,
	int64,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	facts.t.Fatal("credential Detach attempted Custom hook capture")
	return nil, nil
}
