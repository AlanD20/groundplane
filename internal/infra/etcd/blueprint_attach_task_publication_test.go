package etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBlueprintAttachPublicationCarriesCandidateSetAndBackingFences(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	const (
		taskID               = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID        = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		backingProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		backingEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		backingServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		backingNetworkID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		ownerID              = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		dependentID          = "att_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		apiServiceID         = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		workerServiceID      = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	)
	owner, err := testattachments.NewPendingAttachRecord(
		ownerID, environmentID, "api-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, apiServiceID, ownerID, nil, nil, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(owner) error = %v", err)
	}
	dependent, err := testattachments.NewPendingAttachRecord(
		dependentID, environmentID, "worker-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, workerServiceID, ownerID, nil, nil, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(dependent) error = %v", err)
	}
	backingProject := testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: testhierarchy.ProjectRecord{ID: backingProjectID, Kind: testhierarchy.ProjectKindBacking}, Revision: 11,
	}
	backingEnvironment := testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record: testhierarchy.EnvironmentRecord{ID: backingEnvironmentID, ProjectID: backingProjectID}, Revision: 12,
	}
	backingService := selectedServiceFixture(t, backingEnvironmentID, backingServiceID, backingNetworkID, 13)
	preparation, err := testblueprintplanning.PrepareEnvironmentBlueprintAttachTask(
		taskID,
		environmentID,
		[]testblueprintplanning.EnvironmentBlueprintAttachCandidateInput{
			{
				Record:             dependent,
				BackingProject:     backingProject,
				BackingEnvironment: backingEnvironment,
				BackingService:     backingService,
			},
			{
				Record:             owner,
				BackingProject:     backingProject,
				BackingEnvironment: backingEnvironment,
				BackingService:     backingService,
			},
		},
		true,
		now,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintAttachTask() error = %v", err)
	}
	if got := preparation.Intent.Candidates[0].ID; got != ownerID {
		t.Fatalf("first candidate = %q, want owner %q", got, ownerID)
	}
	publication, err := testblueprintplanning.PrepareBlueprintAttachTaskPublication(
		testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		testenvironmentprojection.EnvironmentComposeProjection{
			DesiredServices: []testservices.EnvironmentServiceProjection{
				{EnvironmentID: environmentID, Desired: core.Service{ID: apiServiceID, Name: "api"}},
				{EnvironmentID: environmentID, Desired: core.Service{ID: workerServiceID, Name: "worker"}},
			},
		},
		testblueprintplanning.TaskIdentity{ID: taskID, Target: environmentID},
		preparation,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskPublication() error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintAttachTaskPublication(publication)
	wantKeys := map[string]bool{
		testattachments.BlueprintAttachTaskIntentKey(taskID):               false,
		testattachments.AttachKey(ownerID):                                 false,
		testattachments.AttachKey(dependentID):                             false,
		testattachments.AttachCredentialByKey(ownerID, dependentID):        false,
		testattachments.AttachServiceKey(apiServiceID, ownerID):            false,
		testattachments.AttachServiceKey(workerServiceID, dependentID):     false,
		testattachments.AttachBackingServiceKey(backingServiceID, ownerID): false,
	}
	for _, mutation := range publication.Mutations() {
		if _, wanted := wantKeys[mutation.Key]; wanted {
			wantKeys[mutation.Key] = true
		}
	}
	for key, found := range wantKeys {
		if !found {
			t.Fatalf("publication omitted mutation %q", key)
		}
	}
	if err := validateIdempotencyPlanKeys(publication.Conditions(), publication.Mutations()); err != nil {
		t.Fatalf("publication is not a valid idempotency plan: %v", err)
	}
	values := make([]*testkeyvalue.KeyValue, len(publication.Conditions()))
	for index, condition := range publication.Conditions() {
		if condition.ModRevision > 0 {
			values[index] = &testkeyvalue.KeyValue{ModRevision: condition.ModRevision}
		}
	}
	if err := publicationClassifierOnly(publication)(17, values); err != nil {
		t.Fatalf("publication classifier rejected exact backing revisions: %v", err)
	}
}

// Rationale: only newly introduced candidate Attaches consume the atomic
// Blueprint publication budget; the third candidate must be rejected early.
func TestBlueprintAttachPreparationRejectsThirdNewCandidate(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	inputs := make([]testblueprintplanning.EnvironmentBlueprintAttachCandidateInput, 3)
	for index := range inputs {
		attachID := fmt.Sprintf("att_01ARZ3NDEKTSV4RRFFQ69G5FA%c", 'V'+index)
		inputs[index].Record = testattachments.Record{ID: attachID}
	}
	if _, err := testblueprintplanning.PrepareEnvironmentBlueprintAttachTask(
		"task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		inputs, true, now,
	); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("PrepareEnvironmentBlueprintAttachTask(three) error = %v, want validation", err)
	}
}

func TestBlueprintAttachPreparationRequiresExactReadyRetainedGrantEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testblueprintplanning.EnvironmentBlueprintAttachCandidateInput)
	}{
		{name: "stale", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Revision = input.RetainedGrantTargets[0].ReadRevision + 1
		}},
		{name: "deleting", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.Status = core.AttachDetaching
			input.RetainedGrantTargets[0].Record.Operation = testattachments.AttachOperationDetach
		}},
		{name: "non-ready", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.Status = core.AttachPending
		}},
		{name: "dependent", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.CredentialAttachID = "att_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
		}},
		{
			name: "cross-environment",
			mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
				input.RetainedGrantTargets[0].Record.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
			},
		},
		{name: "wrong-backing", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.BackingServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
		}},
		{name: "missing", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets = nil
		}},
		{name: "duplicate", mutate: func(input *testblueprintplanning.EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets = append(input.RetainedGrantTargets, input.RetainedGrantTargets[0])
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, false)
			task := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 8100)
			projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, task, 1)
			scope := seedEnvironmentBlueprintBackingScope(t, fixture, 8200)
			record, facts, retained := environmentBlueprintMaximumAttachCandidate(
				t, fixture, task, scope, projection.DesiredServices[0].Desired.ID, 8300,
			)
			readRevision := fixture.store.revision
			for index := range retained {
				retained[index].ReadRevision = readRevision
			}
			input := testblueprintplanning.EnvironmentBlueprintAttachCandidateInput{
				Record: record, Facts: &facts,
				BackingProject: scope.project, BackingEnvironment: scope.environment,
				BackingService: scope.service, RetainedGrantTargets: retained,
			}
			test.mutate(&input)
			before := fixture.store.revision
			if _, err := testblueprintplanning.PrepareEnvironmentBlueprintAttachTask(
				task.ID, fixture.environment.Record.ID, []testblueprintplanning.EnvironmentBlueprintAttachCandidateInput{input},
				false, task.CreatedAt,
			); !isKind(err, errs.KindValidationFailed) {
				t.Fatalf("PrepareEnvironmentBlueprintAttachTask() error = %v, want validation", err)
			}
			if fixture.store.revision != before {
				t.Fatalf(
					"rejected retained evidence wrote at revision %d, started at %d",
					fixture.store.revision,
					before,
				)
			}
			clear(facts.Ciphertext)
		})
	}
}

func TestBlueprintAttachPublicationUsesRetainedCredentialOwnerEvidence(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	task := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 8400)
	projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, task, 1)
	scope := seedEnvironmentBlueprintBackingScope(t, fixture, 8500)
	owner := environmentBlueprintRetainedGrantTarget(
		t, fixture, scope, projection.DesiredServices[0].Desired.ID, 8600,
	)
	owner.ReadRevision = fixture.store.revision
	dependentID := ids.NewAt(ids.KindAttach, fixture.now, 8700)
	dependent, err := testattachments.NewPendingAttachRecord(
		dependentID,
		fixture.environment.Record.ID,
		"retained-owner-dependent",
		scope.project.Record.ID,
		scope.environment.Record.ID,
		scope.service.Record.Desired.ID,
		scope.service.Record.BackingNetworkID,
		projection.DesiredServices[0].Desired.ID,
		owner.Record.ID,
		nil, testattachments.CloneAttachFactSets(owner.Record.FactSets), task.ID,
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(dependent) error = %v", err)
	}
	preparation, err := testblueprintplanning.PrepareEnvironmentBlueprintAttachTask(
		task.ID,
		fixture.environment.Record.ID,
		[]testblueprintplanning.EnvironmentBlueprintAttachCandidateInput{{
			Record: dependent, BackingProject: scope.project, BackingEnvironment: scope.environment,
			BackingService: scope.service, RetainedCredentialOwner: &owner,
		}},
		false,
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintAttachTask() error = %v", err)
	}
	publication, err := testblueprintplanning.PrepareBlueprintAttachTaskPublication(
		fixture.environment,
		projection,
		testblueprintplanning.TaskIdentity{ID: task.ID, Target: task.Target},
		preparation,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskPublication() error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintAttachTaskPublication(publication)
	wantConditions := map[string]testkeyvalue.Condition{testattachments.AttachKey(owner.Record.ID): {
		Key:         testattachments.AttachKey(owner.Record.ID),
		ModRevision: owner.Revision,
	}, testdeletions.TombstoneKey("attach", owner.Record.ID): {Key: testdeletions.TombstoneKey("attach", owner.Record.ID)}, testattachments.AttachCredentialByKey(owner.Record.ID, dependentID): {Key: testattachments.AttachCredentialByKey(owner.Record.ID, dependentID)},
	}
	for _, condition := range publication.Conditions() {
		if wanted, exists := wantConditions[condition.Key]; exists && condition == wanted {
			delete(wantConditions, condition.Key)
		}
	}
	if len(wantConditions) != 0 {
		t.Fatalf("retained owner conditions missing: %#v", wantConditions)
	}
	wantMutations := map[string]bool{
		testattachments.AttachKey(owner.Record.ID):                          false,
		testattachments.AttachCredentialByKey(owner.Record.ID, dependentID): false,
	}
	for _, mutation := range publication.Mutations() {
		if _, exists := wantMutations[mutation.Key]; exists && mutation.Type == testkeyvalue.MutationPut {
			wantMutations[mutation.Key] = true
		}
	}
	for key, found := range wantMutations {
		if !found {
			t.Fatalf("retained owner mutation %q is missing", key)
		}
	}
}

func publicationClassifierOnly(publication testblueprintplanning.AttachPublication) idempotencyPlanClassifier {
	return testblueprintplanning.ClassifyEnvironmentBlueprintAttachPublication(
		func(_ int64, values []*testkeyvalue.KeyValue) error {
			if len(values) != 0 {
				return testrecordcodec.StateConflict("unexpected base evidence", "test")
			}
			return nil
		},
		publication,
	)
}
