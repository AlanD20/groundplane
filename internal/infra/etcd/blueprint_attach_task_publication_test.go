package etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
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
	owner, err := NewPendingAttachRecord(
		ownerID, environmentID, "api-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, apiServiceID, ownerID, nil, nil, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(owner) error = %v", err)
	}
	dependent, err := NewPendingAttachRecord(
		dependentID, environmentID, "worker-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, workerServiceID, ownerID, nil, nil, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(dependent) error = %v", err)
	}
	backingProject := Versioned[ProjectRecord]{
		Record: ProjectRecord{ID: backingProjectID, Kind: ProjectKindBacking}, Revision: 11,
	}
	backingEnvironment := Versioned[EnvironmentRecord]{
		Record: EnvironmentRecord{ID: backingEnvironmentID, ProjectID: backingProjectID}, Revision: 12,
	}
	backingService := Versioned[ServiceRecord]{
		Record: ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired:         core.Service{ID: backingServiceID},
			desiredFenceKey: environmentBlueprintHeadKey(backingEnvironmentID),
		},
		Revision: 13,
	}
	preparation, err := PrepareEnvironmentBlueprintAttachTask(
		taskID,
		environmentID,
		[]EnvironmentBlueprintAttachCandidateInput{
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
	publication, err := prepareBlueprintAttachTaskPublication(
		Versioned[EnvironmentRecord]{Record: EnvironmentRecord{ID: environmentID}},
		EnvironmentComposeProjection{DesiredServices: []EnvironmentServiceProjection{
			{EnvironmentID: environmentID, Desired: core.Service{ID: apiServiceID, Name: "api"}},
			{EnvironmentID: environmentID, Desired: core.Service{ID: workerServiceID, Name: "worker"}},
		}},
		TaskRecord{ID: taskID, Target: environmentID},
		preparation,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskPublication() error = %v", err)
	}
	defer clearPreparedBlueprintAttachTaskPublication(publication)
	wantKeys := map[string]bool{
		blueprintAttachTaskIntentKey(taskID):               false,
		attachKey(ownerID):                                 false,
		attachKey(dependentID):                             false,
		attachCredentialByKey(ownerID, dependentID):        false,
		attachServiceKey(apiServiceID, ownerID):            false,
		attachServiceKey(workerServiceID, dependentID):     false,
		attachBackingServiceKey(backingServiceID, ownerID): false,
	}
	for _, mutation := range publication.mutations {
		if _, wanted := wantKeys[mutation.Key]; wanted {
			wantKeys[mutation.Key] = true
		}
	}
	for key, found := range wantKeys {
		if !found {
			t.Fatalf("publication omitted mutation %q", key)
		}
	}
	if err := validateIdempotencyPlanKeys(publication.conditions, publication.mutations); err != nil {
		t.Fatalf("publication is not a valid idempotency plan: %v", err)
	}
	values := make([]*KeyValue, len(publication.conditions))
	for index, condition := range publication.conditions {
		if condition.ModRevision > 0 {
			values[index] = &KeyValue{ModRevision: condition.ModRevision}
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
	inputs := make([]EnvironmentBlueprintAttachCandidateInput, 3)
	for index := range inputs {
		attachID := fmt.Sprintf("att_01ARZ3NDEKTSV4RRFFQ69G5FA%c", 'V'+index)
		inputs[index].Record = AttachRecord{ID: attachID}
	}
	if _, err := PrepareEnvironmentBlueprintAttachTask(
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
		mutate func(*EnvironmentBlueprintAttachCandidateInput)
	}{
		{name: "stale", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Revision = input.RetainedGrantTargets[0].ReadRevision + 1
		}},
		{name: "deleting", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.Status = core.AttachDetaching
			input.RetainedGrantTargets[0].Record.Operation = AttachOperationDetach
		}},
		{name: "non-ready", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.Status = core.AttachPending
		}},
		{name: "dependent", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.CredentialAttachID = "att_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
		}},
		{name: "cross-environment", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
		}},
		{name: "wrong-backing", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets[0].Record.BackingServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FZZ"
		}},
		{name: "missing", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
			input.RetainedGrantTargets = nil
		}},
		{name: "duplicate", mutate: func(input *EnvironmentBlueprintAttachCandidateInput) {
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
			input := EnvironmentBlueprintAttachCandidateInput{
				Record: record, Facts: &facts,
				BackingProject: scope.project, BackingEnvironment: scope.environment,
				BackingService: scope.service, RetainedGrantTargets: retained,
			}
			test.mutate(&input)
			before := fixture.store.revision
			if _, err := PrepareEnvironmentBlueprintAttachTask(
				task.ID, fixture.environment.Record.ID, []EnvironmentBlueprintAttachCandidateInput{input},
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
	dependent, err := NewPendingAttachRecord(
		dependentID,
		fixture.environment.Record.ID,
		"retained-owner-dependent",
		scope.project.Record.ID,
		scope.environment.Record.ID,
		scope.service.Record.Desired.ID,
		scope.service.Record.BackingNetworkID,
		projection.DesiredServices[0].Desired.ID,
		owner.Record.ID,
		nil,
		cloneAttachFactSets(owner.Record.FactSets),
		task.ID,
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(dependent) error = %v", err)
	}
	preparation, err := PrepareEnvironmentBlueprintAttachTask(
		task.ID,
		fixture.environment.Record.ID,
		[]EnvironmentBlueprintAttachCandidateInput{{
			Record: dependent, BackingProject: scope.project, BackingEnvironment: scope.environment,
			BackingService: scope.service, RetainedCredentialOwner: &owner,
		}},
		false,
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintAttachTask() error = %v", err)
	}
	publication, err := prepareBlueprintAttachTaskPublication(
		fixture.environment,
		projection,
		task,
		preparation,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskPublication() error = %v", err)
	}
	defer clearPreparedBlueprintAttachTaskPublication(publication)
	wantConditions := map[string]Condition{
		attachKey(owner.Record.ID): {
			Key:         attachKey(owner.Record.ID),
			ModRevision: owner.Revision,
		},
		deletionTombstoneKey("attach", owner.Record.ID):     {Key: deletionTombstoneKey("attach", owner.Record.ID)},
		attachCredentialByKey(owner.Record.ID, dependentID): {Key: attachCredentialByKey(owner.Record.ID, dependentID)},
	}
	for _, condition := range publication.conditions {
		if wanted, exists := wantConditions[condition.Key]; exists && condition == wanted {
			delete(wantConditions, condition.Key)
		}
	}
	if len(wantConditions) != 0 {
		t.Fatalf("retained owner conditions missing: %#v", wantConditions)
	}
	wantMutations := map[string]bool{
		attachKey(owner.Record.ID):                          false,
		attachCredentialByKey(owner.Record.ID, dependentID): false,
	}
	for _, mutation := range publication.mutations {
		if _, exists := wantMutations[mutation.Key]; exists && mutation.Type == MutationPut {
			wantMutations[mutation.Key] = true
		}
	}
	for key, found := range wantMutations {
		if !found {
			t.Fatalf("retained owner mutation %q is missing", key)
		}
	}
}

func publicationClassifierOnly(publication preparedBlueprintAttachTaskPublication) idempotencyPlanClassifier {
	return classifyEnvironmentBlueprintAttachPublication(
		func(_ int64, values []*KeyValue) error {
			if len(values) != 0 {
				return stateConflict("unexpected base evidence", "test")
			}
			return nil
		},
		publication,
	)
}
