package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
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
			Desired: core.Service{ID: backingServiceID},
		},
		Revision: 13,
	}
	preparation, err := PrepareEnvironmentBlueprintAttachTask(
		taskID,
		environmentID,
		[]EnvironmentBlueprintAttachCandidateInput{
			{Record: dependent, BackingProject: backingProject, BackingEnvironment: backingEnvironment, BackingService: backingService},
			{Record: owner, BackingProject: backingProject, BackingEnvironment: backingEnvironment, BackingService: backingService},
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
		EnvironmentComposeProjection{Services: []EnvironmentComposeIdentity{
			{ID: apiServiceID, Name: "api"},
			{ID: workerServiceID, Name: "worker"},
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
