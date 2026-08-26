package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseGroupPreparedMutationRejectsEscapedKeyspace(t *testing.T) {
	// Rationale: the child repository hands an opaque transaction fragment to
	// the Task publisher; a corrupt fragment must not gain write authority over
	// an unrelated durable capability through the atomic publication seam.
	t.Parallel()

	now := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
	groupID := ids.NewAt(ids.KindReleaseGroup, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	task := releaseGroupMutationValidationTask(now, groupID, TaskCreate)
	prepared := ReleaseGroupPreparedMutation{
		EnvironmentID: environmentID, GroupID: groupID, Type: TaskCreate,
		Mutations: []Mutation{{Type: MutationPut, Key: "/v1/records/secrets/sec_forbidden", Value: []byte("secret")}},
	}
	if err := validateReleaseGroupPreparedMutation(prepared, task); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("validateReleaseGroupPreparedMutation() error = %v, want internal boundary failure", err)
	}
}

func TestReleaseGroupRemovalPublicationCannotDeletePrimary(t *testing.T) {
	// Rationale: removal publication may create only the task and tombstone;
	// physical deletion belongs exclusively to terminal task acknowledgement.
	t.Parallel()

	now := time.Date(2026, 8, 26, 15, 10, 0, 0, time.UTC)
	groupID := ids.NewAt(ids.KindReleaseGroup, now, 1)
	prepared := ReleaseGroupPreparedMutation{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2), GroupID: groupID,
		GroupRevision: 17, Type: TaskRemove,
		Mutations: []Mutation{{Type: MutationDelete, Key: releaseGroupRecordKey(groupID)}},
	}
	err := validateReleaseGroupPreparedMutation(prepared, releaseGroupMutationValidationTask(now, groupID, TaskRemove))
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("validateReleaseGroupPreparedMutation() error = %v, want internal boundary failure", err)
	}
}

func TestReleaseGroupRemovalTaskRejectsOpenEndedParams(t *testing.T) {
	// Rationale: retry/finalization reconstructs authority from the durable Task;
	// accepting an extra parameter would create an unversioned execution input.
	t.Parallel()

	now := time.Date(2026, 8, 26, 15, 20, 0, 0, time.UTC)
	task := releaseGroupMutationValidationTask(now, ids.NewAt(ids.KindReleaseGroup, now, 1), TaskRemove)
	task.Params["unexpected"] = "value"
	if _, err := taskOwnsReleaseGroupRemoval(task); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskOwnsReleaseGroupRemoval() error = %v, want internal boundary failure", err)
	}
}

func releaseGroupMutationValidationTask(now time.Time, groupID string, taskType TaskType) TaskRecord {
	return TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 3), Executor: TaskExecutorController, Type: taskType,
		Target: groupID, Status: TaskStatusPending, Params: map[string]string{TaskResourceKindParam: TaskResourceReleaseGroup},
	}
}
