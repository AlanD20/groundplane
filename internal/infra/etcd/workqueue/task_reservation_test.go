package workqueue

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// Accepted actions already exist before their group graph is captured. This
// atomic-CAS model verifies promotion can reserve its environment without
// granting execution across a competing Task transition or resource owner.
func TestAcceptedTaskReservationPromotesWithoutLosingExclusion(t *testing.T) {
	for _, scenario := range []string{"promotion", "changed accepted Task", "existing reservation", "active claim"} {
		t.Run(scenario, func(t *testing.T) {
			store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
			now := time.Date(2026, 11, 17, 21, 0, 0, 0, time.UTC)
			intent := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
			accepted := TaskSnapshot{Revision: 1, Task: workexecution.Task{
				ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Action: workexecution.ActionDeploy,
				TargetID: ids.New(ids.KindReleaseGroup), TargetName: "application",
				Scope: workexecution.Scope{
					ProjectID:     ids.New(ids.KindProject),
					EnvironmentID: ids.New(ids.KindEnvironment),
				},
				AcceptedIntent: &intent, CreatedAt: now, UpdatedAt: now, Deadline: now.Add(time.Hour),
			}}
			raw, err := EncodeTask(accepted.Task)
			if err != nil {
				t.Fatal(err)
			}
			store.values[TaskKey(accepted.Task.ID)] = keyvalue.KeyValue{
				Key:         TaskKey(accepted.Task.ID),
				Value:       raw,
				ModRevision: 1,
			}
			prepared := accepted.Task
			prepared.Intent = workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)}
			prepared.ExpectedItems = 2
			claim := workexecution.Claim{ScopeID: prepared.Scope.EnvironmentID}
			change, err := PrepareAcceptedTaskReservation(accepted, prepared, claim)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(change.Mutations)
			promoted, err := EncodeTask(prepared)
			if err != nil {
				t.Fatal(err)
			}
			change.Mutations = append(
				change.Mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskKey(prepared.ID), Value: promoted},
			)
			switch scenario {
			case "changed accepted Task":
				store.values[TaskKey(prepared.ID)] = keyvalue.KeyValue{
					Key:         TaskKey(prepared.ID),
					Value:       raw,
					ModRevision: 2,
				}
			case "existing reservation":
				key := TaskReservationKey(claim)
				store.values[key] = keyvalue.KeyValue{Key: key, Value: []byte("another owner"), ModRevision: 1}
			case "active claim":
				key := ClaimKey(claim)
				store.values[key] = keyvalue.KeyValue{Key: key, Value: []byte("assigned work"), ModRevision: 1}
			}
			result, err := store.Transact(t.Context(), change.Conditions, change.Mutations)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "promotion" {
				if result.Succeeded || store.values[taskReservationIndexKey(prepared.ID)].ModRevision != 0 {
					t.Fatal("competing Task or resource state allowed a partial reservation")
				}
				return
			}
			if !result.Succeeded {
				t.Fatal("an accepted action could not atomically promote and reserve its environment")
			}
			owner, err := recordcodec.Decode[TaskReservation](
				store.values[TaskReservationKey(claim)].Value,
				"execution_task_reservation",
			)
			if err != nil || owner.TaskID != prepared.ID || owner.Intent != prepared.Intent || owner.Claim != claim {
				t.Fatalf("promotion did not preserve captured reservation ownership: %+v/%v", owner, err)
			}
			other := prepared
			other.ID = ids.New(ids.KindTask)
			if _, err := taskReservationConditions(other, workexecution.Item{TaskID: other.ID}, []TaskReservationSnapshot{
				{Claim: claim, Owner: &owner, Revision: result.Revision},
			}); err == nil {
				t.Fatal("another Task could enter the reserved environment")
			}
		})
	}
}
