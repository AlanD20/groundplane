package controller

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a durable Attach task must rebuild the same sealed typed identity
// procedure without persisting its plaintext password in Task parameters.
func TestTaskPlanResolverBuildsAttachProcedureFromEncryptedIdentity(t *testing.T) {
	postgres16.Register()
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	attachID := ids.NewAt(ids.KindAttach, now, 1)
	grantID := ids.NewAt(ids.KindAttach, now, 2)
	taskID := ids.NewAt(ids.KindTask, now, 3)
	backingServiceID := ids.NewAt(ids.KindService, now, 4)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 5)
	record, err := etcd.NewPendingAttachRecord(
		attachID, ids.NewAt(ids.KindEnvironment, now, 6), "api-db", ids.NewAt(ids.KindProject, now, 7),
		backingEnvironmentID, backingServiceID, []string{ids.NewAt(ids.KindService, now, 8)},
		[]string{grantID}, []etcd.AttachFactSetMetadata{
			{Facts: []etcd.AttachFactDefinition{{Key: "pg16_DATABASE"}}},
			{GrantAttachID: grantID, Facts: []etcd.AttachFactDefinition{{Key: "pg16_DATABASE"}}},
		}, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	grant := record
	grant.ID = grantID
	grant.GrantAttachIDs = nil
	grant.Status = core.AttachReady
	grant.TaskID = ids.NewAt(ids.KindTask, now, 9)
	service := etcd.ServiceRecord{
		EnvironmentID: backingEnvironmentID,
		Desired:       core.Service{ID: backingServiceID, Name: "postgres", Adapter: "postgres:16"},
		Runtime: core.ServiceRuntime{
			ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	state := &attachPlanTestState{
		attaches: map[string]etcd.Versioned[etcd.AttachRecord]{
			attachID: {Record: record, Revision: 1, ReadRevision: 1},
			grantID:  {Record: grant, Revision: 2, ReadRevision: 2},
		},
		service: etcd.Versioned[etcd.ServiceRecord]{Record: service, Revision: 3, ReadRevision: 3},
		identity: AttachPlanIdentity{
			Database: "api_5d3f9a", Role: "api_5d3f9a", Password: []byte("URL_safe-1"),
			Grants: []AttachPlanGrantIdentity{{AttachID: grantID, Database: "other_4a1b2c"}},
		},
	}
	resolver, err := NewTaskPlanResolver("/var/lib/groundplane/vol")
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	resolver.attaches, resolver.services, resolver.attachIdentities = state, state, state
	task := etcd.TaskRecord{
		ID: taskID, Executor: etcd.TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, now, 10),
		RenderGeneration: 1, Type: etcd.TaskAttach, Target: attachID, TimeoutSeconds: 60,
		Steps: []etcd.TaskStepRecord{
			{ID: ids.NewAt(ids.KindStep, now, 11)}, {ID: ids.NewAt(ids.KindStep, now, 12)},
		},
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH || len(plan.Steps) != 2 ||
		plan.Steps[0].GetAdapterProcedure().Phase !=
			agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION ||
		string(plan.Steps[0].GetAdapterProcedure().Password) != "URL_safe-1" ||
		plan.Steps[1].GetAdapterProcedure().GrantOn != "other_4a1b2c" {
		t.Fatalf("ResolveExecutionPlan() = %#v", plan)
	}
}

type attachPlanTestState struct {
	attaches map[string]etcd.Versioned[etcd.AttachRecord]
	service  etcd.Versioned[etcd.ServiceRecord]
	identity AttachPlanIdentity
}

func (state *attachPlanTestState) GetAttach(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.AttachRecord], error) {
	return state.attaches[id], nil
}

func (state *attachPlanTestState) GetService(
	context.Context,
	string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return state.service, nil
}

func (state *attachPlanTestState) ResolveTaskIdentity(
	_ context.Context,
	_ etcd.Versioned[etcd.AttachRecord],
	_ string,
	consume AttachPlanIdentityConsumer,
) error {
	identity := state.identity
	identity.Password = append([]byte(nil), state.identity.Password...)
	identity.Grants = append([]AttachPlanGrantIdentity(nil), state.identity.Grants...)
	defer identity.Clear()
	return consume(identity)
}
