package taskplanning

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"

	testtaskcontract "github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestTaskPlanResolverRebuildsExactEnvironmentCreatePlan(t *testing.T) {
	resolver, err := NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	task := environmentCreateTaskForPlanTest()
	first, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	task.PlanHash = hex.EncodeToString(first.PlanHash)
	second, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	if !bytes.Equal(first.PlanHash, second.PlanHash) || len(first.Artifacts) != 0 || len(first.Steps) != 1 ||
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
		first.Steps[0].GetEnvironmentDirectoryCreate().ExpectedVolumeDir != task.Params[testtaskcontract.EnvironmentCreateVolumeDirectoryParam] {
		t.Fatalf("resolved plans = %#v / %#v", first, second)
	}
}

func TestTaskPlanResolverRejectsExtraDurableParameter(t *testing.T) {
	resolver, err := NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	task := environmentCreateTaskForPlanTest()
	task.Params["other"] = "confused"
	if _, err := resolver.ResolveExecutionPlan(
		context.Background(),
		task,
	); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("ResolveExecutionPlan(extra param) error = %v, want internal", err)
	}
}

func environmentCreateTaskForPlanTest() etcd.TaskRecord {
	return etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Executor: testtaskjournal.TaskExecutorAgent,
		PlanID:   "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Type: testtaskjournal.TaskCreate, Target: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Params: map[string]string{
			testtaskcontract.EnvironmentCreateVolumeDirectoryParam: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending,
	}
}
