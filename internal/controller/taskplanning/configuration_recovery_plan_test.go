package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskcontract "github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	testtaskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"

	// SVC-15/JOURNEY-02: new destinations restore absence, with a closed probe /
	// compensation pair that reconstructs the exact durable suffix. Foreign suffix
	// IDs must not grant file writes after reconnect.
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestConfigurationRecoveryPlanSealsInitialAbsenceAndExactSuffix(t *testing.T) {
	t.Parallel()
	_, task := blueprintPlanTestState(t)
	resolver, err := NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatal(err)
	}
	configureInitialFileRecoveryFixture(t, resolver, &task)
	procedure, steps, err := resolver.configurationRecoverySteps(
		context.Background(),
		task,
		task.Params[testtaskcontract.EnvironmentBlueprintArtifactParam],
	)
	if err != nil || len(steps) != 2 || len(procedure.GetFiles()) != 1 {
		t.Fatalf("file plan: %v", err)
	}
	if procedure.PriorSnapshotId != "" || len(procedure.PriorSnapshotSha256) != 0 ||
		steps[0].GetMaterializeFile().OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV ||
		steps[1].GetMaterializeFile().OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV ||
		steps[0].Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		steps[1].Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE ||
		steps[1].PrerequisiteStepId != task.Materializations[0].StepID {
		t.Fatal("new path did not seal exact absence and forward write authority")
	}
	start := len(task.Steps)
	for _, step := range steps {
		task.Steps = append(
			task.Steps,
			testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId},
		)
	}
	if err := resolver.configurationRecoverySuffix(context.Background(), task, start); err != nil {
		t.Fatal(err)
	}
	task.Steps[start].ID = ids.New(ids.KindStep)
	if err := resolver.configurationRecoverySuffix(context.Background(), task, start); err == nil {
		t.Fatal("foreign recovery suffix accepted")
	}
}

func configureInitialFileRecoveryFixture(t *testing.T, resolver *TaskPlanResolver, task *etcd.TaskRecord) {
	t.Helper()
	task.Owner.EnvironmentID = task.Target
	digest := sha256.Sum256(nil)
	task.Configuration = &testtaskconfiguration.TaskConfiguration{Current: runtimeconfiguration.Reference{
		ID: ids.New(ids.KindConfig), EnvironmentID: task.Target, Generation: uint64(task.RenderGeneration),
		SHA256: hex.EncodeToString(digest[:]),
	}}
	if err := resolver.EnableConfigurationRecovery(initialConfigurationReader{}); err != nil {
		t.Fatal(err)
	}
}

type initialConfigurationReader struct{}

func (initialConfigurationReader) LoadRetained(
	context.Context,
	runtimeconfiguration.Reference,
) (runtimeconfiguration.Snapshot, error) {
	return runtimeconfiguration.Snapshot{}, errs.New(errs.KindInternal, "initial absence must not load a predecessor")
}
