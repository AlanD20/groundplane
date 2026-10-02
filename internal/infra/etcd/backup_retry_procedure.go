package etcd

import (
	"context"
	"encoding/hex"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *BackupRuntimeRepository) backupRetryProcedureInputs(ctx context.Context, source backupRunRetrySource,
	run backupruntime.BackupRunRecord,
) (*agentpb.BackupPlanScope, []*agentpb.BackupStepAuthority, etcdstore.Condition, error) {
	key := backupruntime.BackupExecutionPlanKey(source.task.Record.ID)
	read, err := repository.ReadFixedKeys(ctx, []string{key}, source.task.ReadRevision)
	if err != nil {
		return nil, nil, etcdstore.Condition{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil, etcdstore.Condition{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup retry has no sealed source procedure",
		)
	}
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[0].Value)
	if err != nil {
		return nil, nil, etcdstore.Condition{}, err
	}
	if plan.PlanId != source.task.Record.PlanID || hex.EncodeToString(plan.PlanHash) != source.task.Record.PlanHash ||
		len(plan.Steps) != len(source.run.Record.Sources) || plan.BackupScope.TaskAttempt == math.MaxUint32 {
		return nil, nil, etcdstore.Condition{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	scope := proto.Clone(plan.BackupScope).(*agentpb.BackupPlanScope)
	scope.TaskAttempt++
	steps := make([]*agentpb.BackupStepAuthority, 0, len(run.Sources))
	for _, attempt := range run.Sources {
		var original *agentpb.BackupStepAuthority
		for index, previous := range source.run.Record.Sources {
			if previous.SourceID == attempt.SourceID {
				original = plan.Steps[index].GetBackupStep()
				break
			}
		}
		if original == nil || original.GetCapture() == nil {
			return nil, nil, etcdstore.Condition{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		step := proto.Clone(original).(*agentpb.BackupStepAuthority)
		step.StepId, step.ExecutionId, step.StepDigest = ids.New(ids.KindStep), ids.NewULID(), nil
		step.StepDeadlineUnixNano = uint64(run.CreatedAt.Add(6 * time.Hour).UnixNano())
		step.GetCapture().PointId = attempt.RecoveryPointID
		step.GetCapture().Target.ObjectKey = attempt.ObjectKey
		sealed, err := executionplan.SealBackupStepAuthority(step)
		if err != nil {
			return nil, nil, etcdstore.Condition{}, err
		}
		steps = append(steps, sealed)
	}
	return scope, steps, etcdstore.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
