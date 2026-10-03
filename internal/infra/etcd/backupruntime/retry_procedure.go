package backupruntime

import (
	"context"
	"encoding/hex"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupRetryProcedureSource struct {
	TaskID       string
	PlanID       string
	PlanHash     string
	ReadRevision int64
	Run          BackupRunRecord
}

func (reader *Reader) PrepareBackupRetryProcedureInputs(ctx context.Context,
	source BackupRetryProcedureSource, run BackupRunRecord,
) (*agentpb.BackupPlanScope, []*agentpb.BackupStepAuthority, etcdstore.Condition, error) {
	key := BackupExecutionPlanKey(source.TaskID)
	read, err := reader.ReadFixedKeys(ctx, []string{key}, source.ReadRevision)
	if err != nil {
		return nil, nil, etcdstore.Condition{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil, etcdstore.Condition{}, errs.New(errs.KindTaskNotRetryable, "backup retry has no sealed source procedure")
	}
	plan, err := DecodeBackupExecutionPlan(read.Values[0].Value)
	if err != nil {
		return nil, nil, etcdstore.Condition{}, err
	}
	if plan.PlanId != source.PlanID || hex.EncodeToString(plan.PlanHash) != source.PlanHash ||
		len(plan.Steps) != len(source.Run.Sources) || plan.BackupScope.TaskAttempt == math.MaxUint32 {
		return nil, nil, etcdstore.Condition{}, CorruptBackupRuntimeRecord()
	}
	scope := proto.CloneOf(plan.BackupScope)
	scope.TaskAttempt++
	steps := make([]*agentpb.BackupStepAuthority, 0, len(run.Sources))
	for _, attempt := range run.Sources {
		var original *agentpb.BackupStepAuthority
		for index, previous := range source.Run.Sources {
			if previous.SourceID == attempt.SourceID {
				original = plan.Steps[index].GetBackupStep()
				break
			}
		}
		if original == nil || original.GetCapture() == nil {
			return nil, nil, etcdstore.Condition{}, CorruptBackupRuntimeRecord()
		}
		step := proto.CloneOf(original)
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
