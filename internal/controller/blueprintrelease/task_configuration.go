package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *Service) prepareTaskConfiguration(ctx context.Context, input PrepareInput,
	candidates bool) (etcd.TaskRecord, error) {
	if !candidates || len(input.Task.Materializations) == 0 {
		return input.Task, nil
	}
	return service.ledger.PrepareTaskConfigurationAtRevision(ctx, input.Task, input.Environment.ReadRevision)
}

func taskStepRecords(steps []*agentpb.ExecutionStep) []etcd.TaskStepRecord {
	result := make([]etcd.TaskStepRecord, len(steps))
	for index, step := range steps {
		result[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId}
	}
	return result
}
