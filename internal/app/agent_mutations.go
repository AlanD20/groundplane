package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type agentEnrollmentMutation interface {
	EnrollAgent(context.Context, string) (etcd.IdempotencyResponse, error)
}

type agentRemovalMutation interface {
	RemoveAgent(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type agentUpdateMutation interface {
	UpdateAgent(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type agentMutationService struct {
	enrollments agentEnrollmentMutation
	updates     agentUpdateMutation
	removals    agentRemovalMutation
}

func newAgentMutationService(
	enrollments agentEnrollmentMutation,
	updates agentUpdateMutation,
	removals agentRemovalMutation,
) (*agentMutationService, error) {
	if enrollments == nil || updates == nil || removals == nil {
		return nil, errs.New(errs.KindInternal, "Agent mutation services are incomplete")
	}
	return &agentMutationService{enrollments: enrollments, updates: updates, removals: removals}, nil
}

func (service *agentMutationService) EnrollAgent(
	ctx context.Context,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.enrollments.EnrollAgent(ctx, idempotencyKey)
}

func (service *agentMutationService) RemoveAgent(
	ctx context.Context,
	agentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.removals.RemoveAgent(ctx, agentID, idempotencyKey)
}

func (service *agentMutationService) UpdateAgent(
	ctx context.Context,
	agentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.updates.UpdateAgent(ctx, agentID, idempotencyKey)
}
