package entry

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumEntryDeletionAttempts = 3

type RemovalService struct {
	repository Repository
	plans      Planner
	now        func() time.Time
}

func NewRemovalService(repository Repository, plans Planner) (*RemovalService, error) {
	if repository == nil || plans == nil {
		return nil, errs.New(errs.KindInternal, "entry deletion service is not configured")
	}
	return &RemovalService{repository: repository, plans: plans, now: time.Now}, nil
}

func (service *RemovalService) RemoveEntry(
	ctx context.Context,
	request RemoveRequest,
) (RemovalOutcome, error) {
	if ctx == nil {
		return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion context is required")
	}
	if ids.Validate(ids.KindEnvEntry, request.EntryID) != nil {
		return RemovalOutcome{}, errs.New(
			errs.KindValidationFailed,
			"entry deletion requires a stable Entry id",
		)
	}
	for attempt := 0; attempt < maximumEntryDeletionAttempts; attempt++ {
		outcome, err := service.removeEntryOnce(ctx, request)
		if err == nil {
			return outcome, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryDeletionAttempts-1 {
			return RemovalOutcome{}, err
		}
	}
	return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion retry bound was not enforced")
}

func (service *RemovalService) removeEntryOnce(
	ctx context.Context,
	request RemoveRequest,
) (RemovalOutcome, error) {
	inspection, err := service.repository.InspectRemoval(ctx, request)
	if err != nil {
		return RemovalOutcome{}, err
	}
	if inspection.Replay != nil {
		return *inspection.Replay, nil
	}
	candidate := inspection.Candidate
	if err := validateRemovalCandidate(candidate, request.EntryID); err != nil {
		return RemovalOutcome{}, err
	}
	now := service.now().UTC()
	task := RemovalTask{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		PlanID: ids.New(ids.KindPlan), CreatedAt: now,
	}
	planRequest := RemovalPlanRequest{
		TaskID: task.ID, PlanID: task.PlanID, EntryID: candidate.EntryID,
		EnvironmentID: candidate.Identity.EnvironmentID, EntryRevision: candidate.EntryRevision,
		ProjectionRevision: candidate.ProjectionRevision, CreatedAt: now,
		ArtifactID: ids.New(ids.KindConfig), Identity: candidate.Identity,
	}
	var plan RemovalTaskPlan
	if candidate.ProjectionRevision > 0 {
		plan, err = service.plans.PrepareEntryRemoval(ctx, planRequest)
	} else {
		plan, err = prepareControllerRemovalPlan(planRequest)
	}
	if err != nil {
		return RemovalOutcome{}, err
	}
	return service.repository.PublishRemoval(ctx, RemovalPublication{
		Request: request, Candidate: candidate, Task: task, Plan: plan,
	})
}

func validateRemovalCandidate(candidate RemovalCandidate, entryID string) error {
	identity := candidate.Identity
	if candidate.EntryID != entryID || ids.Validate(ids.KindEnvEntry, candidate.EntryID) != nil ||
		candidate.EntryRevision <= 0 || candidate.EnvironmentRevision <= 0 || candidate.ProjectRevision <= 0 ||
		ids.Validate(ids.KindProject, identity.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, identity.EnvironmentID) != nil || identity.ProjectSlug == "" ||
		identity.EnvironmentName == "" || identity.AuthorizedVolumeDir == "" {
		return errs.New(errs.KindInternal, "entry deletion candidate is invalid")
	}
	if identity.TenantID == "" {
		if candidate.TenantRevision != 0 || identity.TenantSlug != "" {
			return errs.New(errs.KindInternal, "entry deletion candidate Tenant identity is invalid")
		}
	} else if ids.Validate(ids.KindTenant, identity.TenantID) != nil || candidate.TenantRevision <= 0 ||
		identity.TenantSlug == "" {
		return errs.New(errs.KindInternal, "entry deletion candidate Tenant identity is invalid")
	}
	if candidate.ProjectionRevision < 0 {
		return errs.New(errs.KindInternal, "entry deletion projection fence is invalid")
	}
	return nil
}
