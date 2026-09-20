package agentmanagement

import "github.com/AlanD20/groundplane/pkg/errs"

// MutationService composes the Agent's operator operations without adding a
// second dispatch layer or exposing lifecycle internals to process startup.
type MutationService struct {
	*agentEnrollmentService
	*agentUpdateService
	*agentRemovalService
}

func NewMutationService(
	enrollments *agentEnrollmentService,
	updates *agentUpdateService,
	removals *agentRemovalService,
) (*MutationService, error) {
	if enrollments == nil || updates == nil || removals == nil {
		return nil, errs.New(errs.KindInternal, "Agent mutation services are incomplete")
	}
	return &MutationService{
		agentEnrollmentService: enrollments,
		agentUpdateService:     updates,
		agentRemovalService:    removals,
	}, nil
}
