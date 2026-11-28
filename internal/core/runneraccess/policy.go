// Package runneraccess evaluates Runner permissions against resolved Groundplane ownership.
package runneraccess

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Registration is the ownership ceiling of the authenticated Runner. Load it
// from the live registration, using the identity verified by the transport.
type Registration struct {
	RunnerID  string
	TenantID  string
	OwnerKind runner.OwnerKind
	OwnerID   string
}

// Resource is a stable identity with the complete ancestry needed to authorize
// access. Resolve ancestry from authoritative state. Callers must not fill in
// the ancestry from request fields or substitute a requested scope for the
// affected resource's scope.
type Resource struct {
	Kind          ids.Kind
	ID            string
	TenantID      string
	ProjectID     string
	EnvironmentID string
}

type Operation string

const (
	ReadService   Operation = "service.read"
	DeployService Operation = "service.deploy"
	ReadTask      Operation = "task.read"
	ReportBuild   Operation = "build.report"
	// FetchServiceImage authorizes a Controller-captured image for one Service.
	// Generic host image Fetch is outside the Runner authorization policy.
	FetchServiceImage Operation = "service.image.fetch"
)

// Grant allows one operation on an exact resource and enumerates the related
// resources that the operation may affect. Load grants from the authenticated
// job's binding, not the job's input. Use only active grants for that job.
type Grant struct {
	Operation Operation
	Target    Resource
	Related   []Resource
}

// Request describes the actual operation, primary target, and all other
// resources whose data or state the operation will access. Authorize against
// the same resolved resources used by admission, with ownership changes fenced
// through publication. Missing or unresolved references must fail admission.
type Request struct {
	Operation Operation
	Target    Resource
	Related   []Resource
}

// Authorize permits only a matching grant whose resources also fall within the
// Runner's registered scope. It returns the same public error for all denials.
func Authorize(registration Registration, grants []Grant, request Request) error {
	if request.Operation.targetKind() == request.Target.Kind && registration.allows(request.Target) {
		for _, related := range request.Related {
			if !registration.allows(related) {
				return forbidden()
			}
		}
		for _, grant := range grants {
			if grant.Operation == request.Operation && grant.Target == request.Target &&
				includesAll(grant.Related, request.Related) {
				return nil
			}
		}
	}
	return forbidden()
}

func (operation Operation) targetKind() ids.Kind {
	switch operation {
	case ReadService, DeployService, ReportBuild, FetchServiceImage:
		return ids.KindService
	case ReadTask:
		return ids.KindTask
	default:
		return ""
	}
}

func (registration Registration) allows(resource Resource) bool {
	if ids.Validate(ids.KindRunner, registration.RunnerID) != nil || registration.TenantID != resource.TenantID ||
		!resource.valid() {
		return false
	}
	switch registration.OwnerKind {
	case runner.OwnerTenant:
		return registration.OwnerID == resource.TenantID
	case runner.OwnerProject:
		return resource.ProjectID != "" && registration.OwnerID == resource.ProjectID
	case runner.OwnerEnvironment:
		return resource.EnvironmentID != "" && registration.OwnerID == resource.EnvironmentID
	default:
		return false
	}
}

func (resource Resource) valid() bool {
	if ids.Validate(ids.KindTenant, resource.TenantID) != nil || ids.Validate(resource.Kind, resource.ID) != nil {
		return false
	}
	switch resource.Kind {
	case ids.KindTenant:
		return resource.ID == resource.TenantID && resource.ProjectID == "" && resource.EnvironmentID == ""
	case ids.KindProject:
		return resource.ID == resource.ProjectID && resource.EnvironmentID == ""
	case ids.KindEnvironment:
		return resource.ID == resource.EnvironmentID && ids.Validate(ids.KindProject, resource.ProjectID) == nil
	case ids.KindService,
		ids.KindTask,
		ids.KindEnvEntry,
		ids.KindVolume,
		ids.KindAttach,
		ids.KindRoute,
		ids.KindNetwork:
		return ids.Validate(ids.KindProject, resource.ProjectID) == nil &&
			ids.Validate(ids.KindEnvironment, resource.EnvironmentID) == nil
	default:
		return false
	}
}

func includesAll(granted []Resource, requested []Resource) bool {
	for _, resource := range requested {
		if !slices.Contains(granted, resource) {
			return false
		}
	}
	return true
}

func forbidden() error {
	return errs.New(errs.KindRequestForbidden, "Runner is not authorized for this Groundplane operation")
}
