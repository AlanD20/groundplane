package controller

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MutationAdmission keeps ordinary operator and scheduled writes outside an
// unfinished native recovery. A nonempty id is only the exact Task Abort request;
// it does not authorize any other mutation of that Task or its resources.
type MutationAdmission interface {
	CheckMutation(context.Context, string) error
}

func (server *Server) admitHTTPMutation(request *http.Request) *errs.Error {
	if server.mutationAdmission == nil || !mayAcceptTask(request.Method) {
		return nil
	}
	if request.Method == http.MethodPost {
		if isReadOnlyEnvironmentPostPath(request.URL.Path) || request.URL.Path == "/api/v1/controller/update" {
			// Native update owns protected same-Task replay before its active-journal
			// check; a different acceptance cannot be published during recovery.
			return nil
		}
		const prefix = "/api/v1/tasks/"
		if remainder, found := strings.CutPrefix(request.URL.Path, prefix); found {
			taskID, action, found := strings.Cut(remainder, "/")
			if found && action == "abort" && ids.Validate(ids.KindTask, taskID) == nil {
				return mutationAdmissionProblem(server.mutationAdmission.CheckMutation(request.Context(), taskID))
			}
		}
	}
	return mutationAdmissionProblem(server.mutationAdmission.CheckMutation(request.Context(), ""))
}

func mutationAdmissionProblem(err error) *errs.Error {
	if err == nil {
		return nil
	}
	var domain *errs.Error
	if errors.As(err, &domain) {
		return domain
	}
	return errs.Wrap(errs.KindInternal, err)
}
