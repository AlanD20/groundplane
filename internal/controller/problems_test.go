package controller

import (
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRequestProblemCodeUsesPublicCatalog(t *testing.T) {
	tests := []struct {
		status int
		code   errs.Code
	}{
		{http.StatusBadRequest, errs.CodeValidationFailed},
		{http.StatusUnprocessableEntity, errs.CodeValidationFailed},
		{http.StatusNotFound, errs.CodeRequestNotFound},
		{http.StatusMethodNotAllowed, errs.CodeRequestMethodNotAllowed},
		{http.StatusNotAcceptable, errs.CodeRequestNotAcceptable},
		{http.StatusUnsupportedMediaType, errs.CodeRequestUnsupportedMediaType},
		{http.StatusTeapot, errs.CodeRequestFailed},
	}

	for _, test := range tests {
		if got := requestProblemCode(test.status); got != test.code {
			t.Errorf("requestProblemCode(%d) = %q, want %q", test.status, got, test.code)
		}
	}
}

func TestRequestProblemPreservesFrameworkValidationStatus(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		problem := requestProblem(status, "validation failed", nil)
		if problem.Status != status {
			t.Errorf("requestProblem(%d).Status = %d, want %d", status, problem.Status, status)
		}
		if problem.Code != errs.CodeValidationFailed {
			t.Errorf("requestProblem(%d).Code = %q, want %q", status, problem.Code, errs.CodeValidationFailed)
		}
	}
}
