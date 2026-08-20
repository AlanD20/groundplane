package controller

import (
	"errors"
	"net/http"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

var configureProblemResponsesOnce sync.Once

func configureProblemResponses() {
	configureProblemResponsesOnce.Do(func() {
		huma.NewError = func(status int, message string, details ...error) huma.StatusError {
			return requestProblem(status, message, details)
		}
	})
}

func requestProblem(status int, message string, details []error) *errs.Error {
	for _, detail := range details {
		var tooLarge *http.MaxBytesError
		if errors.As(detail, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
			message = "request body exceeds route limit"
			details = nil
			break
		}
	}
	kind := requestProblemKind(status)
	problem := errs.New(kind, message)
	return errs.New(kind, message, errs.WithTitle(http.StatusText(problem.HTTPStatus())))
}

func requestProblemKind(status int) errs.Kind {
	switch status {
	case http.StatusBadRequest:
		return errs.KindMalformedRequest
	case http.StatusUnprocessableEntity:
		return errs.KindValidationFailed
	case http.StatusNotFound:
		return errs.KindRequestNotFound
	case http.StatusMethodNotAllowed:
		return errs.KindRequestMethodNotAllowed
	case http.StatusNotAcceptable:
		return errs.KindRequestNotAcceptable
	case http.StatusRequestEntityTooLarge:
		return errs.KindRequestTooLarge
	case http.StatusUnsupportedMediaType:
		return errs.KindRequestUnsupportedMediaType
	case http.StatusServiceUnavailable:
		return errs.KindRequestUnavailable
	case http.StatusNotImplemented:
		return errs.KindNotImplemented
	default:
		return errs.KindRequestFailed
	}
}
