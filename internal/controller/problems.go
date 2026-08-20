package controller

import (
	"errors"
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

func configureProblemResponses() {
	huma.NewError = func(status int, message string, details ...error) huma.StatusError {
		return requestProblem(status, message, details)
	}
}

func requestProblem(status int, message string, details []error) errs.Problem {
	for _, detail := range details {
		var tooLarge *http.MaxBytesError
		if errors.As(detail, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
			message = "request body exceeds route limit"
			details = nil
			break
		}
	}
	code := requestProblemCode(status)
	if len(details) > 0 {
		messages := make([]string, 0, len(details))
		for _, detail := range details {
			if detail != nil {
				messages = append(messages, detail.Error())
			}
		}
		if len(messages) > 0 {
			message += ": " + strings.Join(messages, "; ")
		}
	}

	return errs.Problem{
		Type:   errs.ProblemType,
		Title:  http.StatusText(status),
		Status: status,
		Detail: message,
		Code:   code,
	}
}

func requestProblemCode(status int) errs.Code {
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return errs.CodeValidationFailed
	case http.StatusNotFound:
		return errs.CodeRequestNotFound
	case http.StatusMethodNotAllowed:
		return errs.CodeRequestMethodNotAllowed
	case http.StatusNotAcceptable:
		return errs.CodeRequestNotAcceptable
	case http.StatusUnsupportedMediaType:
		return errs.CodeRequestUnsupportedMediaType
	default:
		return errs.CodeRequestFailed
	}
}
