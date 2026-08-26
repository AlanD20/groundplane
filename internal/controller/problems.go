package controller

import (
	"errors"
	"net/http"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

var configureProblemResponsesOnce sync.Once

func problemAPIConfig(title string, version string) huma.Config {
	config := huma.DefaultConfig(title, version)
	config.OpenAPIPath = ""
	config.DocsPath = ""
	config.SchemasPath = ""
	config.RejectUnknownQueryParameters = true
	config.CreateHooks = append(
		[]func(huma.Config) huma.Config{markProblemSchemaLinkHandledHook},
		config.CreateHooks...,
	)
	config.CreateHooks = append(config.CreateHooks, clearProblemSchemaLinkSentinelHook)
	return config
}

// Huma's default schema-link hook adds $schema to every response model. A
// transient sentinel around that hook keeps Error closed without changing the
// schema-link behavior of successful response models.
func markProblemSchemaLinkHandledHook(config huma.Config) huma.Config {
	config.OnAddOperation = append(config.OnAddOperation, markProblemSchemaLinkHandled)
	return config
}

func markProblemSchemaLinkHandled(openAPI *huma.OpenAPI, _ *huma.Operation) {
	schema := openAPI.Components.Schemas.Map()["Error"]
	if schema != nil {
		schema.Properties["$schema"] = &huma.Schema{Type: huma.TypeString}
	}
}

func clearProblemSchemaLinkSentinelHook(config huma.Config) huma.Config {
	config.OnAddOperation = append(config.OnAddOperation, clearProblemSchemaLinkSentinel)
	return config
}

func clearProblemSchemaLinkSentinel(openAPI *huma.OpenAPI, _ *huma.Operation) {
	schema := openAPI.Components.Schemas.Map()["Error"]
	if schema != nil {
		delete(schema.Properties, "$schema")
	}
}

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
