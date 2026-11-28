// Package runneridentity serves the authenticated, closed Runner listener.
package runneridentity

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Admission resolves request ownership, linked resources, and active grants
// from authoritative state. Resolve must not publish work or change state.
type Admission interface {
	Resolve(context.Context, Principal, Request) (ResolvedAction, error)
}

type Request struct {
	Operation      runneraccess.Operation
	ResourceID     string
	IdempotencyKey string
	Deploy         *api.DeployRequest
	Fetch          *api.ImageFetchRequest
	Build          *BuildResult
}

// BuildResult is an optional untrusted report. Admission must independently
// verify the job identity, captured source, and image before using the result.
type BuildResult struct {
	RunID        int64  `json:"run_id"`
	RunAttempt   int64  `json:"run_attempt"`
	SourceCommit string `json:"source_commit"`
	ImageDigest  string `json:"image_digest"`
}

// ResolvedAction binds the checked resources to the effect to be performed.
// Mutating Apply functions must guard the same resource and credential
// revisions in their acceptance transaction. A preliminary permission check
// alone does not authorize a later, unfenced write.
type ResolvedAction struct {
	Registration runneraccess.Registration
	Grants       []runneraccess.Grant
	Policy       runneraccess.Request
	RunnerEpoch  uint64
	Apply        func(context.Context) (Result, error)
}

// Result carries the exact successful JSON bytes and status produced by the
// action owner, including a replayed durable Task acceptance when applicable.
type Result struct {
	Status int
	Body   json.RawMessage
}

type Handler struct {
	reader    Reader
	admission Admission
	logger    *slog.Logger
}

// New constructs the dedicated Runner handler without an operator handler or
// fallback route. Bind this only to a private listener used by the Runner.
func New(reader Reader, admission Admission, logger *slog.Logger) (*Handler, error) {
	if reader == nil || admission == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "runner listener dependencies are required")
	}
	return &Handler{reader: reader, admission: admission, logger: logger}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")

	input, valid := matchRequest(request)
	if !valid {
		handler.writeProblem(writer, errs.New(errs.KindRequestNotFound, "runner route not found"))
		return
	}
	principal, err := handler.authenticate(request)
	if err != nil {
		handler.writeProblem(writer, err)
		return
	}
	if !credentialAllows(principal, input) {
		handler.writeProblem(writer, forbidden())
		return
	}
	if err := decodeRequest(request, &input); err != nil {
		handler.writeProblem(writer, err)
		return
	}
	if input.IdempotencyKey != "" {
		input.IdempotencyKey = namespaceKey(principal, input.IdempotencyKey)
	}
	result, err := handler.apply(request.Context(), principal, input)
	if err != nil {
		handler.writeProblem(writer, err)
		return
	}
	if result.Status != successStatus(input.Operation) || !json.Valid(result.Body) {
		handler.writeProblem(writer, errs.New(errs.KindInternal, "runner action returned an invalid result"))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(result.Status)
	if _, err := writer.Write(result.Body); err != nil {
		handler.logger.Debug("write runner action response failed")
	}
}

func (handler *Handler) apply(ctx context.Context, principal Principal, input Request) (Result, error) {
	resolved, err := handler.admission.Resolve(ctx, principal, input)
	if err != nil {
		if errs.IsNotFound(err) {
			return Result{}, forbidden()
		}
		return Result{}, err
	}
	if resolved.Registration.RunnerID != principal.Credential.RunnerID ||
		resolved.RunnerEpoch != principal.Credential.RunnerEpoch ||
		resolved.Policy.Operation != input.Operation ||
		input.Operation != runneraccess.ReportBuild && resolved.Policy.Target.ID != input.ResourceID ||
		!prerequisitesResolved(input, resolved.Policy.Related) ||
		resolved.Apply == nil {
		return Result{}, forbidden()
	}
	if err := runneraccess.Authorize(resolved.Registration, resolved.Grants, resolved.Policy); err != nil {
		return Result{}, forbidden()
	}
	if !principal.Credential.ActiveAt(time.Now()) {
		return Result{}, forbidden()
	}
	return resolved.Apply(ctx)
}

func prerequisitesResolved(input Request, related []runneraccess.Resource) bool {
	var after []string
	switch input.Operation {
	case runneraccess.DeployService:
		after = input.Deploy.After
	case runneraccess.FetchServiceImage:
		after = input.Fetch.After
	}
	for _, taskID := range after {
		if !slices.ContainsFunc(related, func(resource runneraccess.Resource) bool {
			return resource.Kind == ids.KindTask && resource.ID == taskID
		}) {
			return false
		}
	}
	return true
}

func credentialAllows(principal Principal, input Request) bool {
	bound := principal.Credential.RequestID
	if input.Operation == runneraccess.ReportBuild {
		return bound != "" && bound == input.ResourceID
	}
	return bound == ""
}

func successStatus(operation runneraccess.Operation) int {
	switch operation {
	case runneraccess.ReadService, runneraccess.ReadTask:
		return http.StatusOK
	default:
		return http.StatusAccepted
	}
}

func forbidden() *errs.Error {
	return errs.New(errs.KindRequestForbidden, "runner is not authorized for this Groundplane operation")
}

func (handler *Handler) writeProblem(writer http.ResponseWriter, err error) {
	var problem *errs.Error
	if !errors.As(err, &problem) {
		problem = errs.New(errs.KindInternal, "runner operation failed")
	} else if problem.Class() == errs.ClassForbidden {
		problem = forbidden()
	}
	writer.Header().Set("Content-Type", "application/problem+json")
	writer.WriteHeader(problem.HTTPStatus())
	if err := json.NewEncoder(writer).Encode(problem.ToProblem()); err != nil {
		handler.logger.Debug("write runner problem response failed")
	}
}
