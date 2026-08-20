// Package errs is THE single error type in the Groundplane codebase —
// no multi-type assertion chains, no bespoke error structs anywhere
// else. Every layer (core, adapters, infra, agent, controller, cli)
// constructs, wraps, and inspects errors through this package only. It
// is a leaf: stdlib only, imported by everything, imports nothing of
// its own. See docs/standards.md, section 3.
package errs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Class categorizes an error semantically for retry and control-flow
// decisions. HTTP status is owned independently by the Kind descriptor.
type Class string

const (
	ClassBadRequest           Class = "bad_request"
	ClassValidation           Class = "validation"
	ClassNotFound             Class = "not_found"
	ClassConflict             Class = "conflict"
	ClassRetryable            Class = "retryable"
	ClassMethodNotAllowed     Class = "method_not_allowed"
	ClassNotAcceptable        Class = "not_acceptable"
	ClassUnsupportedMediaType Class = "unsupported_media_type"
	ClassInternal             Class = "internal"
)

// Code is a stable, machine-readable error code — the RFC 7807 "code"
// value returned by the API, verbatim. Public domain codes are
// dot-namespaced and grouped by domain (service.not_found,
// attach.not_found, deploy.in_flight, …). See codes.go for the full list
// and their Class.
type Code string

// ProblemType is the single RFC 7807 type used for domain and framework
// problems. Stable machine handling belongs to Code, not a type URI.
const ProblemType = "about:blank"

// Problem is the public RFC 7807 representation. It is a transport DTO, not
// an error type; only *Error implements error.
type Problem struct {
	Type    string         `json:"type"`
	Title   string         `json:"title"`
	Status  int            `json:"status"`
	Detail  string         `json:"detail"`
	Code    Code           `json:"code"`
	Details map[string]any `json:"details,omitempty"`
}

// Option is closed to this package so callers can attach approved
// presentation metadata without overriding Kind, Code, Class, or status.
type Option interface {
	apply(*Error)
	errorOption()
}

type detailsOption map[string]any

func (option detailsOption) apply(target *Error) { target.details = cloneDetails(option) }
func (detailsOption) errorOption()               {}

type titleOption string

func (option titleOption) apply(target *Error) { target.title = string(option) }
func (titleOption) errorOption()               {}

// WithDetails attaches structured metadata rendered into the RFC 7807
// response's extension members (never into `detail`, which stays a
// human string). map[string]any is the one approved `any` use in this
// package — the same stdlib-driven exception as json.Decoder.Decode,
// since Details is opaque metadata, not a model field or a validator.
func WithDetails(details map[string]any) Option {
	return detailsOption(details)
}

// WithTitle changes only the RFC 7807 presentation title. It cannot alter the
// machine identity, class, or status.
func WithTitle(title string) Option {
	return titleOption(title)
}

// Error is the one error type every layer returns. Its embedded Problem gives
// Huma the exact RFC 7807 schema; MarshalJSON and ToProblem always restore the
// descriptor-owned Code and status.
type Error struct {
	Problem

	kind    Kind
	message string
	title   string
	details map[string]any
	err     error
}

func (e *Error) Error() string {
	kind := e.Kind()
	value, _ := descriptorFor(kind)
	message := e.message
	if message == "" {
		message = defaultPublicDetail(kind)
	}
	if e.err != nil {
		return fmt.Sprintf("%s: %s: %v", value.Code, message, e.err)
	}
	return fmt.Sprintf("%s: %s", value.Code, message)
}

func (e *Error) Unwrap() error { return e.err }

// Is compares by internal Kind. Kinds may intentionally share one public Code
// while retaining different semantics, such as malformed 400 and validated
// 422 input failures.
func (e *Error) Is(target error) bool {
	var te *Error
	if errors.As(target, &te) {
		return e.Kind() == te.Kind()
	}
	return false
}

// New creates an *Error from the closed Kind catalog. Unknown Kind values
// fail closed as internal programming errors.
func New(kind Kind, message string, opts ...Option) *Error {
	value, ok := descriptorFor(kind)
	if !ok {
		kind = KindInternal
		value, _ = descriptorFor(kind)
	}
	e := &Error{
		kind:    kind,
		message: message,
		title:   string(value.Code),
	}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(e)
		}
	}
	e.Problem = e.ToProblem()
	return e
}

// Newf is New with fmt.Sprintf-style formatting.
func Newf(kind Kind, format string, args ...any) *Error {
	return New(kind, fmt.Sprintf(format, args...))
}

// Wrap attaches a Kind to a private cause. The public detail is the safe HTTP
// status phrase; Error and Unwrap retain the cause for diagnostics.
func Wrap(kind Kind, err error) *Error {
	result := New(kind, defaultPublicDetail(kind))
	result.err = err
	return result
}

// Kind, Class, and Op expose descriptor-derived internal semantics without
// making them mutable fields.
func (e *Error) Kind() Kind { return normalizeKind(e.kind) }

func (e *Error) Class() Class {
	value, _ := descriptorFor(e.Kind())
	return value.Class
}

func (e *Error) Op() string {
	value, _ := descriptorFor(e.Kind())
	return opOf(value.Code)
}

// KindOf returns the closed Kind carried anywhere in an error chain.
func KindOf(err error) (Kind, bool) {
	var value *Error
	if !errors.As(err, &value) {
		return kindInvalid, false
	}
	return value.Kind(), true
}

// opOf derives the domain segment from a dot-namespaced code
// ("service.not_found" -> "service"); unnamespaced internal codes use
// the whole code as their own Op.
func opOf(code Code) string {
	s := string(code)
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i]
	}
	return s
}

// HTTPStatus returns the descriptor-owned status. Call sites cannot override
// it independently of Kind.
func (e *Error) HTTPStatus() int {
	value, _ := descriptorFor(e.Kind())
	return value.Status
}

// ToProblem renders the error using the descriptor-owned machine fields.
func (e *Error) ToProblem() Problem {
	kind := e.Kind()
	value, _ := descriptorFor(kind)
	title := e.title
	detail := e.message
	if kind == KindInternal || kind == KindRequestFailed {
		title = http.StatusText(http.StatusInternalServerError)
		detail = http.StatusText(http.StatusInternalServerError)
	} else {
		if title == "" {
			title = string(value.Code)
		}
		if detail == "" {
			detail = defaultPublicDetail(kind)
		}
	}
	return Problem{
		Type:    ProblemType,
		Title:   title,
		Status:  value.Status,
		Detail:  detail,
		Code:    value.Code,
		Details: cloneDetails(e.details),
	}
}

// MarshalJSON keeps direct Huma serialization on the same projection as
// ToProblem, even if a caller mutates an embedded transport field.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.ToProblem())
}

// FromProblem validates an untrusted public problem tuple and reconstructs its
// internal Kind. Unknown codes and impossible code/status combinations are
// rejected rather than becoming dynamic internal identities.
func FromProblem(problem Problem) (*Error, bool) {
	if problem.Type != ProblemType {
		return nil, false
	}
	kind, ok := kindForProblem(problem.Code, problem.Status)
	if !ok {
		return nil, false
	}
	result := New(kind, problem.Detail, WithTitle(problem.Title), WithDetails(problem.Details))
	return result, true
}

func defaultPublicDetail(kind Kind) string {
	value, _ := descriptorFor(normalizeKind(kind))
	if detail := http.StatusText(value.Status); detail != "" {
		return detail
	}
	return "Request failed"
}

func normalizeKind(kind Kind) Kind {
	if _, ok := descriptorFor(kind); !ok {
		return KindInternal
	}
	return kind
}

func cloneDetails(details map[string]any) map[string]any {
	if details == nil {
		return nil
	}
	cloned := make(map[string]any, len(details))
	for key, value := range details {
		cloned[key] = value
	}
	return cloned
}

// IsNotFound and IsRetryable classify any error (not just *Error) by
// Class, via errors.As — the standard way to branch on error semantics
// in this codebase; never string-match error messages.
func IsNotFound(err error) bool  { return hasClass(err, ClassNotFound) }
func IsRetryable(err error) bool { return hasClass(err, ClassRetryable) }

func hasClass(err error, class Class) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Class() == class
	}
	return false
}
