// Package errs is THE single error type in the Groundplane codebase —
// no multi-type assertion chains, no bespoke error structs anywhere
// else. Every layer (core, adapters, infra, agent, controller, cli)
// constructs, wraps, and inspects errors through this package only. It
// is a leaf: stdlib only, imported by everything, imports nothing of
// its own. See docs/standards.md, section 3.
package errs

import (
	"errors"
	"fmt"
	"strings"
)

// Class categorizes an error semantically. HTTP status mapping and
// retry decisions derive from Class, not from Code — a new Code never
// needs a new switch statement anywhere else in the codebase.
type Class string

const (
	ClassValidation           Class = "validation"
	ClassNotFound             Class = "not_found"
	ClassConflict             Class = "conflict"
	ClassRetryable            Class = "retryable"
	ClassNeedsInteraction     Class = "needs_interaction"
	ClassMethodNotAllowed     Class = "method_not_allowed"
	ClassNotAcceptable        Class = "not_acceptable"
	ClassUnsupportedMediaType Class = "unsupported_media_type"
	ClassInternal             Class = "internal"
)

// Code is a stable, machine-readable error code — the RFC 7807 "code"
// value returned by the API, verbatim. Dot-namespaced and grouped by
// domain (service.not_found, attach.not_found, …), EXCEPT the two codes
// already locked verbatim elsewhere in the docs (deploy_in_flight,
// strategy_not_implemented — api-cli.md cites these as literal examples,
// so they stay flat rather than being renamed to fit the dot convention).
// See codes.go for the full list and their Class.
type Code string

// Option configures an *Error at construction time.
type Option func(*Error)

// WithDetails attaches structured metadata rendered into the RFC 7807
// response's extension members (never into `detail`, which stays a
// human string). map[string]any is the one approved `any` use in this
// package — the same stdlib-driven exception as json.Decoder.Decode,
// since Details is opaque metadata, not a model field or a validator.
func WithDetails(details map[string]any) Option {
	return func(e *Error) { e.Details = details }
}

// Error is the one error type every layer returns — comparable via
// errors.Is/As, carrying a stable Code, an auto-derived Class and Op,
// and an optional wrapped cause.
type Error struct {
	Code    Code
	Class   Class
	Op      string // the domain segment of Code, auto-derived (e.g. "service" from "service.not_found")
	Message string
	Err     error
	Details map[string]any
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

// Is compares by Code alone — errors.Is(err, errs.New(SomeCode, "")).
func (e *Error) Is(target error) bool {
	var te *Error
	if errors.As(target, &te) {
		return e.Code == te.Code
	}
	return false
}

// New creates an *Error. Class and Op are auto-derived from Code —
// callers never set them directly, so a Code's meaning can't drift from
// its classification at one call site and not another.
func New(code Code, message string, opts ...Option) *Error {
	e := &Error{
		Code:    code,
		Class:   classify(code),
		Op:      opOf(code),
		Message: message,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Newf is New with fmt.Sprintf-style formatting.
func Newf(code Code, format string, args ...any) *Error {
	return New(code, fmt.Sprintf(format, args...))
}

// Wrap attaches a Code to an existing error, taking the wrapped error's
// message as its own — no separate message needed at the call site.
func Wrap(code Code, err error) *Error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &Error{Code: code, Class: classify(code), Op: opOf(code), Message: msg, Err: err}
}

// opOf derives the domain segment from a dot-namespaced code
// ("service.not_found" -> "service"); flat, locked-verbatim codes
// (deploy_in_flight, not_implemented, internal, …) use the whole code as
// their own Op.
func opOf(code Code) string {
	s := string(code)
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i]
	}
	return s
}

// HTTPStatus derives the Controller's RFC 7807 status from Class, with
// one special case: CodeNotImplemented always maps to 501 regardless of
// its (internal) Class, since that's the literal HTTP meaning.
func (e *Error) HTTPStatus() int {
	if e.Code == CodeNotImplemented {
		return 501
	}
	switch e.Class {
	case ClassValidation:
		return 400
	case ClassNotFound:
		return 404
	case ClassMethodNotAllowed:
		return 405
	case ClassNotAcceptable:
		return 406
	case ClassConflict, ClassNeedsInteraction:
		return 409
	case ClassUnsupportedMediaType:
		return 415
	case ClassRetryable:
		return 503
	default:
		return 500
	}
}

// Problem is the RFC 7807 problem+json shape the Controller's HTTP layer
// serializes every *Error into. See api-cli.md, "Errors".
type Problem struct {
	Type    string         `json:"type"`
	Title   string         `json:"title"`
	Status  int            `json:"status"`
	Detail  string         `json:"detail"`
	Code    Code           `json:"code"`
	Details map[string]any `json:"details,omitempty"`
}

// ToProblem renders e as an RFC 7807 problem at its derived HTTPStatus().
func (e *Error) ToProblem() Problem {
	return Problem{
		Type:    "about:blank",
		Title:   string(e.Code),
		Status:  e.HTTPStatus(),
		Detail:  e.Message,
		Code:    e.Code,
		Details: e.Details,
	}
}

// IsNotFound and IsRetryable classify any error (not just *Error) by
// Class, via errors.As — the standard way to branch on error semantics
// in this codebase; never string-match error messages.
func IsNotFound(err error) bool  { return hasClass(err, ClassNotFound) }
func IsRetryable(err error) bool { return hasClass(err, ClassRetryable) }

func hasClass(err error, class Class) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Class == class
	}
	return false
}
