package errs

// GetStatus lets the one Groundplane error type travel through Huma handlers.
func (e *Error) GetStatus() int {
	return e.HTTPStatus()
}

// ContentType keeps all problem responses on the RFC 7807 media type.
func (e *Error) ContentType(_ string) string {
	return "application/problem+json"
}
