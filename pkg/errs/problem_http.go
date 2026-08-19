package errs

// Error allows Problem to travel through typed HTTP handlers without losing
// its RFC 7807 response body.
func (p Problem) Error() string {
	return p.Detail
}

// GetStatus exposes the HTTP status expected by typed HTTP adapters.
func (p Problem) GetStatus() int {
	return p.Status
}

// ContentType keeps all problem responses on the RFC 7807 media type.
func (p Problem) ContentType(_ string) string {
	return "application/problem+json"
}
