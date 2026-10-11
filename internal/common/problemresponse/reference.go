package problemresponse

import (
	"net/http"

	"github.com/oklog/ulid/v2"
)

const RequestReferenceHeader = "X-GP-Request-Reference"

// RequestReference accepts only the Controller's canonical attempt identifier,
// never arbitrary proxy text that could enter CLI output or diagnostics.
func RequestReference(response *http.Response) string {
	values := response.Header.Values(RequestReferenceHeader)
	if len(values) != 1 {
		return ""
	}
	value, err := ulid.ParseStrict(values[0])
	if err != nil || value.String() != values[0] {
		return ""
	}
	return values[0]
}
