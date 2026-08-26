// Package problemresponse owns the bounded, strict client boundary for RFC
// 7807 response bodies. It leaves response closure to the caller that obtained
// the body so transports retain their existing close ownership.
package problemresponse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// MaximumBytes is the exact accepted size of a non-success API response body.
const MaximumBytes int64 = 1 << 20

// Read preserves unrestricted success response parsing while bounding and
// validating every non-success body before a generated parser can allocate.
func Read(response *http.Response) ([]byte, error) {
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return io.ReadAll(response.Body)
	}
	if response.ContentLength > MaximumBytes {
		return nil, invalid(response.StatusCode, "declared body exceeds the response limit")
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, MaximumBytes+1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("problemresponse: read response body: %w", err))
	}
	if int64(len(body)) > MaximumBytes {
		return nil, invalid(response.StatusCode, "body exceeds the response limit")
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/problem+json" {
		return nil, invalid(response.StatusCode, "content type is not application/problem+json")
	}
	problem, ok := Decode(body)
	if !ok || problem.Status != response.StatusCode {
		return nil, invalid(response.StatusCode, "body is not the exact Problem contract")
	}
	if _, ok := errs.FromProblem(problem); !ok {
		return nil, invalid(response.StatusCode, "body has an unknown Problem identity")
	}
	return body, nil
}

// Decode accepts exactly one JSON object containing each of the five Problem
// members exactly once, with no nullable, unknown, duplicate, or trailing data.
func Decode(body []byte) (errs.Problem, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errs.Problem{}, false
	}

	const (
		typeMember uint8 = 1 << iota
		titleMember
		statusMember
		detailMember
		codeMember
		allProblemMembers = typeMember | titleMember | statusMember | detailMember | codeMember
	)
	var problem errs.Problem
	var seen uint8
	decodeString := func(destination *string) bool {
		var value *string
		if err := decoder.Decode(&value); err != nil || value == nil {
			return false
		}
		*destination = *value
		return true
	}

	for decoder.More() {
		memberToken, err := decoder.Token()
		member, ok := memberToken.(string)
		if err != nil || !ok {
			return errs.Problem{}, false
		}
		var bit uint8
		switch member {
		case "type":
			bit = typeMember
			if !decodeString(&problem.Type) {
				return errs.Problem{}, false
			}
		case "title":
			bit = titleMember
			if !decodeString(&problem.Title) {
				return errs.Problem{}, false
			}
		case "status":
			bit = statusMember
			var value *int
			if err := decoder.Decode(&value); err != nil || value == nil {
				return errs.Problem{}, false
			}
			problem.Status = *value
		case "detail":
			bit = detailMember
			if !decodeString(&problem.Detail) {
				return errs.Problem{}, false
			}
		case "code":
			bit = codeMember
			var value string
			if !decodeString(&value) {
				return errs.Problem{}, false
			}
			problem.Code = errs.Code(value)
		default:
			return errs.Problem{}, false
		}
		if seen&bit != 0 {
			return errs.Problem{}, false
		}
		seen |= bit
	}

	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || seen != allProblemMembers {
		return errs.Problem{}, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errs.Problem{}, false
	}
	return problem, true
}

func invalid(status int, reason string) *errs.Error {
	return errs.Newf(errs.KindInternal, "problemresponse: status %d: %s", status, reason)
}
