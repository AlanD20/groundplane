// Package apiclient is the CLI's Go client for the human API
// (REST/JSON). Every command in internal/cli calls through here —
// nothing else in the CLI speaks HTTP directly. In production this
// package's shapes are generated FROM the OpenAPI document (code-first
// from the Controller's handlers); this hand-written version is the
// seed of that generator output. See architecture.md, "API contracts
// (locked)".
//
// This package sits alongside internal/cli/common as CLI-internal
// plumbing: it's the one place allowed to construct *errs.Error values
// directly from a decoded RFC 7807 problem+json body, so the noun
// command files never need to import pkg/errs themselves.
package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const idempotencyKeyHeader = "Idempotency-Key"

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{16,128}$`)

// Request is one transport request for one human intent. Reusing this value
// preserves its Idempotency-Key; Do never generates or replaces the key.
type Request struct {
	Method         string
	Path           string
	Query          map[string]string
	Body           any
	IdempotencyKey string
}

type Client struct {
	BaseURL       string
	HTTP          *http.Client
	StreamingHTTP *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL:       strings.TrimRight(baseURL, "/"),
		HTTP:          &http.Client{Timeout: 30 * time.Second},
		StreamingHTTP: &http.Client{},
	}
}

// NewRequest creates one reusable request. Human mutation methods receive one
// raw ULID at intent construction; safe methods never receive a key.
func (c *Client) NewRequest(method, path string, query map[string]string, body any) Request {
	request := Request{
		Method: strings.ToUpper(method),
		Path:   path,
		Query:  query,
		Body:   body,
	}
	if requiresIdempotencyKey(request.Method) {
		request.IdempotencyKey = ids.NewULID()
	}
	return request
}

// Do sends a BaseURL-relative request with optional query params and a JSON
// body, and decodes exactly one JSON response document into out (nil to
// discard the body). Request.Body/out are `any` here for the
// same reason json.Marshal/json.Decoder.Decode are — this is a generic
// transport helper, not a model field.
//
// A non-2xx response is decoded as RFC 7807 problem+json and returned as
// an *errs.Error carrying the same Code the Controller sent — see
// api-cli.md, "Errors".
func (c *Client) Do(ctx context.Context, request Request, out any) error {
	request.Method = strings.ToUpper(request.Method)
	if err := validateIdempotencyKey(request); err != nil {
		return err
	}

	u, err := url.Parse(c.BaseURL + request.Path)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: bad url: %w", err))
	}
	if len(request.Query) > 0 {
		q := u.Query()
		for k, v := range request.Query {
			if v != "" {
				q.Set(k, v)
			}
		}
		u.RawQuery = q.Encode()
	}

	var reader io.Reader
	if request.Body != nil {
		b, err := json.Marshal(request.Body)
		if err != nil {
			return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: marshal body: %w", err))
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, request.Method, u.String(), reader)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: build request: %w", err))
	}
	req.Header.Set("Accept", "application/json")
	if request.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if request.IdempotencyKey != "" {
		req.Header.Set(idempotencyKeyHeader, request.IdempotencyKey)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: %s %s: %w (is the Controller running? --host / GROUNDPLANE_HOST)", request.Method, request.Path, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseProblem(request.Method, request.Path, resp)
	}

	if out == nil {
		return nil
	}
	if resp.StatusCode == http.StatusNoContent {
		return errs.Newf(errs.CodeInternal, "apiclient: %s %s returned 204 with an output target", request.Method, request.Path)
	}
	return decodeSingleJSON(request.Method, request.Path, resp.Body, out)
}

func requiresIdempotencyKey(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func validateIdempotencyKey(request Request) error {
	required := requiresIdempotencyKey(request.Method)
	if required && !idempotencyKeyPattern.MatchString(request.IdempotencyKey) {
		return errs.Newf(errs.CodeInternal, "apiclient: %s %s has an invalid Idempotency-Key", request.Method, request.Path)
	}
	if !required && request.IdempotencyKey != "" {
		return errs.Newf(errs.CodeInternal, "apiclient: safe method %s must not carry an Idempotency-Key", request.Method)
	}
	return nil
}

func decodeSingleJSON(method, path string, body io.Reader, out any) error {
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(out); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: %s %s decode response: %w", method, path, err))
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errs.Newf(errs.CodeInternal, "apiclient: %s %s returned multiple JSON documents", method, path)
		}
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: %s %s decode trailing response: %w", method, path, err))
	}
	return nil
}

// Stream opens one SSE connection and invokes onEvent for each complete event
// data payload. Reconnection policy belongs to the calling command because
// finite and follow streams have different lifecycles.
func (c *Client) Stream(ctx context.Context, path string, query map[string]string, onEvent func(data string) error) error {
	u, err := url.Parse(c.BaseURL + path)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: bad stream url: %w", err))
	}
	if len(query) > 0 {
		q := u.Query()
		for key, value := range query {
			if value != "" {
				q.Set(key, value)
			}
		}
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: build stream request: %w", err))
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.StreamingHTTP.Do(req)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: stream %s: %w", path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseProblem(http.MethodGet, path, resp)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		return errs.Newf(errs.CodeInternal, "apiclient: stream %s returned content type %q", path, resp.Header.Get("Content-Type"))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(splitEventStreamLines)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var data []string
	firstLine := true
	for scanner.Scan() {
		line := strings.ToValidUTF8(scanner.Text(), "\uFFFD")
		if firstLine {
			line = strings.TrimPrefix(line, "\uFEFF")
			firstLine = false
		}
		if line == "" {
			if data != nil {
				if err := onEvent(strings.Join(data, "\n")); err != nil {
					return err
				}
				data = nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		} else {
			value = strings.TrimPrefix(value, " ")
		}
		if field == "data" {
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: read stream %s: %w", path, err))
	}
	return nil
}

func splitEventStreamLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i, value := range data {
		switch value {
		case '\n':
			end := i
			if end > 0 && data[end-1] == '\r' {
				end--
			}
			return i + 1, data[:end], nil
		case '\r':
			if i+1 == len(data) && !atEOF {
				return 0, nil, nil
			}
			advance := i + 1
			if i+1 < len(data) && data[i+1] == '\n' {
				advance++
			}
			return advance, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func responseProblem(method, path string, resp *http.Response) error {
	var problem errs.Problem
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&problem); err != nil || problem.Code == "" {
		return errs.Newf(errs.CodeInternal, "%s %s: unexpected status %d", method, path, resp.StatusCode)
	}
	return errs.New(problem.Code, problem.Detail, errs.WithDetails(problem.Details))
}
