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
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Client struct {
	BaseURL       string
	HTTP          *http.Client
	StreamingHTTP *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL:       baseURL,
		HTTP:          &http.Client{Timeout: 30 * time.Second},
		StreamingHTTP: &http.Client{},
	}
}

// Do sends method+path (BaseURL-relative, e.g. "/api/v1/services") with
// optional query params and a JSON body, and decodes a JSON response
// into out (nil to discard the body). body/out are `any` here for the
// same reason json.Marshal/json.Decoder.Decode are — this is a generic
// transport helper, not a model field.
//
// A non-2xx response is decoded as RFC 7807 problem+json and returned as
// an *errs.Error carrying the same Code the Controller sent — see
// api-cli.md, "Errors".
func (c *Client) Do(ctx context.Context, method, path string, query map[string]string, body any, out any) error {
	u, err := url.Parse(c.BaseURL + path)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: bad url: %w", err))
	}
	if len(query) > 0 {
		q := u.Query()
		for k, v := range query {
			if v != "" {
				q.Set(k, v)
			}
		}
		u.RawQuery = q.Encode()
	}

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: marshal body: %w", err))
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: build request: %w", err))
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: %s %s: %w (is the Controller running? --host / GROUNDPLANE_HOST)", method, path, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseProblem(method, path, resp)
	}

	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: decode response: %w", err))
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
