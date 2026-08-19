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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
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
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: %s %s: %w (is the Controller running? --host / GROUNDPLANE_HOST)", method, path, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var problem errs.Problem
		_ = json.NewDecoder(resp.Body).Decode(&problem)
		if problem.Code == "" {
			problem.Code = errs.CodeInternal
			problem.Detail = fmt.Sprintf("%s %s: unexpected status %d", method, path, resp.StatusCode)
		}
		return errs.New(problem.Code, problem.Detail, errs.WithDetails(problem.Details))
	}

	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("apiclient: decode response: %w", err))
	}
	return nil
}

// Stream opens an SSE connection (task events, logs, activity).
//
// TODO: implement proper SSE framing (event:/data:/id: fields, retry).
// This scaffold is enough for `service logs --follow` and `task show
// --follow` to be wired against once the Controller's SSE endpoints
// exist.
func (c *Client) Stream(ctx context.Context, path string, query map[string]string, onLine func(line string)) error {
	return errs.New(errs.CodeNotImplemented, "apiclient: Stream not implemented")
}
