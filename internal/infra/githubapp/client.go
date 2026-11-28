// Package githubapp reads GitHub App and Actions resources for one pinned repository.
package githubapp

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	apiBase         = "https://api.github.com"
	apiVersion      = "2026-03-10"
	requestTimeout  = 15 * time.Second
	maxJSONBytes    = 4 << 20
	maxPrivateBytes = 16 << 10
)

// Scope is supplied by the Controller's authorized deployment binding.
type Scope struct {
	InstallationID int64
	RepositoryID   int64
}

type Client struct {
	appID string
	key   *rsa.PrivateKey
	http  *http.Client
	mu    sync.Mutex
	last  installation
}

func New(appID int64, privateKeyPEM []byte) (*Client, error) {
	if appID <= 0 || len(privateKeyPEM) == 0 || len(privateKeyPEM) > maxPrivateBytes {
		return nil, errs.New(errs.KindValidationFailed, "GitHub App ID and private key are required")
	}
	key, err := parsePrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	return &Client{
		appID: strconv.FormatInt(appID, 10),
		key:   key,
		http: &http.Client{
			Timeout: requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (client *Client) request(
	ctx context.Context,
	method, path, credential string,
	body []byte,
) (*http.Response, error) {
	req, err := client.newRequest(ctx, method, apiBase+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+credential)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.send(ctx, req)
}

func (*Client) newRequest(ctx context.Context, method, endpoint string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Could not construct GitHub request")
	}
	req.Header.Set("User-Agent", "Groundplane-GitHub-App")
	return req, nil
}

func (client *Client) send(ctx context.Context, request *http.Request) (*http.Response, error) {
	response, err := client.http.Do(request)
	if err == nil {
		return response, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// A URL error can contain a signed artifact URL or request metadata.
	return nil, errs.New(errs.KindRequestUnavailable, "GitHub request could not be completed")
}

func (client *Client) requestJSON(
	ctx context.Context,
	method, path, credential string,
	body []byte,
	status int,
	target any,
) error {
	response, err := client.request(ctx, method, path, credential, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
			response.StatusCode == http.StatusNotFound {
			client.forget(credential)
		}
		return responseError(response)
	}
	return decodeBoundedJSON(response.Body, maxJSONBytes, target)
}

func (client *Client) forget(credential string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.last.token == credential {
		client.last = installation{}
	}
}

func decodeBoundedJSON(reader io.Reader, limit int64, target any) error {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(body)) > limit {
		return errs.New(errs.KindRequestUnavailable, "GitHub response is unreadable or oversized")
	}
	defer clear(body)
	if json.Unmarshal(body, target) != nil {
		return errs.New(errs.KindStateConflict, "GitHub returned invalid deployment metadata")
	}
	return nil
}

func responseError(response *http.Response) error {
	switch {
	case response.StatusCode == http.StatusTooManyRequests ||
		(response.StatusCode == http.StatusForbidden && (response.Header.Get("X-RateLimit-Remaining") == "0" || response.Header.Get("Retry-After") != "")):
		return errs.New(errs.KindRequestUnavailable, "GitHub rate limit prevented deployment metadata access")
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return errs.New(errs.KindScopeUnauthorized, "GitHub App lacks access to the selected repository or workflow")
	case response.StatusCode == http.StatusNotFound:
		return errs.New(errs.KindRequestNotFound, "GitHub repository, workflow, run or artifact was not found")
	case response.StatusCode == http.StatusUnprocessableEntity:
		return errs.New(errs.KindValidationFailed, "GitHub rejected the selected deployment input")
	case response.StatusCode >= 500:
		return errs.Newf(errs.KindRequestUnavailable, "GitHub returned HTTP %d", response.StatusCode)
	default:
		return errs.Newf(errs.KindStateConflict, "GitHub returned unexpected HTTP %d", response.StatusCode)
	}
}

func id(value int64) string { return strconv.FormatInt(value, 10) }
