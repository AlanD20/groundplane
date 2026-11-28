package apiclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const runnerTokenBytes = 32
const runnerTokenLength = 48

// NewRunner uses only the configured private origin and the closed Runner
// route set. Keep both controls in the transport so other client helpers
// cannot accidentally send the bearer to the operator API.
func NewRunner(endpoint, tokenPath string) (*Client, error) {
	origin, err := runnerOrigin(endpoint)
	if err != nil {
		return nil, err
	}
	token, err := readRunnerToken(tokenPath)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &runnerTransport{
		origin: *origin,
		token:  token,
		// A Runner endpoint is a private listener. Bypass proxy environment
		// variables regardless of the job's environment.
		base: &http.Transport{DialContext: dialer.DialContext, TLSHandshakeTimeout: 10 * time.Second},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Client{BaseURL: origin.String(), HTTP: client, StreamingHTTP: client, scopedRunner: true}, nil
}

func (c *Client) ScopedRunner() bool { return c.scopedRunner }

func runnerOrigin(endpoint string) (*url.URL, error) {
	origin, err := url.Parse(endpoint)
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") ||
		origin.Opaque != "" || origin.User != nil || origin.Path != "" || origin.RawPath != "" ||
		origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || origin.String() != endpoint {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Runner endpoint must be a private HTTP origin with an explicit port",
		)
	}
	target, err := netip.ParseAddrPort(origin.Host)
	if err != nil || !target.Addr().Is4() || (!target.Addr().IsPrivate() && !target.Addr().IsLoopback()) ||
		target.Port() == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Runner endpoint must use a private IPv4 address and port")
	}
	return origin, nil
}

func readRunnerToken(path string) (string, error) {
	invalid := errs.New(
		errs.KindValidationFailed,
		"Runner bearer file must be an absolute path to a regular file with mode 0400 or 0440",
	)
	if !filepath.IsAbs(path) {
		return "", invalid
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", invalid
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return "", invalid
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || (info.Mode().Perm() != 0o400 && info.Mode().Perm() != 0o440) {
		return "", invalid
	}
	var data [runnerTokenLength + 2]byte
	defer clear(data[:])
	size, err := io.ReadFull(file, data[:])
	if err != io.ErrUnexpectedEOF || size < runnerTokenLength {
		return "", errs.New(errs.KindValidationFailed, "Runner bearer file does not contain a valid credential")
	}
	if size == runnerTokenLength+1 && data[size-1] == '\n' {
		size--
	}
	encoded, ok := bytes.CutPrefix(data[:size], []byte("gpr1_"))
	if !ok || size != runnerTokenLength {
		return "", errs.New(errs.KindValidationFailed, "Runner bearer file does not contain a valid credential")
	}
	var raw [runnerTokenBytes]byte
	defer clear(raw[:])
	decoded, err := base64.RawURLEncoding.Strict().Decode(raw[:], encoded)
	if err != nil || decoded != len(raw) {
		return "", errs.New(errs.KindValidationFailed, "Runner bearer file does not contain a valid credential")
	}
	return string(data[:size]), nil
}

type runnerTransport struct {
	origin url.URL
	token  string
	base   http.RoundTripper
}

func (transport *runnerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	url := request.URL
	if url.Scheme != transport.origin.Scheme || url.Host != transport.origin.Host || url.User != nil ||
		url.Opaque != "" ||
		url.RawQuery != "" ||
		url.ForceQuery ||
		url.Fragment != "" ||
		url.EscapedPath() != url.Path ||
		(request.Host != "" && request.Host != transport.origin.Host) ||
		!allowedRunnerRoute(request.Method, url.Path) {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, errs.New(errs.KindRequestForbidden, "request is outside the scoped Runner API")
	}
	scoped := request.Clone(request.Context())
	scoped.Header.Set("Authorization", "Bearer "+transport.token)
	return transport.base.RoundTrip(scoped)
}

func allowedRunnerRoute(method, path string) bool {
	path, ok := strings.CutPrefix(path, "/api/v1/")
	if !ok {
		return false
	}
	resource, remainder, _ := strings.Cut(path, "/")
	id, action, hasAction := strings.Cut(remainder, "/")
	switch resource {
	case "services":
		return ids.Validate(ids.KindService, id) == nil &&
			((method == http.MethodGet && !hasAction) ||
				(method == http.MethodPost && (action == "deploy" || action == "images/fetch")))
	case "tasks":
		return method == http.MethodGet && !hasAction && ids.Validate(ids.KindTask, id) == nil
	default:
		return false
	}
}

func (c *Client) FetchRunnerServiceImage(
	ctx context.Context,
	serviceID, image, key string,
	after []string,
) (apiTypes.TaskAccepted, error) {
	if !c.ScopedRunner() || ids.Validate(ids.KindService, serviceID) != nil {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindValidationFailed,
			"scoped image fetch requires a Runner and a stable Service ID",
		)
	}
	request := c.NewRequest(
		http.MethodPost, "/api/v1/services/"+serviceID+"/images/fetch", nil,
		apiTypes.ImageFetchRequest{Image: image, After: after}, http.StatusAccepted,
	)
	if key != "" {
		if !idempotencyKeyPattern.MatchString(key) {
			return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "invalid image fetch idempotency key")
		}
		request.IdempotencyKey = key
	}
	var accepted apiTypes.TaskAccepted
	if err := c.Do(ctx, request, &accepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindInternal, "scoped image fetch response is incomplete")
	}
	return accepted, nil
}
