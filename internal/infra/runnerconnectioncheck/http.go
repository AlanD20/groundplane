package runnerconnectioncheck

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"

	contract "github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newTransport() *http.Transport {
	return &http.Transport{DialContext: (&net.Dialer{Timeout: contract.StageTimeout}).DialContext,
		TLSHandshakeTimeout: contract.StageTimeout, ResponseHeaderTimeout: contract.StageTimeout,
		DisableKeepAlives: true, MaxResponseHeaderBytes: 16 << 10}
}

func boundedClient(transport *http.Transport) *http.Client {
	return &http.Client{Transport: transport, Timeout: contract.StageTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func publicTLSClient() (*http.Client, error) {
	// The Runner's SSL_CERT_FILE points to the private registry CA. GitHub
	// uses the image's ordinary public trust bundle, not that private CA.
	certificate, err := readFile("/etc/ssl/certs/ca-certificates.crt", 1<<20)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return nil, errs.New(errs.KindStateConflict, "Runner public trust bundle is invalid")
	}
	transport := newTransport()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	return boundedClient(transport), nil
}

func get(ctx context.Context, client *http.Client, target, authorization string, maximum int64) (int, []byte, string) {
	if ctx.Err() != nil {
		return 0, nil, connectionFailure(ctx.Err())
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, nil, "protocol"
	}
	request.Header.Set("User-Agent", "Groundplane-Runner-Connection-Check")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, connectionFailure(err)
	}
	defer response.Body.Close()
	if maximum == 0 {
		return response.StatusCode, nil, ""
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return response.StatusCode, nil, connectionFailure(err)
	}
	if int64(len(body)) > maximum {
		return response.StatusCode, nil, "protocol"
	}
	return response.StatusCode, body, ""
}

func connectionFailure(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout() {
		return "timeout"
	}
	var certificateError *tls.CertificateVerificationError
	if errors.As(err, &certificateError) {
		return "tls"
	}
	return "connection"
}

func statusFailure(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "authentication"
	case http.StatusForbidden:
		return "permission"
	default:
		return "http_response"
	}
}

func readFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errs.New(errs.KindStateConflict, "Runner diagnostic file is not bounded regular content")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(value)) > maximum {
		clear(value)
		return nil, errs.New(errs.KindStateConflict, "Runner diagnostic file exceeds its bound")
	}
	return value, nil
}
