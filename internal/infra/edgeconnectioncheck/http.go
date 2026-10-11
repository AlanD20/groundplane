package edgeconnectioncheck

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
)

func newTransport() *http.Transport {
	return &http.Transport{
		DialContext:         (&net.Dialer{Timeout: contract.StageTimeout}).DialContext,
		TLSHandshakeTimeout: contract.StageTimeout, ResponseHeaderTimeout: contract.StageTimeout,
		DisableKeepAlives: true, MaxResponseHeaderBytes: 16 << 10,
	}
}

func boundedClient(transport *http.Transport) *http.Client {
	return &http.Client{Transport: transport, Timeout: contract.StageTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func classifyFailure(err error) contract.Failure {
	if errors.Is(err, context.Canceled) {
		return contract.Canceled
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout() {
		return contract.TimeoutFailure
	}
	var certificateError *tls.CertificateVerificationError
	if errors.As(err, &certificateError) {
		return contract.TLSFailure
	}
	return contract.ConnectionFailure
}
