package edgeconnectioncheck

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"strconv"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckPublic performs one HTTPS request using one captured public DNS address.
// It does not follow redirects, use proxy environment variables, authenticate or
// download a body. TLS and HTTP share the same connection, so a second DNS lookup
// cannot rebind the public name to a private address between those stages.
func CheckPublic(ctx context.Context, target contract.PublicTarget) (contract.PublicResult, error) {
	if ctx == nil || target.Validate() != nil {
		return contract.PublicResult{}, errs.New(errs.KindValidationFailed, "public Route diagnostic target is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, contract.PublicTimeout)
	defer cancel()
	result := contract.PublicResult{Target: target, StartedAt: time.Now().UTC(), Addresses: []string{},
		Stages: []contract.Stage{
			{Name: contract.DNS, Outcome: contract.Failed},
			{Name: contract.TLS, Outcome: contract.Skipped},
			{Name: contract.HTTP, Outcome: contract.Skipped},
		}}
	deferFinish := func() (contract.PublicResult, error) {
		result.FinishedAt = time.Now().UTC()
		return result, result.Validate(target)
	}
	dnsStarted := time.Now()
	dnsContext, stopDNS := context.WithTimeout(ctx, contract.StageTimeout)
	addresses, err := (&net.Resolver{PreferGo: true}).LookupNetIP(dnsContext, "ip", target.Hostname)
	stopDNS()
	result.Stages[0].Elapsed = time.Since(dnsStarted)
	if err != nil {
		failure := classifyFailure(err)
		if failure == contract.ConnectionFailure {
			failure = contract.DNSFailure
		}
		result.Stages[0].Failure = failure
		return deferFinish()
	}
	if len(addresses) == 0 || len(addresses) > contract.MaximumAddresses {
		result.Stages[0].Failure = contract.DNSFailure
		return deferFinish()
	}
	seen := make(map[netip.Addr]bool, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !contract.PublicAddress(address) {
			result.Addresses = []string{}
			result.Stages[0].Failure = contract.UnsafeAddress
			return deferFinish()
		}
		if !seen[address] {
			result.Addresses = append(result.Addresses, address.String())
			seen[address] = true
		}
	}
	// Prefer IPv4 when both families are advertised; the result says exactly
	// which address was tested and never claims every advertised edge works.
	result.SelectedAddress = result.Addresses[0]
	for _, value := range result.Addresses {
		address, err := netip.ParseAddr(value)
		if err == nil && address.Is4() {
			result.SelectedAddress = value
			break
		}
	}
	result.Stages[0].Outcome = contract.Passed
	started := time.Now()
	tlsContext, stopTLS := context.WithTimeout(ctx, contract.StageTimeout)
	connection, err := (&net.Dialer{Timeout: contract.StageTimeout}).DialContext(
		tlsContext,
		"tcp",
		net.JoinHostPort(result.SelectedAddress, strconv.Itoa(443)),
	)
	if err != nil {
		stopTLS()
		result.Stages[1] = contract.Stage{
			Name:    contract.TLS,
			Outcome: contract.Failed,
			Failure: classifyFailure(err),
			Elapsed: time.Since(started),
		}
		return deferFinish()
	}
	defer connection.Close()
	secure := tls.Client(
		connection,
		&tls.Config{ServerName: target.Hostname, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
	)
	err = secure.HandshakeContext(tlsContext)
	stopTLS()
	if err != nil {
		failure := classifyFailure(err)
		if failure == contract.ConnectionFailure {
			failure = contract.TLSFailure
		}
		result.Stages[1] = contract.Stage{
			Name:    contract.TLS,
			Outcome: contract.Failed,
			Failure: failure,
			Elapsed: time.Since(started),
		}
		return deferFinish()
	}
	state := secure.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		result.Stages[1] = contract.Stage{
			Name:    contract.TLS,
			Outcome: contract.Failed,
			Failure: contract.TLSFailure,
			Elapsed: time.Since(started),
		}
		return deferFinish()
	}
	result.TLSVersion = state.Version
	expires := state.PeerCertificates[0].NotAfter.UTC()
	result.CertificateExpiresAt = &expires
	result.Stages[1] = contract.Stage{Name: contract.TLS, Outcome: contract.Passed, Elapsed: time.Since(started)}
	transport := newTransport()
	defer transport.CloseIdleConnections()
	used := false
	transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if used || network != "tcp" || address != net.JoinHostPort(target.Hostname, "443") {
			return nil, errs.New(errs.KindStateConflict, "public Route diagnostic connection cannot be reused")
		}
		used = true
		return secure, nil
	}
	result.Stages[2] = checkHTTP(
		ctx,
		boundedClient(transport),
		contract.HTTP,
		"https://"+target.Hostname+target.Path,
		"",
	)
	return deferFinish()
}
