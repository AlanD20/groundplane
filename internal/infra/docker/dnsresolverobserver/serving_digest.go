package dnsresolverobserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"time"

	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/net/dns/dnsmessage"
)

func (executor *Executor) observeServingDigest(ctx context.Context, request Request, digest string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	before, err := executor.readCounters(probeCtx, request.MetricsURL)
	if err != nil {
		return false
	}
	matches, err := executor.dns.MatchesServingDigest(probeCtx, request.ListenEndpoint, digest)
	if err != nil || !matches {
		return false
	}
	// Require a locally answered probe. A forwarded reply is not evidence
	// of the configuration loaded by the resolver.
	after, err := executor.readCounters(probeCtx, request.MetricsURL)
	return err == nil && maps.Equal(before, after)
}

func (prober networkDNSProber) MatchesServingDigest(ctx context.Context, endpoint, digest string) (bool, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	name := hex.EncodeToString(nonce[:]) + "." + dnsresolver.ServingProofZone
	response, _, err := prober.roundTrip(ctx, endpoint, name, dnsmessage.TypeTXT, true, false)
	if err != nil {
		return false, err
	}
	if response.RCode != dnsmessage.RCodeSuccess || !response.Authoritative || response.RecursionDesired ||
		response.RecursionAvailable || len(response.Answers) != 1 ||
		len(response.Authorities) != 0 || len(response.Additionals) != 0 {
		return false, nil
	}
	answer := response.Answers[0]
	if answer.Header.Name != response.Questions[0].Name || answer.Header.Type != dnsmessage.TypeTXT ||
		answer.Header.Class != dnsmessage.ClassINET || answer.Header.TTL != 0 {
		return false, nil
	}
	value, ok := answer.Body.(*dnsmessage.TXTResource)
	return ok && value != nil && len(value.TXT) == 1 && value.TXT[0] == digest, nil
}
