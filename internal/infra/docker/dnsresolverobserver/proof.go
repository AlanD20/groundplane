package dnsresolverobserver

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) observeStatic(
	ctx context.Context,
	request Request,
	parsed parsedArtifact,
) (*agentpb.DNSQueryProof, error) {
	for attempt := uint32(1); attempt <= maximumProofAttempts; attempt++ {
		before, err := executor.readCounters(ctx, request.MetricsURL)
		if err != nil {
			continue
		}
		proof, err := executor.dns.Query(
			ctx, request.ListenEndpoint, absoluteName(parsed.staticName), agentpb.DNSQueryType_DNS_QUERY_TYPE_A,
		)
		if err != nil {
			continue
		}
		after, err := executor.readCounters(ctx, request.MetricsURL)
		if err != nil || counterDelta(before, after) != 0 || !proofHasIPv4(proof, parsed.staticAddress) {
			continue
		}
		proof.Attempts = attempt
		proof.Counters = counterEvidence(before, after, parsed.allEndpoints())
		return proof, nil
	}
	return nil, errs.New(errs.KindRequestFailed, "DNS resolver static serving proof failed")
}

func (executor *Executor) observeForward(
	ctx context.Context,
	request Request,
	group forwardGroup,
	name string,
	queryType agentpb.DNSQueryType,
) (*agentpb.DNSQueryProof, error) {
	requiresRecursiveRoot := absoluteName(name) == "." && queryType == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS
	for attempt := uint32(1); attempt <= maximumProofAttempts; attempt++ {
		before, err := executor.readCounters(ctx, request.MetricsURL)
		if err != nil {
			continue
		}
		local, err := executor.dns.Query(ctx, request.ListenEndpoint, absoluteName(name), queryType)
		if err != nil || (requiresRecursiveRoot && !proofHasRootNS(local)) {
			continue
		}
		after, err := executor.readCounters(ctx, request.MetricsURL)
		if err != nil {
			continue
		}
		selected, ok := selectedUpstream(before, after, group.endpoints)
		if !ok {
			continue
		}
		direct, err := executor.dns.Query(ctx, selected, absoluteName(name), queryType)
		if err != nil || direct.GetLocalRcode() != local.GetLocalRcode() ||
			(requiresRecursiveRoot && !proofHasRootNS(direct)) {
			continue
		}
		local.SelectedUpstream = selected
		local.DirectRcode = direct.GetLocalRcode()
		local.DirectUsedTcp = direct.GetLocalUsedTcp()
		local.Attempts = attempt
		local.Counters = counterEvidence(before, after, group.endpoints)
		return local, nil
	}
	return nil, errs.New(errs.KindRequestFailed, "DNS resolver forward serving proof failed")
}

func (executor *Executor) readCounters(ctx context.Context, endpoint string) (map[counterKey]uint64, error) {
	value, err := executor.metrics.Read(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer clear(value)
	return parseForwardCounters(value)
}
