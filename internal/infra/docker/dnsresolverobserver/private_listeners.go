package dnsresolverobserver

import (
	"context"
	"net"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A successful loopback query cannot prove a newly bound bridge listener. Both
// transports must return the captured local answer before configuration is ACKed.
func (executor *Executor) observePrivateListeners(ctx context.Context, parsed parsedArtifact) error {
	if len(parsed.privateListeners) != 0 && parsed.staticName == "" {
		return errs.New(errs.KindStateConflict, "DNS private listeners have no captured local answer to verify")
	}
	for _, address := range parsed.privateListeners {
		endpoint := net.JoinHostPort(address.String(), "53")
		for _, tcp := range []bool{false, true} {
			queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			var proof *agentpb.DNSQueryProof
			var err error
			if tcp {
				proof, err = executor.dns.QueryTCP(
					queryCtx,
					endpoint,
					absoluteName(parsed.staticName),
					agentpb.DNSQueryType_DNS_QUERY_TYPE_A,
				)
			} else {
				proof, err = executor.dns.Query(queryCtx, endpoint, absoluteName(parsed.staticName), agentpb.DNSQueryType_DNS_QUERY_TYPE_A)
			}
			cancel()
			if err != nil {
				return errs.WrapJoined(
					errs.KindRequestFailed,
					errs.Newf(
						errs.KindRequestFailed,
						"coredns private listener %s is not answering on %s",
						endpoint,
						listenerTransport(tcp),
					),
					err,
				)
			}
			if !proofHasIPv4(proof, parsed.staticAddress) {
				return errs.Newf(
					errs.KindStateConflict,
					"coredns private listener %s returned a different local DNS answer on %s",
					endpoint,
					listenerTransport(tcp),
				)
			}
		}
	}
	return nil
}

func listenerTransport(tcp bool) string {
	if tcp {
		return "TCP"
	}
	return "UDP"
}
