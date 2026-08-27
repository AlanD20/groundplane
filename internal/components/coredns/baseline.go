package coredns

import (
	"bufio"
	"bytes"
	"fmt"
	"net/netip"
	"strings"
)

const maximumResolverBaselineBytes = 64 * 1024

// ResolverBaseline is the Agent-captured host resolver snapshot used by upstream_auto.
type ResolverBaseline struct {
	Generation uint64
	Resolvers  []ResolverEndpoint
}

// ParseResolverBaseline parses the bounded, authoritative host resolver
// snapshot captured by the Agent. Only nameserver directives are inputs; the
// renderer never reads the host filesystem itself.
func ParseResolverBaseline(content []byte) ([]ResolverEndpoint, error) {
	if len(content) == 0 || len(content) > maximumResolverBaselineBytes || !bytes.HasSuffix(content, []byte("\n")) {
		return nil, invalid("coredns: resolver baseline is empty, oversized, or missing a final LF")
	}
	resolvers := make([]ResolverEndpoint, 0, maxResolverEndpoints)
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "nameserver" {
			continue
		}
		if len(fields) != 2 {
			return nil, invalid("coredns: resolver baseline nameserver directive is invalid")
		}
		address, err := netip.ParseAddr(fields[1])
		if err != nil || address.Zone() != "" {
			return nil, invalid(fmt.Sprintf("coredns: resolver baseline nameserver %q is invalid", fields[1]))
		}
		resolvers = append(resolvers, ResolverEndpoint{Address: address})
	}
	if err := scanner.Err(); err != nil {
		return nil, invalid("coredns: resolver baseline cannot be read")
	}
	return normalizeResolvers(resolvers)
}
