package dnsresolver

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/netip"
	"sort"
	"strings"
)

type ResolverEndpoint struct {
	Address netip.Addr
	Port    uint16
}

type Host struct {
	Address   netip.Addr
	Hostnames []string
}

type Forwarder struct {
	Domain    string
	Resolvers []ResolverEndpoint
}

type RenderInput struct {
	CorefileTemplate string
	Hosts            []Host
	Forwarders       []Forwarder
	CatchAll         []ResolverEndpoint
}

type Renderer interface {
	Render(RenderInput) ([]byte, error)
	Digest(RenderInput) ([sha256.Size]byte, error)
}

func CloneRenderInput(input RenderInput) RenderInput {
	cloned := RenderInput{
		CorefileTemplate: input.CorefileTemplate,
		Hosts:            make([]Host, len(input.Hosts)),
		Forwarders:       make([]Forwarder, len(input.Forwarders)),
		CatchAll:         append([]ResolverEndpoint(nil), input.CatchAll...),
	}
	for index, host := range input.Hosts {
		cloned.Hosts[index] = Host{
			Address:   host.Address,
			Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	for index, forwarder := range input.Forwarders {
		cloned.Forwarders[index] = Forwarder{
			Domain:    forwarder.Domain,
			Resolvers: append([]ResolverEndpoint(nil), forwarder.Resolvers...),
		}
	}
	return cloned
}

type ResolverBaseline struct {
	Generation uint64
	Resolvers  []ResolverEndpoint
}

const maximumResolverBaselineBytes = 64 * 1024

func ParseResolverBaseline(content []byte) ([]ResolverEndpoint, error) {
	if len(content) == 0 || len(content) > maximumResolverBaselineBytes || !bytes.HasSuffix(content, []byte("\n")) {
		return nil, fmt.Errorf("dns resolver: baseline is empty, oversized, or missing a final LF")
	}
	resolvers := make([]ResolverEndpoint, 0, 15)
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
			return nil, fmt.Errorf("dns resolver: baseline nameserver directive is invalid")
		}
		address, err := netip.ParseAddr(fields[1])
		if err != nil || address.Is4In6() || address.Zone() != "" || address.IsUnspecified() || address.IsMulticast() ||
			address.IsLoopback() && address != netip.MustParseAddr("127.0.0.53") {
			return nil, fmt.Errorf("dns resolver: baseline nameserver %q is invalid", fields[1])
		}
		resolvers = append(resolvers, ResolverEndpoint{Address: address, Port: 53})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("dns resolver: baseline cannot be read")
	}
	if len(resolvers) == 0 || len(resolvers) > 15 {
		return nil, fmt.Errorf("dns resolver: baseline must contain between 1 and 15 nameservers")
	}
	seen := make(map[netip.Addr]struct{}, len(resolvers))
	for _, resolver := range resolvers {
		if _, duplicate := seen[resolver.Address]; duplicate {
			return nil, fmt.Errorf("dns resolver: baseline nameserver is duplicated")
		}
		seen[resolver.Address] = struct{}{}
	}
	sort.Slice(resolvers, func(left, right int) bool {
		return resolvers[left].Address.Compare(resolvers[right].Address) < 0
	})
	return resolvers, nil
}

// Intent is the immutable, provider-neutral output of DNS capability
// planning. Artifact content is rendered by the registered Component and is
// represented here only by its digest and length.
type Intent struct {
	ComponentID    string
	ServiceID      string
	ArtifactSHA256 [sha256.Size]byte
	ArtifactLength uint64
	InputSHA256    [sha256.Size]byte
	PlanSHA256     [sha256.Size]byte
}

func (intent Intent) Validate() error {
	if intent.ComponentID == "" || intent.ServiceID == "" || intent.ArtifactLength == 0 {
		return fmt.Errorf("dns resolver: intent is incomplete")
	}
	var zero [sha256.Size]byte
	if intent.ArtifactSHA256 == zero || intent.InputSHA256 == zero || intent.PlanSHA256 == zero {
		return fmt.Errorf("dns resolver: intent digests are required")
	}
	return nil
}
