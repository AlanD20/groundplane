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
	Hosts      []Host
	Forwarders []Forwarder
	CatchAll   []ResolverEndpoint
}

type Renderer interface {
	Render(RenderInput) ([]byte, error)
	Digest(RenderInput) ([sha256.Size]byte, error)
}

func CloneRenderInput(input RenderInput) RenderInput {
	cloned := RenderInput{
		Hosts:      make([]Host, len(input.Hosts)),
		Forwarders: make([]Forwarder, len(input.Forwarders)),
		CatchAll:   append([]ResolverEndpoint(nil), input.CatchAll...),
	}
	for index, host := range input.Hosts {
		cloned.Hosts[index] = Host{
			Address: host.Address,
			Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	for index, forwarder := range input.Forwarders {
		cloned.Forwarders[index] = Forwarder{
			Domain: forwarder.Domain,
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

type TaskStep string

const (
	TaskStepValidateConfig TaskStep = "validate_config"
	TaskStepRender         TaskStep = "render_corefile"
	TaskStepApply          TaskStep = "validate_before_reload"
	TaskStepObserve        TaskStep = "observe"
)

type TaskPlan struct {
	ComponentID    string
	ServiceID      string
	Corefile       []byte
	CorefileSHA256 [sha256.Size]byte
	InputSHA256    [sha256.Size]byte
	Steps          []TaskStep
}

func (plan TaskPlan) Validate() error {
	if plan.ComponentID == "" || plan.ServiceID == "" || len(plan.Corefile) == 0 || len(plan.Steps) != 4 {
		return fmt.Errorf("dns resolver: task plan is incomplete")
	}
	if plan.Steps[0] != TaskStepValidateConfig || plan.Steps[1] != TaskStepRender ||
		plan.Steps[2] != TaskStepApply || plan.Steps[3] != TaskStepObserve {
		return fmt.Errorf("dns resolver: task steps are not in deterministic order")
	}
	if sha256.Sum256(plan.Corefile) != plan.CorefileSHA256 {
		return fmt.Errorf("dns resolver: task Corefile digest does not match bytes")
	}
	return nil
}

type ObservedState struct {
	ComponentID         string
	ServiceID           string
	Enabled             bool
	Healthy             bool
	CorefileSHA256      [sha256.Size]byte
	InputSHA256         [sha256.Size]byte
	DesiredGeneration   uint64
	RenderGeneration    uint64
	AgentID             string
	AgentGeneration     uint64
	BaselineGeneration  uint64
	OwnershipGeneration uint64
}

func Observe(plan TaskPlan, healthy bool) (ObservedState, error) {
	if err := plan.Validate(); err != nil {
		return ObservedState{}, err
	}
	return ObservedState{
		ComponentID: plan.ComponentID,
		ServiceID: plan.ServiceID,
		Enabled: true,
		Healthy: healthy,
		CorefileSHA256: plan.CorefileSHA256,
		InputSHA256: plan.InputSHA256,
	}, nil
}
