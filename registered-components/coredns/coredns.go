package coredns

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
)

const (
	maxHostGroups        = 128
	maxHostnames         = 1024
	maxForwarders        = 8
	maxResolverEndpoints = 15
	ServiceName          = "coredns"
	CorefileSource       = "coredns/Corefile"
	CorefileTarget       = "/etc/groundplane/coredns/Corefile"
	ConfigDirectory      = "/etc/groundplane/coredns"
	ObserveServingAction = component.ActionID("observe-serving")
)

var Image = component.OCIImage{
	Repository:  "docker.io/coredns/coredns",
	IndexDigest: "9caabbf6238b189a65d0d6e6ac138de60d6a1c419e5a341fbbb7c78382559c6e",
	Platforms: []component.OCIPlatform{
		{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  "f0b8c589314ed010a0c326e987a52b50801f0145ac9b75423af1b5c66dbd6d50",
		},
		{
			OS:           "linux",
			Architecture: "arm64",
			Variant:      "v8",
			ChildDigest:  "31440a2bef59e2f1ffb600113b557103740ff851e27b0aef5b849f6e3ab994a6",
		},
	},
}

func ValidateConfigCommand() []string {
	return []string{"-conf", "/dev/stdin", "-dns.port", "0"}
}

type Renderer struct{}

type PlanInput struct {
	GeneratedServiceID string
	Render             dnsresolver.RenderInput
}

func Plan(input PlanInput) (component.EnvironmentPlan, error) {
	if input.GeneratedServiceID == "" {
		return component.EnvironmentPlan{}, fmt.Errorf("coredns: generated Service id is required")
	}
	corefile, err := (Renderer{}).Render(input.Render)
	if err != nil {
		return component.EnvironmentPlan{}, err
	}
	return component.CloneEnvironmentPlan(component.EnvironmentPlan{
		Services: []component.ManagedService{{
			ID: input.GeneratedServiceID, Name: ServiceName, Image: Image,
			NetworkMode: component.ManagedNetworkModeHost,
			Command:     []string{"-conf", CorefileTarget}, Restart: "unless-stopped", Replicas: 1,
			ObservationAction: ObserveServingAction,
			Mounts: []component.ManagedMount{{
				Source: "coredns", Target: ConfigDirectory,
				Kind: component.ManagedMountKindDirectory, ReadOnly: true,
			}},
		}},
		Files: []component.ManagedFile{{Path: CorefileSource, Content: corefile}},
	}), nil
}

func Definition() (component.Definition, error) {
	httpRouter, err := component.NewGrant(
		component.CapabilityHTTPRouter,
		component.OperationList,
		component.OperationRead,
	)
	if err != nil {
		return component.Definition{}, err
	}
	services, err := component.NewGrant(
		component.CapabilityServices,
		component.OperationRead,
		component.OperationCreate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	managedConfig, err := component.NewGrant(
		component.CapabilityManagedConfig,
		component.OperationConfigure,
		component.OperationActivate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	hostResolution, err := component.NewGrant(
		component.CapabilityHostResolution,
		component.OperationConfigure,
		component.OperationObserve,
	)
	if err != nil {
		return component.Definition{}, err
	}
	activate, err := component.NewActionDefinition(
		"activate-config",
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	observe, err := component.NewActionDefinition(
		ObserveServingAction,
		component.CapabilityHostResolution,
		component.OperationObserve,
	)
	if err != nil {
		return component.Definition{}, err
	}
	return component.NewDefinition(component.DefinitionInput{
		Implementation: "coredns",
		ConfigVariant:  "coredns-v1",
		Provides:       []component.Capability{component.CapabilityDNSResolver},
		Grants:         []component.Grant{httpRouter, services, managedConfig, hostResolution},
		OwnerScopes:    []component.OwnerScope{component.OwnerScopePlatform},
		Actions:        []component.ActionDefinition{activate, observe},
	})
}

func (Renderer) Render(input dnsresolver.RenderInput) ([]byte, error) {
	normalized, err := normalize(input)
	if err != nil {
		return nil, err
	}
	var output strings.Builder
	output.WriteString(".:53 {\n")
	output.WriteString("    bind 127.0.0.1\n")
	if len(normalized.Hosts) > 0 {
		output.WriteString("    hosts {\n")
		for _, host := range normalized.Hosts {
			output.WriteString("        ")
			output.WriteString(host.Address.String())
			for _, hostname := range host.Hostnames {
				output.WriteByte(' ')
				output.WriteString(hostname)
			}
			output.WriteByte('\n')
		}
		output.WriteString("        no_reverse\n")
		output.WriteString("        fallthrough\n")
		output.WriteString("    }\n")
	}
	for _, forwarder := range normalized.Forwarders {
		writeForward(&output, forwarder.Domain, forwarder.Resolvers)
	}
	writeForward(&output, ".", normalized.CatchAll)
	output.WriteString("    reload\n")
	output.WriteString("    prometheus 127.0.0.1:9153\n")
	output.WriteString("    log\n")
	output.WriteString("    errors\n")
	output.WriteString("}\n")
	return []byte(output.String()), nil
}

func (Renderer) Digest(input dnsresolver.RenderInput) ([sha256.Size]byte, error) {
	normalized, err := normalize(input)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("coredns: canonical input encoding failed")
	}
	return sha256.Sum256(encoded), nil
}

func normalize(input dnsresolver.RenderInput) (dnsresolver.RenderInput, error) {
	input = dnsresolver.CloneRenderInput(input)
	hosts, err := normalizeHosts(input.Hosts)
	if err != nil {
		return dnsresolver.RenderInput{}, err
	}
	forwarders, err := normalizeForwarders(input.Forwarders)
	if err != nil {
		return dnsresolver.RenderInput{}, err
	}
	catchAll, err := normalizeResolvers(input.CatchAll)
	if err != nil {
		return dnsresolver.RenderInput{}, err
	}
	return dnsresolver.RenderInput{Hosts: hosts, Forwarders: forwarders, CatchAll: catchAll}, nil
}

func normalizeHosts(input []dnsresolver.Host) ([]dnsresolver.Host, error) {
	if len(input) > maxHostGroups {
		return nil, fmt.Errorf("coredns: too many static host address groups")
	}
	byAddress := make(map[netip.Addr]map[string]struct{}, len(input))
	addressByHostname := make(map[string]netip.Addr)
	for _, host := range input {
		if !host.Address.IsValid() || !host.Address.Is4() || host.Address.Is4In6() ||
			host.Address.IsUnspecified() || host.Address.IsMulticast() || len(host.Hostnames) == 0 {
			return nil, fmt.Errorf("coredns: static host group is invalid")
		}
		names := byAddress[host.Address]
		if names == nil {
			names = make(map[string]struct{}, len(host.Hostnames))
			byAddress[host.Address] = names
		}
		for _, hostname := range host.Hostnames {
			if !validDNSName(hostname, 253) {
				return nil, fmt.Errorf("coredns: static hostname is not a canonical Route DNS name")
			}
			if address, exists := addressByHostname[hostname]; exists && address != host.Address {
				return nil, fmt.Errorf("coredns: hostname maps to more than one HTTP router address")
			}
			addressByHostname[hostname] = host.Address
			names[hostname] = struct{}{}
		}
	}
	if len(byAddress) > maxHostGroups || len(addressByHostname) > maxHostnames {
		return nil, fmt.Errorf("coredns: static host limits exceeded")
	}
	hosts := make([]dnsresolver.Host, 0, len(byAddress))
	for address, names := range byAddress {
		hostnames := make([]string, 0, len(names))
		for hostname := range names {
			hostnames = append(hostnames, hostname)
		}
		sort.Strings(hostnames)
		hosts = append(hosts, dnsresolver.Host{Address: address, Hostnames: hostnames})
	}
	sort.Slice(hosts, func(left, right int) bool {
		return hosts[left].Address.Compare(hosts[right].Address) < 0
	})
	return hosts, nil
}

func normalizeForwarders(input []dnsresolver.Forwarder) ([]dnsresolver.Forwarder, error) {
	if len(input) > maxForwarders {
		return nil, fmt.Errorf("coredns: too many domain forwarders")
	}
	seen := make(map[string]struct{}, len(input))
	forwarders := make([]dnsresolver.Forwarder, 0, len(input))
	for _, forwarder := range input {
		if !validDNSName(forwarder.Domain, 253) {
			return nil, fmt.Errorf("coredns: forwarder domain is not canonical")
		}
		if _, duplicate := seen[forwarder.Domain]; duplicate {
			return nil, fmt.Errorf("coredns: forwarder domain is duplicated")
		}
		seen[forwarder.Domain] = struct{}{}
		resolvers, err := normalizeResolvers(forwarder.Resolvers)
		if err != nil {
			return nil, err
		}
		forwarders = append(forwarders, dnsresolver.Forwarder{Domain: forwarder.Domain, Resolvers: resolvers})
	}
	sort.Slice(forwarders, func(left, right int) bool {
		leftLabels := strings.Count(forwarders[left].Domain, ".") + 1
		rightLabels := strings.Count(forwarders[right].Domain, ".") + 1
		if leftLabels != rightLabels {
			return leftLabels > rightLabels
		}
		return forwarders[left].Domain < forwarders[right].Domain
	})
	return forwarders, nil
}

func normalizeResolvers(input []dnsresolver.ResolverEndpoint) ([]dnsresolver.ResolverEndpoint, error) {
	if len(input) == 0 || len(input) > maxResolverEndpoints {
		return nil, fmt.Errorf("coredns: resolver list must contain between 1 and 15 endpoints")
	}
	seen := make(map[netip.AddrPort]struct{}, len(input))
	resolvers := make([]dnsresolver.ResolverEndpoint, 0, len(input))
	for _, endpoint := range input {
		address := endpoint.Address
		if !address.IsValid() || address.Is4In6() || address.Zone() != "" || address.IsUnspecified() ||
			address.IsLoopback() && address != netip.MustParseAddr("127.0.0.53") || address.IsMulticast() {
			return nil, fmt.Errorf("coredns: resolver endpoint is not usable")
		}
		port := endpoint.Port
		if port == 0 {
			port = 53
		}
		key := netip.AddrPortFrom(address, port)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("coredns: resolver endpoint is duplicated")
		}
		seen[key] = struct{}{}
		resolvers = append(resolvers, dnsresolver.ResolverEndpoint{Address: address, Port: port})
	}
	sort.Slice(resolvers, func(left, right int) bool {
		if comparison := resolvers[left].Address.Compare(resolvers[right].Address); comparison != 0 {
			return comparison < 0
		}
		return resolvers[left].Port < resolvers[right].Port
	})
	return resolvers, nil
}

func validDNSName(value string, maximum int) bool {
	if maximum <= 0 || value == "" || len(value) > maximum || value[len(value)-1] == '.' {
		return false
	}
	if _, err := netip.ParseAddr(value); err == nil {
		return false
	}
	start := 0
	for index := 0; index <= len(value); index++ {
		if index < len(value) && value[index] != '.' {
			continue
		}
		label := value[start:index]
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for characterIndex := range len(label) {
			character := label[characterIndex]
			if character < 'a' || character > 'z' {
				if character < '0' || character > '9' {
					if character != '-' {
						return false
					}
				}
			}
		}
		start = index + 1
	}
	return true
}

func writeForward(output *strings.Builder, domain string, resolvers []dnsresolver.ResolverEndpoint) {
	output.WriteString("    forward ")
	output.WriteString(domain)
	for _, endpoint := range resolvers {
		output.WriteByte(' ')
		if endpoint.Port == 53 {
			output.WriteString(endpoint.Address.String())
		} else {
			output.WriteString(netip.AddrPortFrom(endpoint.Address, endpoint.Port).String())
		}
	}
	output.WriteByte('\n')
}
