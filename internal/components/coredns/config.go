package coredns

import (
	"fmt"
	"net/netip"
	"strconv"
)

// Config is the complete desired configuration for the platform CoreDNS
// component. It deliberately contains no observed state or rendered bytes.
type Config struct {
	UpstreamAuto      bool
	UpstreamResolvers []ResolverEndpoint
	Forwarders        []CoreDNSForwarder
	TailnetDelegation bool
}

var configKeys = map[string]struct{}{
	"upstream_auto":      {},
	"upstream_resolvers": {},
	"forwarders":         {},
	"tailnet_delegation": {},
}

// DecodeConfig parses the persisted/API map using the exact CoreDNS desired
// configuration shape. Unknown or missing keys are rejected so a typo cannot
// silently select a different resolver policy.
func DecodeConfig(raw map[string]any) (Config, error) {
	if raw == nil {
		return Config{}, invalid("coredns: complete desired config is required")
	}
	for key := range raw {
		if _, ok := configKeys[key]; !ok {
			return Config{}, invalid(fmt.Sprintf("coredns: unknown config key %q", key))
		}
	}
	for key := range configKeys {
		if _, ok := raw[key]; !ok {
			return Config{}, invalid(fmt.Sprintf("coredns: config key %q is required", key))
		}
	}

	upstreamAuto, ok := raw["upstream_auto"].(bool)
	if !ok {
		return Config{}, invalid("coredns: upstream_auto must be a boolean")
	}
	tailnetDelegation, ok := raw["tailnet_delegation"].(bool)
	if !ok {
		return Config{}, invalid("coredns: tailnet_delegation must be a boolean")
	}
	upstreamResolvers, err := decodeResolvers(raw["upstream_resolvers"])
	if err != nil {
		return Config{}, err
	}
	forwarders, err := decodeForwarders(raw["forwarders"])
	if err != nil {
		return Config{}, err
	}
	config := Config{
		UpstreamAuto:      upstreamAuto,
		UpstreamResolvers: upstreamResolvers,
		Forwarders:        forwarders,
		TailnetDelegation: tailnetDelegation,
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// ConfigFromMap is the descriptive alias used by controller callers.
func ConfigFromMap(raw map[string]any) (Config, error) { return DecodeConfig(raw) }

// ValidateConfig validates a complete persisted/API desired configuration.
func ValidateConfig(raw map[string]any) error {
	_, err := DecodeConfig(raw)
	return err
}

// ValidateDesiredConfig is the capability-facing name for ValidateConfig.
func ValidateDesiredConfig(raw map[string]any) error { return ValidateConfig(raw) }

// Validate enforces semantic constraints after type decoding. Auto mode gets
// its catch-all resolvers from the host baseline; explicit mode must carry its
// own usable resolver list.
func (config Config) Validate() error {
	if config.UpstreamAuto {
		if len(config.UpstreamResolvers) != 0 {
			return invalid("coredns: upstream_resolvers must be empty when upstream_auto is enabled")
		}
	} else if _, err := normalizeResolvers(config.UpstreamResolvers); err != nil {
		return err
	}
	if _, err := normalizeForwarders(config.Forwarders); err != nil && len(config.Forwarders) > 0 {
		return err
	}
	if config.TailnetDelegation {
		for _, forwarder := range config.Forwarders {
			if forwarder.Domain == "ts.net" {
				return invalid("coredns: ts.net is managed when tailnet_delegation is enabled")
			}
		}
	}
	return nil
}

// ToMap returns the canonical shape suitable for persistence or an API
// response. It is intentionally a fresh map and cannot mutate desired state.
func (config Config) ToMap() map[string]any {
	resolvers := make([]any, 0, len(config.UpstreamResolvers))
	for _, resolver := range config.UpstreamResolvers {
		resolvers = append(resolvers, resolverText(resolver))
	}
	forwarders := make([]any, 0, len(config.Forwarders))
	for _, forwarder := range config.Forwarders {
		forwarderResolvers := make([]any, 0, len(forwarder.Resolvers))
		for _, resolver := range forwarder.Resolvers {
			forwarderResolvers = append(forwarderResolvers, resolverText(resolver))
		}
		forwarders = append(forwarders, map[string]any{
			"domain":    forwarder.Domain,
			"resolvers": forwarderResolvers,
		})
	}
	return map[string]any{
		"upstream_auto":      config.UpstreamAuto,
		"upstream_resolvers": resolvers,
		"forwarders":         forwarders,
		"tailnet_delegation": config.TailnetDelegation,
	}
}

func decodeResolvers(raw any) ([]ResolverEndpoint, error) {
	var values []any
	switch typed := raw.(type) {
	case []any:
		values = typed
	case []string:
		values = make([]any, len(typed))
		for index := range typed {
			values[index] = typed[index]
		}
	case []ResolverEndpoint:
		return append([]ResolverEndpoint(nil), typed...), nil
	default:
		return nil, invalid("coredns: resolver list must be an array")
	}
	result := make([]ResolverEndpoint, 0, len(values))
	for _, value := range values {
		resolver, err := decodeResolver(value)
		if err != nil {
			return nil, err
		}
		result = append(result, resolver)
	}
	return result, nil
}

func decodeResolver(raw any) (ResolverEndpoint, error) {
	switch value := raw.(type) {
	case string:
		if address, err := netip.ParseAddr(value); err == nil {
			return ResolverEndpoint{Address: address}, nil
		}
		if address, err := netip.ParseAddrPort(value); err == nil {
			return ResolverEndpoint{Address: address.Addr(), Port: address.Port()}, nil
		}
		return ResolverEndpoint{}, invalid("coredns: resolver endpoint must be an IP address or address:port")
	case ResolverEndpoint:
		return value, nil
	case map[string]any:
		if len(value) < 1 || len(value) > 2 {
			return ResolverEndpoint{}, invalid("coredns: resolver object has unknown keys")
		}
		addressText, ok := value["address"].(string)
		if !ok {
			return ResolverEndpoint{}, invalid("coredns: resolver address must be a string")
		}
		address, err := netip.ParseAddr(addressText)
		if err != nil {
			return ResolverEndpoint{}, invalid("coredns: resolver address is invalid")
		}
		port, err := decodePort(value["port"])
		if err != nil {
			return ResolverEndpoint{}, err
		}
		if _, ok := value["port"]; !ok {
			port = 0
		}
		return ResolverEndpoint{Address: address, Port: port}, nil
	default:
		return ResolverEndpoint{}, invalid("coredns: resolver endpoint has an invalid type")
	}
}

func decodeForwarders(raw any) ([]CoreDNSForwarder, error) {
	var values []any
	switch typed := raw.(type) {
	case []any:
		values = typed
	case []CoreDNSForwarder:
		return append([]CoreDNSForwarder(nil), typed...), nil
	default:
		return nil, invalid("coredns: forwarders must be an array")
	}
	result := make([]CoreDNSForwarder, 0, len(values))
	for _, value := range values {
		fields, ok := value.(map[string]any)
		if !ok || len(fields) != 2 {
			return nil, invalid("coredns: forwarder must contain only domain and resolvers")
		}
		domain, ok := fields["domain"].(string)
		if !ok {
			return nil, invalid("coredns: forwarder domain must be a string")
		}
		resolvers, err := decodeResolvers(fields["resolvers"])
		if err != nil {
			return nil, err
		}
		result = append(result, CoreDNSForwarder{Domain: domain, Resolvers: resolvers})
	}
	return result, nil
}

func decodePort(raw any) (uint16, error) {
	if raw == nil {
		return 0, invalid("coredns: resolver port must be an integer")
	}
	var value uint64
	switch typed := raw.(type) {
	case uint16:
		return typed, nil
	case uint64:
		value = typed
	case uint32:
		value = uint64(typed)
	case uint:
		value = uint64(typed)
	case int:
		if typed < 0 {
			return 0, invalid("coredns: resolver port must be between 1 and 65535")
		}
		value = uint64(typed)
	case int64:
		if typed < 0 {
			return 0, invalid("coredns: resolver port must be between 1 and 65535")
		}
		value = uint64(typed)
	case float64:
		if typed < 0 || typed != float64(uint64(typed)) {
			return 0, invalid("coredns: resolver port must be an integer")
		}
		value = uint64(typed)
	case string:
		parsed, err := strconv.ParseUint(typed, 10, 16)
		if err != nil {
			return 0, invalid("coredns: resolver port must be between 1 and 65535")
		}
		value = parsed
	default:
		return 0, invalid("coredns: resolver port must be an integer")
	}
	if value > 65535 {
		return 0, invalid("coredns: resolver port must be between 1 and 65535")
	}
	return uint16(value), nil
}

func resolverText(endpoint ResolverEndpoint) string {
	port := endpoint.Port
	if port == 0 || port == 53 {
		return endpoint.Address.String()
	}
	return netip.AddrPortFrom(endpoint.Address, port).String()
}
