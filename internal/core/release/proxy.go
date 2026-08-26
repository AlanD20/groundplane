package release

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumProxyPorts = 32

type ProxyConfig struct {
	JSON   []byte
	SHA256 [sha256.Size]byte
}

func ProxyPorts(exposures []string) ([]uint16, error) {
	ports := make([]uint16, 0, len(exposures))
	seen := make(map[uint16]struct{}, len(exposures))
	for _, raw := range exposures {
		value := strings.TrimSpace(raw)
		if strings.HasSuffix(value, "/udp") {
			return nil, errs.New(errs.KindValidationFailed, "release proxy does not support UDP exposure")
		}
		value = strings.TrimSuffix(value, "/tcp")
		if colon := strings.LastIndexByte(value, ':'); colon >= 0 {
			value = value[colon+1:]
		}
		if strings.Contains(value, "-") {
			return nil, errs.New(errs.KindValidationFailed, "release proxy requires individual TCP ports")
		}
		parsed, err := strconv.ParseUint(value, 10, 16)
		if err != nil || parsed == 0 {
			return nil, errs.New(errs.KindValidationFailed, "release proxy exposure is invalid")
		}
		port := uint16(parsed)
		if _, duplicate := seen[port]; duplicate {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	if len(ports) == 0 || len(ports) > MaximumProxyPorts {
		return nil, errs.New(errs.KindValidationFailed, "release proxy requires between 1 and 32 TCP ports")
	}
	slices.Sort(ports)
	return ports, nil
}

func RenderProxyConfig(serviceName, releaseID string, target WorkloadTarget, generation uint64, ports []uint16) (ProxyConfig, error) {
	if serviceName == "" || strings.ContainsAny(serviceName, "/\\\x00") ||
		target.Validate() != nil || generation == 0 || releaseID == "" ||
		len(ports) == 0 || len(ports) > MaximumProxyPorts {
		return ProxyConfig{}, errs.New(errs.KindValidationFailed, "release proxy config identity is invalid")
	}
	workloadName, err := WorkloadComposeName(serviceName, target)
	if err != nil {
		return ProxyConfig{}, err
	}
	servers := make(map[string]any, len(ports))
	for _, port := range ports {
		if port == 0 {
			return ProxyConfig{}, errs.New(errs.KindValidationFailed, "release proxy port is invalid")
		}
		name := fmt.Sprintf("gp_g%d_%s_p%d", generation, strings.ToLower(releaseID), port)
		servers[name] = map[string]any{
			"listen": []string{fmt.Sprintf(":%d", port)},
			"routes": []any{map[string]any{
				"handle": []any{map[string]any{
					"handler": "reverse_proxy",
					"upstreams": []any{map[string]any{
						"dial": fmt.Sprintf("%s:%d", workloadName, port),
					}},
				}},
			}},
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"admin": map[string]any{"listen": "127.0.0.1:2019"},
		"apps":  map[string]any{"http": map[string]any{"servers": servers}},
	})
	if err != nil {
		return ProxyConfig{}, errs.Wrap(errs.KindInternal, err)
	}
	return ProxyConfig{JSON: encoded, SHA256: sha256.Sum256(encoded)}, nil
}
