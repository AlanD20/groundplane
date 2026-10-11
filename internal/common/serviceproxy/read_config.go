package serviceproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumConfigurationBytes = 1024 * 1024

// Read opens only the selected implementation's complete canonical document.
// It checks the actual listeners and upstreams, not merely a generation label.
// The returned routing set is evidence from these bytes, never current intent.
func Read(implementation Implementation, value []byte) (Configuration, error) {
	if len(value) == 0 || len(value) > MaximumConfigurationBytes {
		return Configuration{}, invalidConfiguration()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var configuration Configuration
	var err error
	switch implementation {
	case Caddy:
		var document caddyDocument
		if decoder.Decode(&document) != nil {
			return Configuration{}, invalidConfiguration()
		}
		configuration, err = readCaddy(document)
	case Envoy:
		var document envoyDocument
		if decoder.Decode(&document) != nil {
			return Configuration{}, invalidConfiguration()
		}
		configuration, err = readEnvoy(document)
	default:
		return Configuration{}, invalidConfiguration()
	}
	if err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Configuration{}, invalidConfiguration()
	}
	slices.SortFunc(configuration.Ports, comparePorts)
	canonical, err := Render(configuration)
	if err != nil || !bytes.Equal(canonical, value) {
		return Configuration{}, invalidConfiguration()
	}
	return configuration, nil
}

func readCaddy(document caddyDocument) (Configuration, error) {
	configuration := Configuration{Implementation: Caddy}
	servers := document.Apps.HTTP.Servers
	if document.Admin.Listen != "127.0.0.1:2019" || len(servers) == 0 || len(servers) > MaximumPorts {
		return Configuration{}, invalidConfiguration()
	}
	for name, server := range servers {
		port, err := readListenerIdentity(&configuration, name)
		if err != nil || len(server.Listen) != 1 || server.Listen[0] != fmt.Sprintf(":%d", port.Number) ||
			len(server.Routes) != 1 || len(server.Routes[0].Handle) != 1 {
			return Configuration{}, invalidConfiguration()
		}
		handler := server.Routes[0].Handle[0]
		names := make([]string, len(handler.Upstreams))
		if len(names) > MaximumUpstreams {
			return Configuration{}, invalidConfiguration()
		}
		if len(names) == 0 {
			if handler.Handler != "static_response" || handler.StatusCode != 503 {
				return Configuration{}, invalidConfiguration()
			}
		} else {
			if handler.Handler != "reverse_proxy" || handler.StatusCode != 0 {
				return Configuration{}, invalidConfiguration()
			}
			suffix := fmt.Sprintf(":%d", port.Number)
			for index, upstream := range handler.Upstreams {
				if !strings.HasSuffix(upstream.Dial, suffix) {
					return Configuration{}, invalidConfiguration()
				}
				names[index] = strings.TrimSuffix(upstream.Dial, suffix)
			}
		}
		if err := appendRoutingPort(&configuration, port, names); err != nil {
			return Configuration{}, err
		}
	}
	return configuration, nil
}

func readEnvoy(document envoyDocument) (Configuration, error) {
	configuration := Configuration{Implementation: Envoy}
	resources := document.StaticResources
	if document.Admin.Address != envoySocketAddress("127.0.0.1", 9901, TCP) || len(resources.Listeners) == 0 ||
		len(resources.Listeners) > MaximumPorts || len(resources.Clusters) != len(resources.Listeners) {
		return Configuration{}, invalidConfiguration()
	}
	clusters := make(map[string]envoyCluster, len(resources.Clusters))
	for _, cluster := range resources.Clusters {
		if _, duplicate := clusters[cluster.Name]; duplicate {
			return Configuration{}, invalidConfiguration()
		}
		clusters[cluster.Name] = cluster
	}
	for _, listener := range resources.Listeners {
		port, err := readListenerIdentity(&configuration, listener.Name)
		cluster, found := clusters[listener.Name]
		if err != nil || !found || len(cluster.Assignment.Endpoints) != 1 {
			return Configuration{}, invalidConfiguration()
		}
		endpoints := cluster.Assignment.Endpoints[0].Endpoints
		if len(endpoints) > MaximumUpstreams {
			return Configuration{}, invalidConfiguration()
		}
		names := make([]string, len(endpoints))
		for index, endpoint := range endpoints {
			names[index] = endpoint.Endpoint.Address.Socket.Address
		}
		if err := appendRoutingPort(&configuration, port, names); err != nil {
			return Configuration{}, err
		}
	}
	// Render comparison validates filters, addresses, protocols, cluster linkage,
	// timeouts and every endpoint field; no accepted field escapes that check.
	return configuration, nil
}

func appendRoutingPort(configuration *Configuration, port Port, names []string) error {
	if len(configuration.Ports) == 0 {
		configuration.Upstreams = slices.Clone(names)
	} else if !slices.Equal(configuration.Upstreams, names) {
		return invalidConfiguration()
	}
	configuration.Ports = append(configuration.Ports, port)
	return nil
}

func readListenerIdentity(configuration *Configuration, name string) (Port, error) {
	if !strings.HasPrefix(name, "gp_g") {
		return Port{}, invalidConfiguration()
	}
	rest := strings.TrimPrefix(name, "gp_g")
	separator := strings.IndexByte(rest, '_')
	portSeparator := strings.LastIndex(rest, "_p")
	if separator < 1 || portSeparator <= separator {
		return Port{}, invalidConfiguration()
	}
	generation, err := strconv.ParseUint(rest[:separator], 10, 64)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != rest[:separator] {
		return Port{}, invalidConfiguration()
	}
	label := rest[separator+1 : portSeparator]
	releaseID := "dep_" + strings.ToUpper(strings.TrimPrefix(label, "dep_"))
	if ids.Validate(ids.KindDeployment, releaseID) != nil || strings.ToLower(releaseID) != label ||
		configuration.Generation != 0 &&
			(configuration.Generation != generation || configuration.ReleaseID != releaseID) {
		return Port{}, invalidConfiguration()
	}
	configuration.Generation, configuration.ReleaseID = generation, releaseID
	portText := rest[portSeparator+2:]
	protocol := TCP
	if configuration.Implementation == Envoy {
		index := strings.LastIndexByte(portText, '_')
		if index < 1 {
			return Port{}, invalidConfiguration()
		}
		protocol, portText = Protocol(portText[index+1:]), portText[:index]
	}
	portNumber, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || portNumber == 0 || strconv.FormatUint(portNumber, 10) != portText {
		return Port{}, invalidConfiguration()
	}
	return Port{Number: uint16(portNumber), Protocol: protocol}, nil
}

func invalidConfiguration() error {
	return errs.New(
		errs.KindValidationFailed,
		"Service proxy configuration does not match its captured implementation, listeners or workloads",
	)
}
