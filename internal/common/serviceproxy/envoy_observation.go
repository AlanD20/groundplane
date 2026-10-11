package serviceproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/envoyadmin"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumObservationBytes = 4 * MaximumConfigurationBytes

// The three compact files precede the HTTP response. Each is data, not shell
// input. PID1 must load these exact restart files; an unused matching file is
// insufficient. Exec exit status and output bounds are checked by the caller.
const EnvoyObservation = "test \"$(tr '\\000' '\\n' < /proc/1/cmdline)\" = 'envoy\n--config-path\n" +
	EnvoyBootstrapPath + "\n--disable-hot-restart'; " +
	"test ! -L " + EnvoyRuntimeDirectory + "; " +
	"test ! -L " + EnvoyRoutingPath + "; test ! -L " + EnvoyBootstrapPath + "; test ! -L " + EnvoyClusterConfigPath + "; " +
	"cat " + EnvoyRoutingPath + " " + EnvoyBootstrapPath + " " + EnvoyClusterConfigPath + "; " + envoyadmin.ReadCommand

type envoyConfigDump struct {
	Configs []json.RawMessage `json:"configs"`
}
type envoyDumpKind struct {
	Type string `json:"@type"`
}
type envoyClusterDump struct {
	Version string               `json:"version_info"`
	Static  []json.RawMessage    `json:"static_clusters"`
	Active  []envoyActiveCluster `json:"dynamic_active_clusters"`
	Warming []json.RawMessage    `json:"dynamic_warming_clusters"`
}
type envoyActiveCluster struct {
	Version string          `json:"version_info"`
	Cluster json.RawMessage `json:"cluster"`
}
type envoyListenerDump struct {
	Static  []envoyStaticListener `json:"static_listeners"`
	Dynamic []json.RawMessage     `json:"dynamic_listeners"`
}
type envoyStaticListener struct {
	Listener json.RawMessage `json:"listener"`
}
type envoyListenerResource struct {
	Type string `json:"@type"`
	envoyListener
}

// VerifyEnvoyObservation requires the complete active routing set and exact
// restart files, not merely the CDS version string. Metadata timestamps have
// no authority; typed listener/cluster configuration rejects unknown fields.
func VerifyEnvoyObservation(value, expectedDigest []byte) (bool, error) {
	if len(value) == 0 || len(value) > MaximumObservationBytes || len(expectedDigest) != sha256.Size {
		return false, invalidEnvoyObservation()
	}
	parts := bytes.SplitN(value, []byte{'\n'}, 4)
	if len(parts) != 4 {
		return false, invalidEnvoyObservation()
	}
	manifest, bootstrap, discovery := parts[0], parts[1], parts[2]
	configuration, err := Read(Envoy, manifest)
	if err != nil {
		return false, invalidEnvoyObservation()
	}
	digest := sha256.Sum256(manifest)
	if !bytes.Equal(digest[:], expectedDigest) {
		return false, nil
	}
	source, err := PrepareRuntimeSource(Envoy, manifest)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(bootstrap, source.Bootstrap) || !bytes.Equal(discovery, source.Discovery) {
		return false, nil
	}
	body, err := envoyadmin.ReadResponse(parts[3], MaximumObservationBytes)
	if err != nil {
		return false, invalidEnvoyObservation()
	}
	var dump envoyConfigDump
	canonical, err := jcs.Canonicalize(body)
	if err != nil || decodeEnvoyValue(canonical, &dump, false) != nil || len(dump.Configs) > 16 {
		return false, invalidEnvoyObservation()
	}
	resources := envoyRuntimeResources(configuration)
	version := "sha256:" + hex.EncodeToString(expectedDigest)
	clusters, listeners := false, false
	for _, raw := range dump.Configs {
		var kind envoyDumpKind
		if decodeEnvoyValue(raw, &kind, false) != nil {
			return false, invalidEnvoyObservation()
		}
		switch kind.Type {
		case "type.googleapis.com/envoy.admin.v3.ClustersConfigDump":
			var observed envoyClusterDump
			if clusters || decodeEnvoyValue(raw, &observed, false) != nil {
				return false, invalidEnvoyObservation()
			}
			if !envoyClustersMatch(observed, resources.Clusters, version) {
				return false, nil
			}
			clusters = true
		case "type.googleapis.com/envoy.admin.v3.ListenersConfigDump":
			var observed envoyListenerDump
			if listeners || decodeEnvoyValue(raw, &observed, false) != nil {
				return false, invalidEnvoyObservation()
			}
			if !envoyListenersMatch(observed, resources.Listeners) {
				return false, nil
			}
			listeners = true
		}
	}
	if !clusters || !listeners {
		return false, invalidEnvoyObservation()
	}
	return true, nil
}

func envoyClustersMatch(dump envoyClusterDump, expected []envoyCluster, version string) bool {
	if dump.Version != version || len(dump.Static) != 0 || len(dump.Warming) != 0 || len(dump.Active) != len(expected) {
		return false
	}
	seen := make(map[string]bool, len(expected))
	for _, active := range dump.Active {
		var actual envoyClusterResource
		if active.Version != version || decodeEnvoyValue(active.Cluster, &actual, true) != nil ||
			actual.TypeURL != "type.googleapis.com/envoy.config.cluster.v3.Cluster" || seen[actual.Name] {
			return false
		}
		seen[actual.Name] = true
		index := slices.IndexFunc(expected, func(cluster envoyCluster) bool { return cluster.Name == actual.Name })
		if index < 0 || !envoyClusterMatches(actual.envoyCluster, expected[index]) {
			return false
		}
	}
	return true
}

func envoyClusterMatches(actual, expected envoyCluster) bool {
	// Proto JSON omits enum zero values; those are TCP and ROUND_ROBIN here.
	if actual.Policy == "" {
		actual.Policy = "ROUND_ROBIN"
	}
	if actual.Name != expected.Name || actual.Type != expected.Type ||
		actual.ConnectTimeout != expected.ConnectTimeout ||
		actual.DNSLookupFamily != expected.DNSLookupFamily ||
		actual.Policy != expected.Policy ||
		actual.Assignment.ClusterName != expected.Assignment.ClusterName ||
		len(actual.Assignment.Endpoints) != 1 {
		return false
	}
	return slices.EqualFunc(actual.Assignment.Endpoints[0].Endpoints, expected.Assignment.Endpoints[0].Endpoints,
		func(left, right envoyLoadEndpoint) bool {
			return envoySocketMatches(left.Endpoint.Address, right.Endpoint.Address)
		})
}

func envoyListenersMatch(dump envoyListenerDump, expected []envoyListener) bool {
	if len(dump.Dynamic) != 0 || len(dump.Static) != len(expected) {
		return false
	}
	seen := make(map[string]bool, len(expected))
	for _, entry := range dump.Static {
		var actual envoyListenerResource
		if decodeEnvoyValue(entry.Listener, &actual, true) != nil ||
			actual.Type != "type.googleapis.com/envoy.config.listener.v3.Listener" || seen[actual.Name] {
			return false
		}
		seen[actual.Name] = true
		index := slices.IndexFunc(expected, func(listener envoyListener) bool { return listener.Name == actual.Name })
		if index < 0 || !envoyListenerMatches(actual.envoyListener, expected[index]) {
			return false
		}
	}
	return true
}

func envoyListenerMatches(actual, expected envoyListener) bool {
	if actual.Name != expected.Name || !envoySocketMatches(actual.Address, expected.Address) ||
		!slices.EqualFunc(actual.FilterChains, expected.FilterChains, func(left, right envoyFilterChain) bool {
			return slices.Equal(left.Filters, right.Filters)
		}) || !slices.Equal(actual.ListenerFilters, expected.ListenerFilters) {
		return false
	}
	if actual.UDPConfig == nil || expected.UDPConfig == nil {
		return actual.UDPConfig == nil && expected.UDPConfig == nil
	}
	return *actual.UDPConfig == *expected.UDPConfig
}

func envoySocketMatches(actual, expected envoyAddress) bool {
	if actual.Socket.Protocol == "" {
		actual.Socket.Protocol = "TCP"
	}
	return actual == expected
}

// The target type is constrained to the three external JSON boundaries above;
// it is not a dynamic model or conversion between internal representations.
func decodeEnvoyValue[T envoyConfigDump | envoyDumpKind | envoyClusterDump | envoyListenerDump | envoyClusterResource | envoyListenerResource](
	value []byte,
	target *T,
	strict bool,
) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return invalidEnvoyObservation()
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return invalidEnvoyObservation()
	}
	return nil
}

func invalidEnvoyObservation() error {
	return errs.New(errs.KindRequestFailed, "Envoy active and restart configuration could not be read completely")
}
