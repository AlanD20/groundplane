package serviceproxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	EnvoyInitialConfigPath = "/etc/envoy/groundplane-proxy.input"
	// These are container-private writable-layer files, not host or repository
	// scratch. They survive a Docker restart and retire with the owned container.
	EnvoyRuntimeDirectory  = "/tmp/groundplane-envoy"
	EnvoyBootstrapPath     = EnvoyRuntimeDirectory + "/bootstrap.json"
	EnvoyClusterConfigPath = EnvoyRuntimeDirectory + "/clusters.json"
	EnvoyRoutingPath       = EnvoyRuntimeDirectory + "/routing.json"
	EnvoyRunningCommand    = "envoy\x00--config-path\x00" + EnvoyBootstrapPath + "\x00--disable-hot-restart\x00" +
		"--service-node\x00groundplane-service-proxy\x00--service-cluster\x00groundplane-service-proxy\x00"
	EnvoyStartup = "umask 077; command -v bash timeout sed sync mv cmp cat tr >/dev/null; " +
		"test ! -L " + EnvoyRuntimeDirectory + "; mkdir -p " + EnvoyRuntimeDirectory + "; " +
		"if [ ! -e " + EnvoyBootstrapPath + " ]; then sed -n '1p' " + EnvoyInitialConfigPath + " > " + EnvoyBootstrapPath + ".next; " +
		"sync; mv -f " + EnvoyBootstrapPath + ".next " + EnvoyBootstrapPath + "; sync; fi; " +
		"if [ ! -e " + EnvoyClusterConfigPath + " ]; then sed -n '2p' " + EnvoyInitialConfigPath + " > " + EnvoyClusterConfigPath + ".next; " +
		"sync; mv -f " + EnvoyClusterConfigPath + ".next " + EnvoyClusterConfigPath + "; sync; fi; " +
		"if [ ! -e " + EnvoyRoutingPath + " ]; then sed -n '3p' " + EnvoyInitialConfigPath + " > " + EnvoyRoutingPath + ".next; " +
		"sync; mv -f " + EnvoyRoutingPath + ".next " + EnvoyRoutingPath + "; sync; fi; " +
		"exec envoy --config-path " + EnvoyBootstrapPath + " --disable-hot-restart " +
		"--service-node groundplane-service-proxy --service-cluster groundplane-service-proxy"
	// Activation receives the compiled three-line runtime source over stdin.
	// An unchanged bootstrap is required before writes; changing listeners needs
	// the protected proxy replacement path. Partial publication cannot pass proof.
	EnvoyActivate = "umask 077; test ! -L " + EnvoyRuntimeDirectory + "; " +
		"test -f " + EnvoyBootstrapPath + "; test ! -L " + EnvoyBootstrapPath + "; " +
		"test -f " + EnvoyClusterConfigPath + "; test ! -L " + EnvoyClusterConfigPath + "; " +
		"test -f " + EnvoyRoutingPath + "; test ! -L " + EnvoyRoutingPath + "; " +
		"cat > " + EnvoyRuntimeDirectory + "/input.next; " +
		"sed -n '1p' " + EnvoyRuntimeDirectory + "/input.next | cmp - " + EnvoyBootstrapPath + "; " +
		"sed -n '2p' " + EnvoyRuntimeDirectory + "/input.next > " + EnvoyClusterConfigPath + ".next; " +
		"sed -n '3p' " + EnvoyRuntimeDirectory + "/input.next > " + EnvoyRoutingPath + ".next; sync; " +
		"mv -f " + EnvoyRoutingPath + ".next " + EnvoyRoutingPath + "; " +
		"mv -f " + EnvoyClusterConfigPath + ".next " + EnvoyClusterConfigPath + "; sync; " +
		"rm -f " + EnvoyRuntimeDirectory + "/input.next"
)

type RuntimeSource struct {
	Path      string
	Content   []byte
	Startup   string
	Bootstrap []byte
	Discovery []byte
}

// PrepareRuntimeSource derives executable files from one captured routing set.
// Its one Compose config is also the native runtime projection's source, so a
// newly recreated proxy receives the same targets as its acknowledgement.
func PrepareRuntimeSource(implementation Implementation, manifest []byte) (RuntimeSource, error) {
	configuration, err := Read(implementation, manifest)
	if err != nil {
		return RuntimeSource{}, err
	}
	if implementation == Caddy {
		return RuntimeSource{Path: InitialConfigPath, Content: append([]byte(nil), manifest...), Startup: Startup}, nil
	}
	resources := envoyRuntimeResources(configuration)
	bootstrap, err := json.Marshal(envoyRuntimeBootstrap{
		Admin: envoyAdmin{Address: envoySocketAddress("127.0.0.1", 9901, TCP)},
		Dynamic: envoyDynamicResources{CDS: envoyConfigSource{
			Path: envoyPathConfigSource{Path: EnvoyClusterConfigPath}, Version: "V3",
		}},
		Static: envoyResources{Clusters: []envoyCluster{}, Listeners: resources.Listeners},
	})
	if err != nil {
		return RuntimeSource{}, errs.Wrap(errs.KindInternal, err)
	}
	clusters := make([]envoyClusterResource, len(resources.Clusters))
	for index, cluster := range resources.Clusters {
		clusters[index] = envoyClusterResource{
			TypeURL:      "type.googleapis.com/envoy.config.cluster.v3.Cluster",
			envoyCluster: cluster,
		}
	}
	digest := sha256.Sum256(manifest)
	discovery, err := json.Marshal(envoyDiscoveryResponse{Resources: clusters,
		TypeURL: "type.googleapis.com/envoy.config.cluster.v3.Cluster", Version: "sha256:" + hex.EncodeToString(digest[:])})
	if err != nil {
		return RuntimeSource{}, errs.Wrap(errs.KindInternal, err)
	}
	content := make([]byte, 0, len(bootstrap)+len(discovery)+len(manifest)+3)
	content = append(content, bootstrap...)
	content = append(content, '\n')
	content = append(content, discovery...)
	content = append(content, '\n')
	content = append(content, manifest...)
	content = append(content, '\n')
	return RuntimeSource{Path: EnvoyInitialConfigPath, Content: content, Startup: EnvoyStartup,
		Bootstrap: bootstrap, Discovery: discovery}, nil
}

type envoyRuntimeBootstrap struct {
	Admin   envoyAdmin            `json:"admin"`
	Dynamic envoyDynamicResources `json:"dynamic_resources"`
	Static  envoyResources        `json:"static_resources"`
}
type envoyDynamicResources struct {
	CDS envoyConfigSource `json:"cds_config"`
}
type envoyConfigSource struct {
	Path    envoyPathConfigSource `json:"path_config_source"`
	Version string                `json:"resource_api_version"`
}
type envoyPathConfigSource struct {
	Path string `json:"path"`
}
type envoyDiscoveryResponse struct {
	Resources []envoyClusterResource `json:"resources"`
	TypeURL   string                 `json:"type_url"`
	Version   string                 `json:"version_info"`
}
type envoyClusterResource struct {
	TypeURL string `json:"@type"`
	envoyCluster
}

func envoyRuntimeResources(configuration Configuration) envoyResources {
	document := envoyConfigurationDocument(configuration)
	for index, port := range configuration.Ports {
		name := fmt.Sprintf("gp_p%d_%s", port.Number, port.Protocol)
		cluster := &document.StaticResources.Clusters[index]
		cluster.Name, cluster.Assignment.ClusterName = name, name
		listener := &document.StaticResources.Listeners[index]
		listener.Name = name
		if port.Protocol == TCP {
			filter := &listener.FilterChains[0].Filters[0].Config
			filter.StatPrefix, filter.Cluster = name, name
		} else {
			filter := &listener.ListenerFilters[0].Config
			filter.StatPrefix, filter.Matcher.NoMatch.Action.Config.Cluster = name, name
		}
	}
	return document.StaticResources
}
