package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"sort"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
)

type ensuredNetwork struct {
	ComposeHash string            `yaml:"-"`
	Name        string            `yaml:"name"`
	Driver      string            `yaml:"driver"`
	Internal    bool              `yaml:"internal"`
	External    bool              `yaml:"external"`
	EnableIPv4  *bool             `yaml:"enable_ipv4"`
	EnableIPv6  bool              `yaml:"enable_ipv6"`
	Attachable  bool              `yaml:"attachable"`
	DriverOpts  map[string]string `yaml:"driver_opts"`
	Labels      map[string]string `yaml:"labels"`
	IPAM        struct {
		Driver  string            `yaml:"driver"`
		Options map[string]string `yaml:"options"`
		Config  []struct {
			Subnet       string            `yaml:"subnet"`
			Gateway      string            `yaml:"gateway"`
			IPRange      string            `yaml:"ip_range"`
			AuxAddresses map[string]string `yaml:"aux_addresses"`
		} `yaml:"config"`
	} `yaml:"ipam"`
}

type inspectedOwnedNetwork struct {
	ID         string `json:"Id"`
	Name       string
	Driver     string
	Internal   bool
	EnableIPv6 bool
	EnableIPv4 *bool
	Attachable bool
	Labels     map[string]string
	Options    map[string]string
	IPAM       struct {
		Driver string
		Config []struct {
			Subnet, Gateway, IPRange string
			AuxiliaryAddresses       map[string]string
		}
	}
}

func selectedEnsuredNetwork(
	artifact *agentpb.ComposeArtifact,
	ensure *agentpb.ManagedNetworkEnsure,
) (ensuredNetwork, error) {
	var selected *agentpb.ComposeNetwork
	for _, network := range artifact.GetNetworks() {
		if network.GetNetworkId() == ensure.GetNetworkId() {
			selected = network
			break
		}
	}
	if selected == nil {
		return ensuredNetwork{}, errs.New(errs.KindValidationFailed, "owned network selection is missing")
	}
	var document struct {
		Networks map[string]ensuredNetwork `yaml:"networks"`
	}
	if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &document); err != nil {
		return ensuredNetwork{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	network, found := document.Networks[selected.GetComposeName()]
	if len(network.IPAM.Options) != 0 {
		return ensuredNetwork{}, errs.New(errs.KindValidationFailed, "owned network IPAM options are unsupported")
	}
	if !found || network.Name != selected.GetDockerName() || network.External ||
		network.Driver != "" && network.Driver != "bridge" ||
		len(network.IPAM.Config) != 1 ||
		network.EnableIPv6 ||
		network.EnableIPv4 != nil && !*network.EnableIPv4 ||
		network.Attachable ||
		len(network.DriverOpts) != 0 ||
		network.IPAM.Driver != "" && network.IPAM.Driver != "default" {
		return ensuredNetwork{}, errs.New(errs.KindValidationFailed, "owned network rendered bridge shape is invalid")
	}
	pool := network.IPAM.Config[0]
	prefix, err := netip.ParsePrefix(pool.Subnet)
	if err != nil || !prefix.Addr().Is4() || prefix.Masked().String() != pool.Subnet || pool.IPRange != "" ||
		len(pool.AuxAddresses) != 0 {
		return ensuredNetwork{}, errs.New(errs.KindValidationFailed, "owned network requires one canonical IPv4 subnet")
	}
	if pool.Gateway != "" {
		gateway, err := netip.ParseAddr(pool.Gateway)
		if err != nil || !prefix.Contains(gateway) {
			return ensuredNetwork{}, errs.New(errs.KindValidationFailed, "owned network gateway is invalid")
		}
	}
	for _, label := range selected.GetExpectedLabels() {
		if network.Labels[label.GetKey()] != label.GetValue() {
			return ensuredNetwork{}, errs.New(
				errs.KindValidationFailed,
				"owned network labels differ from sealed ownership",
			)
		}
	}
	for key := range network.Labels {
		if strings.HasPrefix(key, "com.docker.compose.") {
			return ensuredNetwork{}, errs.New(
				errs.KindValidationFailed,
				"sealed network contains reserved Compose labels",
			)
		}
	}
	// Compose 2.40.3 NetworkHash serializes NetworkConfig, excluding
	// CustomLabels/extensions. Restricted MVP fields have the same JSON shape
	// in the pinned compose-go 2.9.1 and the repository's existing model.
	var hashDocument struct {
		Networks map[string]composetypes.NetworkConfig `yaml:"networks"`
	}
	if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &hashDocument); err != nil {
		return ensuredNetwork{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	config := hashDocument.Networks[selected.GetComposeName()]
	encoded, err := json.Marshal(&config)
	if err != nil {
		return ensuredNetwork{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	hash := sha256.Sum256(encoded)
	network.ComposeHash = hex.EncodeToString(hash[:])
	network.Labels["com.docker.compose.project"] = artifact.GetProjectName()
	network.Labels["com.docker.compose.network"] = selected.GetComposeName()
	return network, nil
}

func executeManagedNetworkEnsure(
	ctx context.Context,
	taskRunner runner.Runner,
	timeout uint32,
	artifact *agentpb.ComposeArtifact,
	ensure *agentpb.ManagedNetworkEnsure,
) (*agentpb.ComposeHelperResponse, error) {
	network, err := selectedEnsuredNetwork(artifact, ensure)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	run := func(args ...string) (runner.Result, error) {
		result, err := taskRunner.Run(
			executionCtx,
			runner.RunCmdOpts{
				Name:              DockerExecutable,
				Args:              args,
				Dir:               WorkDirectory,
				Env:               append([]string(nil), fixedEnvironment...),
				ReplaceEnv:        true,
				CaptureLimitBytes: maximumComponentConfig,
			},
		)
		if executionCtx.Err() != nil {
			return runner.Result{}, executionCtx.Err()
		}
		if err != nil {
			return runner.Result{}, errs.Wrap(errs.KindInternal, err)
		}
		if result.ExitCode != 0 {
			return runner.Result{}, errs.New(errs.KindStateConflict, "owned network command failed")
		}
		return result, nil
	}
	listed, err := run(
		"network",
		"ls",
		"--filter",
		"name=^"+network.Name+"$",
		"--format",
		"{{.ID}} {{.Name}}",
		"--no-trunc",
	)
	if err != nil {
		return nil, err
	}
	lines := strings.Fields(string(listed.Stdout))
	if len(lines) == 0 {
		args := []string{"network", "create", "--driver", "bridge", "--subnet", network.IPAM.Config[0].Subnet}
		if network.Internal {
			args = append(args, "--internal")
		}
		if network.IPAM.Config[0].Gateway != "" {
			args = append(args, "--gateway", network.IPAM.Config[0].Gateway)
		}
		var keys []string
		for key := range network.Labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			args = append(args, "--label", key+"="+network.Labels[key])
		}
		args = append(args, network.Name)
		created, err := run(args...)
		if err != nil {
			return nil, err
		}
		lines = []string{strings.TrimSpace(string(created.Stdout)), network.Name}
	}
	if len(lines) != 2 || lines[1] != network.Name || !validOwnedNetworkID(lines[0]) {
		return nil, errs.New(errs.KindStateConflict, "owned network name resolution is ambiguous or invalid")
	}
	inspected, err := run("network", "inspect", "--format", "{{json .}}", lines[0])
	if err != nil {
		return nil, err
	}
	var actual inspectedOwnedNetwork
	if json.Unmarshal(inspected.Stdout, &actual) != nil || actual.ID != lines[0] ||
		!ownedNetworkMatches(actual, network) {
		return nil, errs.New(errs.KindStateConflict, "existing owned network differs from sealed authority")
	}
	return completedResponse(), nil
}

func validOwnedNetworkID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, character := range id {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func ownedNetworkMatches(actual inspectedOwnedNetwork, expected ensuredNetwork) bool {
	if actual.Name != expected.Name || actual.Driver != "bridge" || actual.Internal != expected.Internal ||
		actual.EnableIPv6 ||
		actual.EnableIPv4 != nil && !*actual.EnableIPv4 ||
		actual.Attachable ||
		len(actual.Options) != 0 ||
		actual.IPAM.Driver != "default" ||
		len(actual.IPAM.Config) != 1 {
		return false
	}
	pool := actual.IPAM.Config[0]
	wanted := expected.IPAM.Config[0]
	if pool.Subnet != wanted.Subnet || pool.IPRange != "" || len(pool.AuxiliaryAddresses) != 0 ||
		wanted.Gateway != "" && wanted.Gateway != pool.Gateway {
		return false
	}
	prefix, err := netip.ParsePrefix(wanted.Subnet)
	if err != nil {
		return false
	}
	gateway, err := netip.ParseAddr(pool.Gateway)
	if err != nil || !prefix.Contains(gateway) {
		return false
	}
	for key, value := range expected.Labels {
		if actual.Labels[key] != value {
			return false
		}
	}
	hash := actual.Labels["com.docker.compose.config-hash"]
	return hash == "" || hash == expected.ComposeHash
}
