package config

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/remotehost"
)

type RemoteControllerTLSConfig struct {
	Listen                  string `yaml:"listen"`
	Certificate             string `yaml:"certificate"`
	PrivateKey              string `yaml:"private_key"`
	EnrollmentCACertificate string `yaml:"enrollment_ca_certificate"`
	EnrollmentCAPrivateKey  string `yaml:"enrollment_ca_private_key"`
}

func (c RemoteControllerTLSConfig) ListenAddress() (netip.AddrPort, error) {
	address, err := netip.ParseAddrPort(c.Listen)
	if err != nil || address.Addr().IsUnspecified() || address.Addr().IsMulticast() || address.Port() == 0 {
		return netip.AddrPort{}, fmt.Errorf(
			"config: controller remote_agent.listen requires a concrete address and port",
		)
	}
	return address, nil
}

func (c ControllerConfig) validateRemoteAgentListener() error {
	remote := c.RemoteAgent
	if remote == nil {
		return nil
	}
	bound, err := remote.ListenAddress()
	if err != nil {
		return err
	}
	if !absoluteCertificatePaths(
		remote.Certificate,
		remote.PrivateKey,
		remote.EnrollmentCACertificate,
		remote.EnrollmentCAPrivateKey,
	) ||
		remote.Certificate == remote.EnrollmentCACertificate ||
		remote.PrivateKey == remote.EnrollmentCAPrivateKey {
		return fmt.Errorf("config: controller remote_agent requires distinct absolute TLS and enrollment CA paths")
	}
	otherListeners := append(
		c.HTTPAddresses(),
		c.Listen.ScopedRunnerHTTP,
		c.Listen.ScopedRunnerRegistry,
		c.Listen.GitHubWebhooks,
	)
	for _, value := range otherListeners {
		other, err := netip.ParseAddrPort(value)
		if err == nil && other.Port() == bound.Port() &&
			(other.Addr().Unmap() == bound.Addr().Unmap() || other.Addr().IsUnspecified()) {
			return fmt.Errorf("config: controller remote_agent.listen conflicts with an HTTP listener")
		}
	}
	return nil
}

type RemoteAgentTLSConfig struct {
	Endpoint                string `yaml:"endpoint"`
	ControllerServerName    string `yaml:"controller_server_name"`
	ControllerCACertificate string `yaml:"controller_ca_certificate"`
	EnrollmentCACertificate string `yaml:"enrollment_ca_certificate"`
	Certificate             string `yaml:"certificate"`
	PrivateKey              string `yaml:"private_key"`
	HostID                  string `yaml:"host_id"`
	Generation              uint64 `yaml:"generation"`
}

func (c RemoteAgentTLSConfig) Identity(agentID string) remotehost.Identity {
	return remotehost.Identity{HostID: c.HostID, AgentID: agentID, Generation: c.Generation}
}

func (c RemoteAgentTLSConfig) validate(agentID string) error {
	host, portText, err := net.SplitHostPort(c.Endpoint)
	port, portErr := strconv.ParseUint(portText, 10, 16)
	if err != nil || portErr != nil || port == 0 || !remoteHostname(host) || !remoteHostname(c.ControllerServerName) {
		return fmt.Errorf("config: agent remote requires a Controller endpoint and TLS server name")
	}
	if !c.Identity(agentID).Valid() || !absoluteCertificatePaths(c.ControllerCACertificate, c.EnrollmentCACertificate,
		c.Certificate, c.PrivateKey) {
		return fmt.Errorf(
			"config: agent remote requires enrolled Host identity, generation and absolute credential paths",
		)
	}
	return nil
}

func remoteHostname(name string) bool {
	if address, err := netip.ParseAddr(name); err == nil {
		return !address.IsUnspecified() && !address.IsMulticast()
	}
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(name, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.ContainsFunc(label, func(r rune) bool {
				return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-')
			}) {
			return false
		}
	}
	return true
}

func absoluteCertificatePaths(paths ...string) bool {
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsRune(path, 0) {
			return false
		}
	}
	return true
}
