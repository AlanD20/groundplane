package serviceproxy

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Implementation is the captured proxy choice, not a lookup of current desired
// state. Only the two compiled implementations can produce managed routing.
type Implementation string

const (
	Caddy            Implementation = "caddy"
	Envoy            Implementation = "envoy"
	MaximumPorts                    = 32
	MaximumUpstreams                = 16
)

type Protocol string

const (
	TCP Protocol = "tcp"
	UDP Protocol = "udp"
)

type Port struct {
	Number   uint16
	Protocol Protocol
}

// Configuration describes one exact, complete routing set. An empty upstream
// set is intentional: an initial rollout must not send traffic to unready work.
type Configuration struct {
	Implementation Implementation
	ReleaseID      string
	Generation     uint64
	Ports          []Port
	Upstreams      []string
}

func (configuration Configuration) Validate() error {
	if ids.Validate(ids.KindDeployment, configuration.ReleaseID) != nil || configuration.Generation == 0 ||
		len(configuration.Upstreams) > MaximumUpstreams {
		return errs.New(errs.KindValidationFailed, "Service proxy release, generation or upstream count is invalid")
	}
	if err := ValidatePorts(configuration.Implementation, configuration.Ports); err != nil {
		return err
	}
	for index, name := range configuration.Upstreams {
		if name == "" || len(name) > 253 || strings.ContainsAny(name, "/\\\x00: \t\n\r") ||
			index > 0 && configuration.Upstreams[index-1] >= name {
			return errs.New(
				errs.KindValidationFailed,
				"Service proxy upstream names must be sorted, unique routing names",
			)
		}
	}
	return nil
}

func ValidatePorts(implementation Implementation, ports []Port) error {
	if implementation != Caddy && implementation != Envoy {
		return errs.New(errs.KindValidationFailed, "Service proxy implementation must be Caddy or Envoy")
	}
	if len(ports) == 0 || len(ports) > MaximumPorts {
		return errs.Newf(
			errs.KindValidationFailed,
			"Service proxy requires between 1 and %d port/protocol pairs",
			MaximumPorts,
		)
	}
	for index, port := range ports {
		if port.Number == 0 || port.Protocol != TCP && port.Protocol != UDP ||
			index > 0 && comparePorts(ports[index-1], port) >= 0 {
			return errs.New(
				errs.KindValidationFailed,
				"Service proxy ports must be valid, sorted and unique port/protocol pairs",
			)
		}
		if implementation == Caddy && port.Protocol == UDP {
			return errs.Newf(
				errs.KindValidationFailed,
				"Caddy is HTTP-only and cannot forward %d/udp; select Envoy for this Environment",
				port.Number,
			)
		}
		if implementation == Envoy && port.Number == 9901 && port.Protocol == TCP {
			return errs.New(
				errs.KindValidationFailed,
				"Envoy reserves container TCP port 9901 for its private admin API",
			)
		}
	}
	return nil
}

func comparePorts(left, right Port) int {
	if order := cmp.Compare(left.Number, right.Number); order != 0 {
		return order
	}
	return cmp.Compare(left.Protocol, right.Protocol)
}

func Render(configuration Configuration) ([]byte, error) {
	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	switch configuration.Implementation {
	case Caddy:
		return renderCaddy(configuration)
	case Envoy:
		return renderEnvoy(configuration)
	default:
		return nil, errs.New(errs.KindValidationFailed, "Service proxy implementation is not compiled")
	}
}

func listenerName(configuration Configuration, port Port) string {
	name := fmt.Sprintf(
		"gp_g%d_%s_p%d",
		configuration.Generation,
		strings.ToLower(configuration.ReleaseID),
		port.Number,
	)
	if configuration.Implementation == Envoy {
		name += "_" + string(port.Protocol)
	}
	return name
}
