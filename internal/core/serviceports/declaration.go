// Package serviceports owns internal Compose port declarations, not host
// publishing or deployment-strategy support.
package serviceports

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Protocol string

const (
	TCP Protocol = "tcp"
	UDP Protocol = "udp"
)

type Declaration struct {
	First    uint16
	Last     uint16
	Protocol Protocol
}

// Parse accepts an individual container port or inclusive range, optionally
// followed by /tcp or /udp. Bare ports use Compose's TCP default. Host addresses,
// published ports and Service-name prefixes are not expose declarations.
func Parse(raw string) (Declaration, error) {
	value := strings.TrimSpace(raw)
	ports, protocolText, explicitProtocol := strings.Cut(value, "/")
	protocol := TCP
	if explicitProtocol {
		protocol = Protocol(protocolText)
		if protocol != TCP && protocol != UDP {
			return Declaration{}, invalid(raw, "protocol must be tcp or udp")
		}
	}
	firstText, lastText, ranged := strings.Cut(ports, "-")
	first, ok := parsePort(firstText)
	if !ok {
		return Declaration{}, invalid(raw, "use a container port from 1 to 65535, not a host or Service-name prefix")
	}
	last := first
	if ranged {
		last, ok = parsePort(lastText)
		if !ok || last < first {
			return Declaration{}, invalid(raw, "range must end at a port from its start through 65535")
		}
	}
	return Declaration{First: first, Last: last, Protocol: protocol}, nil
}

func parsePort(value string) (uint16, bool) {
	if len(value) == 0 || len(value) > 5 {
		return 0, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	port, err := strconv.ParseUint(value, 10, 16)
	return uint16(port), err == nil && port != 0
}

func invalid(raw, reason string) error {
	return errs.New(errs.KindValidationFailed, fmt.Sprintf("expose %q: %s", raw, reason))
}
