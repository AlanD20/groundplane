package dnsresolverobserver

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateRequest(request Request) error {
	if ids.Validate(ids.KindComponent, request.ComponentID) != nil ||
		ids.Validate(ids.KindService, request.ServiceID) != nil ||
		ids.Validate(ids.KindConfig, request.ArtifactID) != nil || request.RenderGeneration == 0 ||
		request.ProjectName == "" || request.ServiceName == "" || request.ArtifactTarget == "" ||
		!imageref.IsDigestPinned(request.ImageReference) || request.ListenEndpoint != "127.0.0.1:53" ||
		request.ImageRepository == "" || request.ImageOS != "linux" ||
		(request.ImageArchitecture != "amd64" && request.ImageArchitecture != "arm64") ||
		request.MetricsURL != "http://127.0.0.1:9153/metrics" ||
		request.ReloadMetric == "" ||
		len(request.ExpectedLabels) == 0 {
		return errs.New(errs.KindValidationFailed, "DNS resolver observation identity is invalid")
	}
	return nil
}

type parsedArtifact struct {
	staticName    string
	staticAddress netip.Addr
	forwardGroups []forwardGroup
}

type forwardGroup struct {
	domain    string
	endpoints []string
}

func (artifact parsedArtifact) forwardGroup(domain string) (forwardGroup, bool) {
	for _, group := range artifact.forwardGroups {
		if group.domain == domain {
			return group, true
		}
	}
	return forwardGroup{}, false
}

func (artifact parsedArtifact) allEndpoints() []string {
	set := make(map[string]struct{})
	for _, group := range artifact.forwardGroups {
		for _, endpoint := range group.endpoints {
			set[endpoint] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for endpoint := range set {
		result = append(result, endpoint)
	}
	sort.Strings(result)
	return result
}

func parseArtifact(content []byte) (parsedArtifact, error) {
	if len(content) == 0 || len(content) > maximumArtifactBytes {
		return parsedArtifact{}, errs.New(errs.KindStateConflict, "DNS resolver artifact size is invalid")
	}
	result := parsedArtifact{}
	inHosts := false
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 1024), maximumArtifactBytes)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "hosts" && len(fields) == 2 && fields[1] == "{" {
			inHosts = true
			continue
		}
		if inHosts && fields[0] == "}" {
			inHosts = false
			continue
		}
		if inHosts && fields[0] != "no_reverse" && fields[0] != "fallthrough" {
			address, err := netip.ParseAddr(fields[0])
			if err != nil || !address.Is4() || len(fields) < 2 {
				return parsedArtifact{}, errs.New(errs.KindStateConflict, "DNS resolver static proof input is invalid")
			}
			for _, name := range fields[1:] {
				if result.staticName == "" || name < result.staticName {
					result.staticName, result.staticAddress = name, address
				}
			}
		}
		if fields[0] == "forward" && len(fields) >= 3 {
			group := forwardGroup{domain: absoluteName(fields[1])}
			for _, value := range fields[2:] {
				endpoint, endpointErr := normalizeEndpoint(value)
				if endpointErr != nil {
					return parsedArtifact{}, endpointErr
				}
				group.endpoints = append(group.endpoints, endpoint)
			}
			sort.Strings(group.endpoints)
			result.forwardGroups = append(result.forwardGroups, group)
		}
	}
	if err := scanner.Err(); err != nil {
		return parsedArtifact{}, errs.Wrap(errs.KindInternal, err)
	}
	sort.Slice(result.forwardGroups, func(left, right int) bool {
		return result.forwardGroups[left].domain < result.forwardGroups[right].domain
	})
	return result, nil
}

func absoluteName(value string) string {
	if value == "." || strings.HasSuffix(value, ".") {
		return value
	}
	return value + "."
}

func normalizeEndpoint(value string) (string, error) {
	value = strings.TrimPrefix(value, "tls://")
	if host, port, err := net.SplitHostPort(value); err == nil {
		address, parseErr := netip.ParseAddr(host)
		if parseErr != nil || port != "53" {
			return "", errs.New(errs.KindStateConflict, "DNS resolver forward endpoint is invalid")
		}
		return net.JoinHostPort(address.String(), "53"), nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil {
		return "", errs.New(errs.KindStateConflict, "DNS resolver forward endpoint is invalid")
	}
	return net.JoinHostPort(address.String(), "53"), nil
}

func forwardProofName(componentID string, generation uint64, index int, domain string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", componentID, generation, index)))
	return "gp-" + hex.EncodeToString(digest[:10]) + "." + domain
}
