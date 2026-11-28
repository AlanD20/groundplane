package dnsresolver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"
)

// ServingProofZone is reserved for verifying the configuration loaded by the
// managed resolver. A probe uses a fresh name below this zone.
const ServingProofZone = "gp-resolver-proof.invalid."

const (
	servingProofRule  = "    # Groundplane resolver serving proof v1\n"
	servingProofStart = servingProofRule + "    template IN TXT " + ServingProofZone + " {\n" +
		"        answer \"{{ .Name }} 0 IN TXT "
	servingProofEnd    = "\"\n    }\n"
	managedCorefileEnd = "    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n"
)

// The exact managed profile excludes external files, environment expansion
// and other plugins that could answer the proof name or vary behind its digest.
func isManagedCorefile(content []byte) bool {
	body, header := strings.CutPrefix(string(content), ". {\n    bind 127.0.0.1")
	body, footer := strings.CutSuffix(body, "    reload\n"+managedCorefileEnd)
	listeners, body, bound := strings.Cut(body, "\n")
	if !header || !footer || !bound {
		return false
	}
	if listeners != "" {
		listeners, separated := strings.CutPrefix(listeners, " ")
		if !separated {
			return false
		}
		for address := range strings.SplitSeq(listeners, " ") {
			if !corefileIPv4(address) {
				return false
			}
		}
	}
	if hosts, found := strings.CutPrefix(body, "    hosts {\n"); found {
		rows, following, closed := strings.Cut(hosts, "        no_reverse\n        fallthrough\n    }\n")
		if !closed || !managedHostRows(rows) {
			return false
		}
		body = following
	}
	body, newline := strings.CutSuffix(body, "\n")
	if !newline {
		return false
	}
	for line := range strings.SplitSeq(body, "\n") {
		forward, found := strings.CutPrefix(line, "    forward ")
		domain, endpoints, separated := strings.Cut(forward, " ")
		if !found || !separated || !corefileDNSName(domain) {
			return false
		}
		for endpoint := range strings.SplitSeq(endpoints, " ") {
			if _, err := netip.ParseAddr(endpoint); err != nil {
				if _, err := netip.ParseAddrPort(endpoint); err != nil {
					return false
				}
			}
		}
	}
	return true
}

func managedHostRows(rows string) bool {
	rows, terminated := strings.CutSuffix(rows, "\n")
	if !terminated {
		return false
	}
	for line := range strings.SplitSeq(rows, "\n") {
		row, found := strings.CutPrefix(line, "        ")
		address, names, separated := strings.Cut(row, " ")
		if !found || !separated || !corefileIPv4(address) {
			return false
		}
		for name := range strings.SplitSeq(names, " ") {
			if !corefileDNSName(name) {
				return false
			}
		}
	}
	return true
}

func corefileIPv4(value string) bool {
	address, err := netip.ParseAddr(value)
	return err == nil && address.Is4()
}

func corefileDNSName(value string) bool {
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '-') {
			return false
		}
	}
	return value != ""
}

// AddCorefileServingProof inserts a rule binding the entire managed Corefile
// to the answer from the running resolver. The answer names the digest of the
// authored bytes before this rule is inserted.
func AddCorefileServingProof(content []byte) ([]byte, error) {
	if !isManagedCorefile(content) || bytes.Contains(content, []byte(ServingProofZone)) {
		return nil, fmt.Errorf("dns resolver: serving proof requires the closed managed Corefile")
	}
	digest := sha256.Sum256(content)
	insert := len(content) - len(managedCorefileEnd)
	var marked bytes.Buffer
	marked.Grow(len(content) + len(servingProofStart) + sha256.Size*2 + len(servingProofEnd))
	marked.Write(content[:insert])
	marked.WriteString(servingProofStart)
	marked.WriteString(hex.EncodeToString(digest[:]))
	marked.WriteString(servingProofEnd)
	marked.Write(content[insert:])
	return marked.Bytes(), nil
}

// CorefileServingProof returns the digest to prove and whether the managed
// rule is present. A malformed managed rule cannot select the legacy path.
func CorefileServingProof(content []byte) (digest string, marked bool, err error) {
	insert := bytes.Index(content, []byte(servingProofRule))
	if insert < 0 {
		return "", false, nil
	}
	end := insert + len(servingProofStart) + sha256.Size*2 + len(servingProofEnd)
	if end > len(content) || !bytes.HasPrefix(content[insert:], []byte(servingProofStart)) ||
		!bytes.Equal(content[end-len(servingProofEnd):end], []byte(servingProofEnd)) ||
		!bytes.Equal(content[end:], []byte(managedCorefileEnd)) {
		return "", true, invalidServingProof()
	}
	base := append(bytes.Clone(content[:insert]), content[end:]...)
	if !isManagedCorefile(base) || bytes.Contains(base, []byte(ServingProofZone)) {
		return "", true, invalidServingProof()
	}
	expected := sha256.Sum256(base)
	digest = hex.EncodeToString(expected[:])
	if !bytes.Equal(content[insert+len(servingProofStart):end-len(servingProofEnd)], []byte(digest)) {
		return "", true, invalidServingProof()
	}
	return digest, true, nil
}

func invalidServingProof() error {
	return fmt.Errorf("dns resolver: managed Corefile serving proof is invalid")
}
