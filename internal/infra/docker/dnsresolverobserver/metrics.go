package dnsresolverobserver

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func reloadMetricSHA512(metrics []byte, metricName string) ([sha512.Size]byte, bool, error) {
	if len(metrics) == 0 || len(metrics) > maximumMetricsBytes {
		return [sha512.Size]byte{}, false, errs.New(errs.KindRequestFailed, "DNS resolver metrics response is invalid")
	}
	prefix := metricName + "{"
	found := false
	result := [sha512.Size]byte{}
	for _, line := range strings.Split(string(metrics), "\n") {
		if !strings.HasPrefix(line, metricName) {
			continue
		}
		if found || !strings.HasPrefix(line, prefix) || strings.Count(line, "{") != 1 ||
			strings.Count(line, "}") != 1 {
			return [sha512.Size]byte{}, false, errs.New(errs.KindStateConflict, "DNS resolver reload metric is invalid")
		}
		brace := strings.IndexByte(line, '}')
		labels, ok := exactMetricLabels(line[len(prefix):brace])
		if !ok || len(labels) != 2 || labels["hash"] != "sha512" || strings.TrimSpace(line[brace+1:]) != "1" {
			return [sha512.Size]byte{}, false, errs.New(errs.KindStateConflict, "DNS resolver reload metric is invalid")
		}
		decoded, err := hex.DecodeString(labels["value"])
		if err != nil || len(decoded) != sha512.Size || hex.EncodeToString(decoded) != labels["value"] {
			return [sha512.Size]byte{}, false, errs.New(errs.KindStateConflict, "DNS resolver reload metric is invalid")
		}
		copy(result[:], decoded)
		clear(decoded)
		found = true
	}
	return result, found, nil
}

func exactMetricLabels(value string) (map[string]string, bool) {
	result := make(map[string]string)
	for _, field := range strings.Split(value, ",") {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			return nil, false
		}
		key := strings.TrimSpace(parts[0])
		quoted := strings.TrimSpace(parts[1])
		if key == "" || len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' ||
			strings.Contains(quoted[1:len(quoted)-1], `"`) {
			return nil, false
		}
		if _, duplicate := result[key]; duplicate {
			return nil, false
		}
		result[key] = quoted[1 : len(quoted)-1]
	}
	return result, true
}

type counterKey struct {
	upstream string
	rcode    uint32
}

func parseForwardCounters(metrics []byte) (map[counterKey]uint64, error) {
	if len(metrics) == 0 || len(metrics) > maximumMetricsBytes {
		return nil, errs.New(errs.KindRequestFailed, "DNS resolver metrics response is invalid")
	}
	result := make(map[counterKey]uint64)
	for _, line := range strings.Split(string(metrics), "\n") {
		if !strings.HasPrefix(line, "coredns_proxy_request_duration_seconds_count{") &&
			!strings.HasPrefix(line, "coredns_forward_requests_total{") {
			continue
		}
		brace := strings.IndexByte(line, '}')
		if brace < 0 {
			return nil, errs.New(errs.KindRequestFailed, "DNS resolver forward counter is invalid")
		}
		labels := parseMetricLabels(line[strings.IndexByte(line, '{')+1 : brace])
		if proxy := labels["proxy_name"]; proxy != "" && proxy != "forward" {
			continue
		}
		upstream, err := normalizeEndpoint(labels["to"])
		if err != nil {
			return nil, err
		}
		rcode, ok := dnsRCode(labels["rcode"])
		if !ok {
			return nil, errs.New(errs.KindRequestFailed, "DNS resolver forward rcode is invalid")
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(line[brace+1:]), 64)
		if err != nil || value < 0 || value != float64(uint64(value)) {
			return nil, errs.New(errs.KindRequestFailed, "DNS resolver forward counter value is invalid")
		}
		result[counterKey{upstream: upstream, rcode: rcode}] = uint64(value)
	}
	return result, nil
}

func parseMetricLabels(value string) map[string]string {
	result := make(map[string]string)
	for _, field := range strings.Split(value, ",") {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), `"`)
		}
	}
	return result
}

func dnsRCode(value string) (uint32, bool) {
	codes := map[string]uint32{
		"NOERROR": 0, "FORMERR": 1, "SERVFAIL": 2, "NXDOMAIN": 3,
		"NOTIMP": 4, "REFUSED": 5,
	}
	code, ok := codes[value]
	return code, ok
}

func counterDelta(before, after map[counterKey]uint64) int64 {
	var delta int64
	for key, afterValue := range after {
		beforeValue := before[key]
		if afterValue < beforeValue {
			return -1
		}
		delta += int64(afterValue - beforeValue)
	}
	for key, beforeValue := range before {
		if _, found := after[key]; !found && beforeValue != 0 {
			return -1
		}
	}
	return delta
}

func selectedUpstream(before, after map[counterKey]uint64, endpoints []string) (string, bool) {
	selected := ""
	for _, endpoint := range endpoints {
		var delta uint64
		for key, afterValue := range after {
			if key.upstream != endpoint {
				continue
			}
			beforeValue := before[key]
			if afterValue < beforeValue {
				return "", false
			}
			delta += afterValue - beforeValue
		}
		if delta == 1 && selected == "" {
			selected = endpoint
		} else if delta != 0 {
			return "", false
		}
	}
	return selected, selected != ""
}

func counterEvidence(before, after map[counterKey]uint64, endpoints []string) []*agentpb.DNSForwardCounter {
	allowed := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		allowed[endpoint] = true
	}
	keys := make([]counterKey, 0)
	seen := make(map[counterKey]bool)
	for key := range before {
		if allowed[key.upstream] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	for key := range after {
		if allowed[key.upstream] && !seen[key] {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].upstream == keys[right].upstream {
			return keys[left].rcode < keys[right].rcode
		}
		return keys[left].upstream < keys[right].upstream
	})
	result := make([]*agentpb.DNSForwardCounter, 0, len(keys))
	for _, key := range keys {
		result = append(result, &agentpb.DNSForwardCounter{
			Upstream: key.upstream, Rcode: key.rcode, Before: before[key], After: after[key],
		})
	}
	return result
}

func proofHasIPv4(proof *agentpb.DNSQueryProof, expected netip.Addr) bool {
	if proof == nil || proof.GetLocalRcode() != 0 || len(proof.GetAnswers()) != 1 {
		return false
	}
	return bytes.Equal(proof.GetAnswers()[0].GetIpv4(), expected.AsSlice())
}

func proofHasRootNS(proof *agentpb.DNSQueryProof) bool {
	if proof == nil || proof.GetName() != "." || proof.GetType() != agentpb.DNSQueryType_DNS_QUERY_TYPE_NS ||
		proof.GetLocalRcode() != 0 || !proof.GetRecursionAvailable() {
		return false
	}
	for _, answer := range proof.GetAnswers() {
		if answer.GetOwnerName() == "." && answer.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS &&
			answer.GetNameServer() != "" {
			return true
		}
	}
	return false
}
