// Package dnsproof owns canonical DNS resolver observation proof integrity.
package dnsproof

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func Seal(evidence *agentpb.DNSResolverObservationEvidence) error {
	if err := validateSemantics(evidence); err != nil {
		return err
	}
	digest, err := Digest(evidence)
	if err != nil {
		return err
	}
	evidence.ProofSha256 = append(evidence.ProofSha256[:0], digest[:]...)
	return nil
}

func Verify(evidence *agentpb.DNSResolverObservationEvidence) error {
	if evidence == nil || len(evidence.GetProofSha256()) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "DNS resolver observation proof digest is invalid")
	}
	if err := validateSemantics(evidence); err != nil {
		return err
	}
	digest, err := Digest(evidence)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(digest[:], evidence.GetProofSha256()) != 1 {
		return errs.New(errs.KindValidationFailed, "DNS resolver observation proof digest changed")
	}
	return nil
}

func validateSemantics(evidence *agentpb.DNSResolverObservationEvidence) error {
	if evidence == nil || !validForwardQuery(evidence.GetCatchAllQuery(), true) ||
		len(evidence.GetForwarderQueries()) > 8 {
		return errs.New(errs.KindValidationFailed, "DNS resolver observation proof semantics are invalid")
	}
	if static := evidence.GetStaticQuery(); static != nil && !validStaticQuery(static) {
		return errs.New(errs.KindValidationFailed, "DNS resolver static proof semantics are invalid")
	}
	seen := make(map[string]struct{}, len(evidence.GetForwarderQueries()))
	for _, proof := range evidence.GetForwarderQueries() {
		if !validForwardQuery(proof, false) {
			return errs.New(errs.KindValidationFailed, "DNS resolver forwarder proof semantics are invalid")
		}
		if _, found := seen[proof.GetName()]; found {
			return errs.New(errs.KindValidationFailed, "DNS resolver forwarder proof is duplicated")
		}
		seen[proof.GetName()] = struct{}{}
	}
	return nil
}

func validStaticQuery(proof *agentpb.DNSQueryProof) bool {
	if proof == nil || proof.GetName() == "." || !validAbsoluteDNSName(proof.GetName()) ||
		proof.GetType() != agentpb.DNSQueryType_DNS_QUERY_TYPE_A || proof.GetLocalRcode() != 0 ||
		proof.GetAttempts() == 0 || proof.GetAttempts() > 3 || proof.GetSelectedUpstream() != "" ||
		proof.GetDirectRcode() != 0 || proof.GetDirectUsedTcp() || len(proof.GetAnswers()) != 1 ||
		!validUnchangedCounters(proof.GetCounters()) {
		return false
	}
	answer := proof.GetAnswers()[0]
	return answer != nil && answer.GetOwnerName() == proof.GetName() &&
		answer.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_A && len(answer.GetIpv4()) == 4 &&
		answer.GetNameServer() == ""
}

func validForwardQuery(proof *agentpb.DNSQueryProof, catchAll bool) bool {
	if proof == nil || !validAbsoluteDNSName(proof.GetName()) || proof.GetAttempts() == 0 ||
		proof.GetAttempts() > 3 || proof.GetLocalRcode() > 5 || proof.GetDirectRcode() != proof.GetLocalRcode() ||
		!validEndpoint(proof.GetSelectedUpstream()) || !validAnswers(proof.GetAnswers()) ||
		!validSelectedCounter(proof.GetCounters(), proof.GetSelectedUpstream(), proof.GetLocalRcode()) {
		return false
	}
	if catchAll {
		if proof.GetName() != "." || proof.GetType() != agentpb.DNSQueryType_DNS_QUERY_TYPE_NS ||
			proof.GetLocalRcode() != 0 || !proof.GetRecursionAvailable() {
			return false
		}
		for _, answer := range proof.GetAnswers() {
			if answer.GetOwnerName() == "." && answer.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS &&
				validAbsoluteDNSName(answer.GetNameServer()) {
				return true
			}
		}
		return false
	}
	return proof.GetName() != "." && proof.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_A
}

func validAnswers(answers []*agentpb.DNSAnswerRecord) bool {
	if len(answers) > 64 {
		return false
	}
	for _, answer := range answers {
		if answer == nil || !validAbsoluteDNSName(answer.GetOwnerName()) {
			return false
		}
		switch answer.GetType() {
		case agentpb.DNSQueryType_DNS_QUERY_TYPE_A:
			if len(answer.GetIpv4()) != 4 || answer.GetNameServer() != "" {
				return false
			}
		case agentpb.DNSQueryType_DNS_QUERY_TYPE_NS:
			if len(answer.GetIpv4()) != 0 || !validAbsoluteDNSName(answer.GetNameServer()) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func validSelectedCounter(counters []*agentpb.DNSForwardCounter, selected string, rcode uint32) bool {
	if len(counters) == 0 || len(counters) > 128 {
		return false
	}
	selectedDelta := uint64(0)
	previousUpstream := ""
	previousRcode := uint32(0)
	for index, counter := range counters {
		if counter == nil || !validEndpoint(counter.GetUpstream()) || counter.GetRcode() > 5 ||
			counter.GetAfter() < counter.GetBefore() ||
			(index > 0 && (counter.GetUpstream() < previousUpstream ||
				counter.GetUpstream() == previousUpstream && counter.GetRcode() <= previousRcode)) {
			return false
		}
		delta := counter.GetAfter() - counter.GetBefore()
		if counter.GetUpstream() == selected && counter.GetRcode() == rcode {
			selectedDelta += delta
		} else if delta != 0 {
			return false
		}
		previousUpstream, previousRcode = counter.GetUpstream(), counter.GetRcode()
	}
	return selectedDelta == 1
}

func validUnchangedCounters(counters []*agentpb.DNSForwardCounter) bool {
	if len(counters) > 128 {
		return false
	}
	previousUpstream := ""
	previousRcode := uint32(0)
	for index, counter := range counters {
		if counter == nil || !validEndpoint(counter.GetUpstream()) || counter.GetRcode() > 5 ||
			counter.GetAfter() != counter.GetBefore() ||
			(index > 0 && (counter.GetUpstream() < previousUpstream ||
				counter.GetUpstream() == previousUpstream && counter.GetRcode() <= previousRcode)) {
			return false
		}
		previousUpstream, previousRcode = counter.GetUpstream(), counter.GetRcode()
	}
	return true
}

func validEndpoint(value string) bool {
	host, portText, err := net.SplitHostPort(value)
	if err != nil || host == "" || strings.TrimSpace(host) != host {
		return false
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	return err == nil && port != 0
}

func validAbsoluteDNSName(value string) bool {
	if value == "." {
		return true
	}
	if len(value) < 2 || len(value) > 254 || !strings.HasSuffix(value, ".") {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(value, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !(character == '-' || character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' || character >= '0' && character <= '9') {
				return false
			}
		}
	}
	return true
}

func Digest(evidence *agentpb.DNSResolverObservationEvidence) ([sha256.Size]byte, error) {
	if evidence == nil {
		return [sha256.Size]byte{}, errs.New(errs.KindValidationFailed, "DNS resolver observation proof is missing")
	}
	owned := proto.Clone(evidence).(*agentpb.DNSResolverObservationEvidence)
	clear(owned.ProofSha256)
	owned.ProofSha256 = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	return sha256.Sum256(encoded), nil
}

func Marshal(evidence *agentpb.DNSResolverObservationEvidence) ([]byte, error) {
	if err := Verify(evidence); err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(evidence)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}

func Unmarshal(encoded []byte) (*agentpb.DNSResolverObservationEvidence, error) {
	if len(encoded) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver observation proof bytes are missing")
	}
	evidence := &agentpb.DNSResolverObservationEvidence{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, evidence); err != nil ||
		len(evidence.ProtoReflect().GetUnknown()) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver observation proof bytes are invalid")
	}
	if err := Verify(evidence); err != nil {
		return nil, err
	}
	return evidence, nil
}
