package dnsresolverobserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"sort"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type httpMetricsReader struct{ client *http.Client }

func (reader httpMetricsReader) Read(ctx context.Context, endpoint string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	response, err := reader.client.Do(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errs.New(errs.KindRequestFailed, "DNS resolver metrics endpoint is unavailable")
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, maximumMetricsBytes+1))
	if err != nil || len(value) > maximumMetricsBytes {
		clear(value)
		return nil, errs.New(errs.KindRequestFailed, "DNS resolver metrics response is invalid")
	}
	return value, nil
}

type networkDNSProber struct {
	exchange func(context.Context, string, []byte, bool) ([]byte, error)
}

const maximumDNSSectionRecords = 64

func (prober networkDNSProber) Query(
	ctx context.Context,
	endpoint string,
	name string,
	queryType agentpb.DNSQueryType,
) (*agentpb.DNSQueryProof, error) {
	messageType := dnsmessage.TypeA
	if queryType == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS {
		messageType = dnsmessage.TypeNS
	} else if queryType != agentpb.DNSQueryType_DNS_QUERY_TYPE_A {
		return nil, errs.New(errs.KindValidationFailed, "DNS query type is invalid")
	}
	dnsName, err := dnsmessage.NewName(absoluteName(name))
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS query name is invalid")
	}
	var idBytes [2]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	id := binary.BigEndian.Uint16(idBytes[:])
	query := dnsmessage.Message{
		Header:    dnsmessage.Header{ID: id, RecursionDesired: true},
		Questions: []dnsmessage.Question{{Name: dnsName, Type: messageType, Class: dnsmessage.ClassINET}},
	}
	encoded, err := query.Pack()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	response, err := prober.exchangeMessage(ctx, endpoint, encoded, false)
	if err != nil {
		return nil, err
	}
	parsed := dnsmessage.Message{}
	if !unpackExactDNSResponse(&parsed, response, id, query.Questions[0], true) {
		return nil, errs.New(errs.KindRequestFailed, "DNS UDP response is invalid")
	}
	usedTCP := false
	if parsed.Header.Truncated {
		response, err = prober.exchangeMessage(ctx, endpoint, encoded, true)
		if err != nil {
			return nil, err
		}
		parsed = dnsmessage.Message{}
		if !unpackExactDNSResponse(&parsed, response, id, query.Questions[0], false) {
			return nil, errs.New(errs.KindRequestFailed, "DNS TCP response is invalid")
		}
		usedTCP = true
	}
	proof := &agentpb.DNSQueryProof{
		Name: absoluteName(name), Type: queryType, LocalRcode: uint32(parsed.Header.RCode),
		RecursionAvailable: parsed.Header.RecursionAvailable, LocalUsedTcp: usedTCP,
	}
	for _, answer := range parsed.Answers {
		switch resource := answer.Body.(type) {
		case *dnsmessage.AResource:
			proof.Answers = append(proof.Answers, &agentpb.DNSAnswerRecord{
				OwnerName: answer.Header.Name.String(), Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A,
				Ipv4: append([]byte(nil), resource.A[:]...),
			})
		case *dnsmessage.NSResource:
			proof.Answers = append(proof.Answers, &agentpb.DNSAnswerRecord{
				OwnerName: answer.Header.Name.String(), Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: resource.NS.String(),
			})
		}
	}
	sort.Slice(proof.Answers, func(left, right int) bool {
		if proof.Answers[left].GetOwnerName() == proof.Answers[right].GetOwnerName() {
			return bytes.Compare(proof.Answers[left].GetIpv4(), proof.Answers[right].GetIpv4()) < 0 ||
				proof.Answers[left].GetNameServer() < proof.Answers[right].GetNameServer()
		}
		return proof.Answers[left].GetOwnerName() < proof.Answers[right].GetOwnerName()
	})
	return proof, nil
}

func unpackExactDNSResponse(
	parsed *dnsmessage.Message,
	response []byte,
	id uint16,
	question dnsmessage.Question,
	allowTruncated bool,
) bool {
	if len(response) == 0 || len(response) > 65535 || parsed.Unpack(response) != nil ||
		!parsed.Header.Response || parsed.Header.ID != id || (!allowTruncated && parsed.Header.Truncated) ||
		len(parsed.Questions) != 1 || parsed.Questions[0] != question ||
		len(parsed.Answers) > maximumDNSSectionRecords ||
		len(parsed.Authorities) > maximumDNSSectionRecords ||
		len(parsed.Additionals) > maximumDNSSectionRecords {
		return false
	}
	return true
}

func (prober networkDNSProber) exchangeMessage(
	ctx context.Context,
	endpoint string,
	query []byte,
	tcp bool,
) ([]byte, error) {
	if prober.exchange != nil {
		return prober.exchange(ctx, endpoint, query, tcp)
	}
	return exchangeDNS(ctx, endpoint, query, tcp)
}

func exchangeDNS(ctx context.Context, endpoint string, query []byte, tcp bool) ([]byte, error) {
	network := "udp"
	if tcp {
		network = "tcp"
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, network, endpoint)
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if tcp {
		framed := make([]byte, 2+len(query))
		binary.BigEndian.PutUint16(framed[:2], uint16(len(query)))
		copy(framed[2:], query)
		if _, err := connection.Write(framed); err != nil {
			return nil, errs.Wrap(errs.KindRequestFailed, err)
		}
		header := make([]byte, 2)
		if _, err := io.ReadFull(connection, header); err != nil {
			return nil, errs.Wrap(errs.KindRequestFailed, err)
		}
		response := make([]byte, int(binary.BigEndian.Uint16(header)))
		if len(response) == 0 || len(response) > 65535 {
			return nil, errs.New(errs.KindRequestFailed, "DNS TCP response length is invalid")
		}
		if _, err := io.ReadFull(connection, response); err != nil {
			return nil, errs.Wrap(errs.KindRequestFailed, err)
		}
		return response, nil
	}
	if _, err := connection.Write(query); err != nil {
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	response := make([]byte, 65535)
	count, err := connection.Read(response)
	if err != nil || count == 0 {
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	return response[:count], nil
}
