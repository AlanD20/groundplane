package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const hostResolutionProjectionKey = "/v1/records/platform/host-resolution/projection"

type HostResolutionRouteRecord struct {
	EnvironmentID     string `json:"environment_id"`
	DesiredRevisionID string `json:"desired_revision_id"`
	AppliedRevision   int64  `json:"applied_revision"`
	RouteID           string `json:"route_id"`
	ServiceID         string `json:"service_id"`
	Hostname          string `json:"hostname"`
	IPv4              string `json:"ipv4"`
}

type HostResolutionProjectionRecord struct {
	InputRevision int64                       `json:"input_revision"`
	InputSHA256   string                      `json:"input_sha256"`
	Routes        []HostResolutionRouteRecord `json:"routes"`
}

func NewHostResolutionProjectionRecord(
	inputRevision int64,
	routes []HostResolutionRouteRecord,
) (HostResolutionProjectionRecord, error) {
	record := HostResolutionProjectionRecord{InputRevision: inputRevision, Routes: cloneHostResolutionRoutes(routes)}
	if record.Routes == nil {
		record.Routes = []HostResolutionRouteRecord{}
	}
	canonicalizeHostResolutionRoutes(record.Routes)
	digest, err := hostResolutionRoutesDigest(record.Routes)
	if err != nil {
		return HostResolutionProjectionRecord{}, err
	}
	record.InputSHA256 = hex.EncodeToString(digest[:])
	if err := validateHostResolutionProjectionRecord(record); err != nil {
		return HostResolutionProjectionRecord{}, err
	}
	return record, nil
}

func (repository *ComponentRepository) GetHostResolutionProjection(
	ctx context.Context,
) (etcdstore.Versioned[HostResolutionProjectionRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, hostResolutionProjectionKey)
	if err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return etcdstore.Versioned[HostResolutionProjectionRecord]{ReadRevision: readRevision}, false, nil
	}
	record, err := decodeHostResolutionProjectionRecord(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	return etcdstore.Versioned[HostResolutionProjectionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

type hostResolutionProjectionPublication struct {
	record     HostResolutionProjectionRecord
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func prepareHostResolutionProjectionPublication(
	current *etcdstore.KeyValue,
	inputRevision int64,
	routes []HostResolutionRouteRecord,
) (hostResolutionProjectionPublication, error) {
	condition := etcdstore.Condition{Key: hostResolutionProjectionKey}
	if current != nil {
		stored, err := decodeHostResolutionProjectionRecord(current.Value)
		if err != nil {
			return hostResolutionProjectionPublication{}, err
		}
		if inputRevision < stored.InputRevision {
			return hostResolutionProjectionPublication{}, errs.New(
				errs.KindStateConflict,
				"host-resolution input revision is stale",
			)
		}
		condition.ModRevision = current.ModRevision
	}
	record, err := NewHostResolutionProjectionRecord(inputRevision, routes)
	if err != nil {
		return hostResolutionProjectionPublication{}, err
	}
	value, err := encodeHostResolutionProjectionRecord(record)
	if err != nil {
		return hostResolutionProjectionPublication{}, err
	}
	return hostResolutionProjectionPublication{
		record: record, conditions: []etcdstore.Condition{condition},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: hostResolutionProjectionKey, Value: value}},
		values:    [][]byte{value},
	}, nil
}

func (publication hostResolutionProjectionPublication) clear() {
	for _, value := range publication.values {
		clear(value)
	}
}

func encodeHostResolutionProjectionRecord(record HostResolutionProjectionRecord) ([]byte, error) {
	if err := validateHostResolutionProjectionRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("host_resolution_projection", record)
}

func decodeHostResolutionProjectionRecord(value []byte) (HostResolutionProjectionRecord, error) {
	record, err := recordcodec.Decode[HostResolutionProjectionRecord](value, "host_resolution_projection")
	if err != nil || validateHostResolutionProjectionRecord(record) != nil {
		return HostResolutionProjectionRecord{}, errs.New(errs.KindInternal, "host-resolution projection is corrupt")
	}
	return cloneHostResolutionProjection(record), nil
}

func validateHostResolutionProjectionRecord(record HostResolutionProjectionRecord) error {
	if record.InputRevision <= 0 || !recordcodec.ValidSHA256(record.InputSHA256) || record.Routes == nil {
		return errs.New(errs.KindValidationFailed, "host-resolution projection identity is invalid")
	}
	previous := ""
	seenRoutes := make(map[string]struct{}, len(record.Routes))
	seenHosts := make(map[string]string, len(record.Routes))
	for _, route := range record.Routes {
		if recordcodec.ValidateID(ids.KindEnvironment, route.EnvironmentID) != nil ||
			recordcodec.ValidateID(ids.KindTask, route.DesiredRevisionID) != nil || route.AppliedRevision <= 0 ||
			recordcodec.ValidateID(
				ids.KindRoute,
				route.RouteID,
			) != nil || recordcodec.ValidateID(ids.KindService, route.ServiceID) != nil ||
			!validHostResolutionName(route.Hostname) || !validHostResolutionAddress(route.IPv4) {
			return errs.New(errs.KindValidationFailed, "host-resolution Route record is invalid")
		}
		identity := route.EnvironmentID + "\x00" + route.RouteID
		if _, duplicate := seenRoutes[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "host-resolution Route identity is duplicated")
		}
		seenRoutes[identity] = struct{}{}
		key := hostResolutionRouteKey(route)
		if key <= previous {
			return errs.New(errs.KindValidationFailed, "host-resolution Route records are not sorted")
		}
		previous = key
		if address, exists := seenHosts[route.Hostname]; exists && address != route.IPv4 {
			return errs.New(errs.KindValidationFailed, "host-resolution hostname maps to multiple addresses")
		}
		seenHosts[route.Hostname] = route.IPv4
	}
	digest, err := hostResolutionRoutesDigest(record.Routes)
	if err != nil || hex.EncodeToString(digest[:]) != record.InputSHA256 {
		return errs.New(errs.KindValidationFailed, "host-resolution projection digest is invalid")
	}
	return nil
}

func cloneHostResolutionProjection(record HostResolutionProjectionRecord) HostResolutionProjectionRecord {
	record.Routes = cloneHostResolutionRoutes(record.Routes)
	return record
}

func cloneHostResolutionRoutes(routes []HostResolutionRouteRecord) []HostResolutionRouteRecord {
	return append([]HostResolutionRouteRecord(nil), routes...)
}

func canonicalizeHostResolutionRoutes(routes []HostResolutionRouteRecord) {
	sort.Slice(routes, func(left, right int) bool {
		return hostResolutionRouteKey(routes[left]) < hostResolutionRouteKey(routes[right])
	})
}

func hostResolutionRouteKey(route HostResolutionRouteRecord) string {
	return strings.Join([]string{
		route.EnvironmentID, route.Hostname, route.RouteID, route.ServiceID, route.DesiredRevisionID,
		fmt.Sprintf("%020d", route.AppliedRevision), route.IPv4,
	}, "\x00")
}

func hostResolutionRoutesDigest(routes []HostResolutionRouteRecord) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(routes)
	if err != nil {
		return [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	return sha256.Sum256(encoded), nil
}

func validHostResolutionAddress(value string) bool {
	address, err := netip.ParseAddr(value)
	return err == nil && address.Is4() && !address.Is4In6() && !address.IsUnspecified() &&
		!address.IsMulticast() && address.String() == value
}

func validHostResolutionName(value string) bool {
	if value == "" || len(value) > 253 || value[len(value)-1] == '.' {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !(character == '-' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return false
			}
		}
	}
	return true
}
