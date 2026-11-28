// Package publicingress owns durable Platform ingress publication and claims.
package publicingress

import (
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/ids"
)

type Desired struct {
	Enabled   bool         `json:"enabled"`
	Addresses []netip.Addr `json:"addresses"`
	RouteIDs  []string     `json:"route_ids"`
}

type Claim struct {
	Hostname      string `json:"hostname"`
	EnvironmentID string `json:"environment_id"`
}

type Publication struct {
	TaskID           string  `json:"task_id"`
	Revision         int64   `json:"revision"`
	Desired          Desired `json:"desired"`
	Digest           string  `json:"digest"`
	Configuration    []byte  `json:"configuration"`
	Claims           []Claim `json:"claims"`
	ReviewToken      string  `json:"review_token"`
	ReviewedRevision int64   `json:"reviewed_revision"`
	Authorized       bool    `json:"authorized,omitempty"`
}

type Receipt struct {
	RuntimeID string `json:"runtime_id"`
	Revision  int64  `json:"revision"`
	Digest    string `json:"digest"`
}

type State struct {
	HostID           string       `json:"host_id"`
	Revision         int64        `json:"-"`
	LastIssued       int64        `json:"last_issued"`
	Applied          *Publication `json:"applied,omitempty"`
	Active           *Receipt     `json:"active,omitempty"`
	Pending          *Publication `json:"pending,omitempty"`
	LastFailedTaskID string       `json:"last_failed_task_id,omitempty"`
}

func Digest(configuration []byte) string {
	sum := sha256.Sum256(configuration)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (publication Publication) Matches(receipt Receipt) bool {
	return publication.Revision == receipt.Revision && publication.Digest == receipt.Digest &&
		validHex(receipt.RuntimeID)
}

func validHex(value string) bool {
	_, err := hex.DecodeString(value)
	return len(value) == 64 && strings.ToLower(value) == value && err == nil
}

func (desired Desired) Validate() bool {
	return desired.Enabled && len(desired.Addresses) > 0 && len(desired.Addresses) <= 16 &&
		len(desired.RouteIDs) <= 512 &&
		ordered(desired.Addresses, netip.Addr.Compare, func(address netip.Addr) bool {
			return address.IsGlobalUnicast() && !address.Is4In6() && address.Zone() == ""
		}) &&
		ordered(desired.RouteIDs, strings.Compare, func(id string) bool {
			return ids.Validate(ids.KindRoute, id) == nil
		})
}

func validPublication(item *Publication, latest int64) bool {
	return item != nil && item.Revision > 0 && item.Revision <= latest &&
		ids.Validate(ids.KindTask, item.TaskID) == nil && item.Desired.Validate() &&
		item.ReviewedRevision >= 0 &&
		len(item.Configuration) > 0 && len(item.Configuration) <= 256<<10 &&
		item.Digest == Digest(item.Configuration) && validHex(item.ReviewToken) &&
		len(item.Claims) <= len(item.Desired.RouteIDs) &&
		ordered(
			item.Claims,
			func(a, b Claim) int { return strings.Compare(a.Hostname, b.Hostname) },
			func(claim Claim) bool {
				return dnsname.Valid(claim.Hostname) && ids.Validate(ids.KindEnvironment, claim.EnvironmentID) == nil
			},
		)
}

func (state State) valid() bool {
	if ids.Validate(ids.KindHost, state.HostID) != nil || state.Revision < 0 || state.LastIssued < 0 ||
		(state.Applied == nil) != (state.Active == nil) {
		return false
	}
	if state.Applied != nil && (!validPublication(state.Applied, state.LastIssued) ||
		!state.Applied.Authorized || !state.Applied.Matches(*state.Active)) {
		return false
	}
	if state.Pending != nil && (!validPublication(state.Pending, state.LastIssued) ||
		state.Pending.Revision != state.LastIssued ||
		state.Applied != nil && state.Pending.Revision <= state.Applied.Revision) {
		return false
	}
	return state.LastFailedTaskID == "" || ids.Validate(ids.KindTask, state.LastFailedTaskID) == nil
}

func ordered[T any](values []T, compare func(T, T) int, valid func(T) bool) bool {
	for index, value := range values {
		if !valid(value) || index > 0 && compare(values[index-1], value) >= 0 {
			return false
		}
	}
	return true
}
