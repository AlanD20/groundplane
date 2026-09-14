// Package backingendpoint owns the canonical runtime DNS identity for one backing Service.
package backingendpoint

import "strings"

const prefix = "gp-"

// New returns the stable DNS label published by a backing Service and used by Attach facts.
// The caller supplies a validated canonical Service id from durable backing state.
func New(serviceID string) string {
	return prefix + strings.ReplaceAll(strings.ToLower(serviceID), "_", "-")
}
