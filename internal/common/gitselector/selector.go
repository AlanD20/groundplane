// Package gitselector validates a GitHub Actions Runner label and optional group.
package gitselector

import "regexp"

var (
	labelPattern = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]{0,99}\z`)
	groupPattern = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._ -]{0,99}\z`)
)

type Selector struct {
	Label   string `json:"label"`
	Group   string `json:"group,omitempty"`
	GroupID int64  `json:"group_id,omitempty"`
}

func (selector Selector) Valid() bool {
	return labelPattern.MatchString(selector.Label) && ((selector.Group == "" && selector.GroupID == 0) ||
		(selector.GroupID > 0 && groupPattern.MatchString(selector.Group)))
}
