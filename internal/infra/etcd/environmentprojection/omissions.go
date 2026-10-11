package environmentprojection

import (
	"cmp"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// OwnedOmission retains an admitted identity no longer declared by this head.
// It is cleanup intent, not evidence of runtime existence or absence. Native
// removal captures and fences the actual applied source separately.
type OwnedOmission struct {
	Kind             ids.Kind `json:"kind"`
	ID               string   `json:"id"`
	SourceRevisionID string   `json:"source_revision_id"`
}

func ownedIdentityTargets(value EnvironmentOwnedIdentities) []OwnedOmission {
	var targets []OwnedOmission
	appendKind := func(kind ids.Kind, identities []OwnedIdentity) {
		for _, identity := range identities {
			targets = append(targets, OwnedOmission{Kind: kind, ID: identity.ID})
		}
	}
	appendKind(ids.KindService, value.Services)
	appendKind(ids.KindNetwork, value.Networks)
	appendKind(ids.KindVolume, value.Volumes)
	appendKind(ids.KindEnvEntry, value.Entries)
	appendKind(ids.KindRoute, value.Routes)
	appendKind(ids.KindDNSRecord, value.DNSRecords)
	appendKind(ids.KindAttach, value.Attaches)
	appendKind(ids.KindComponent, value.Components)
	appendKind(ids.KindScript, value.Scripts)
	return targets
}

// RetainOwnedOmissions carries cleanup intent across superseding heads, including
// directly-created resources that have no Blueprint-unit receipt. A redeclared
// stable identity is no longer an omission. Settled absence remains in the ledger
// and is the only proof allowing the planner to skip that removal.
func RetainOwnedOmissions(current, previous EnvironmentOwnedIdentities) []OwnedOmission {
	selected := make(map[OwnedOmission]bool)
	for _, target := range ownedIdentityTargets(current) {
		selected[target] = true
	}
	seen := make(map[OwnedOmission]bool)
	var omissions []OwnedOmission
	prior := ownedIdentityTargets(previous)
	for index := range prior {
		prior[index].SourceRevisionID = previous.RevisionID
	}
	prior = append(prior, previous.PendingOmissions...)
	for _, target := range prior {
		key := omissionIdentity(target)
		// Volume omission is rejected at admission. Its separate protected
		// removal must not become another Blueprint cleanup intent.
		if target.Kind != ids.KindVolume && !selected[key] && !seen[key] {
			seen[key] = true
			omissions = append(omissions, target)
		}
	}
	slices.SortFunc(omissions, compareOwnedOmissions)
	return omissions
}

func omissionIdentity(target OwnedOmission) OwnedOmission {
	return OwnedOmission{Kind: target.Kind, ID: target.ID}
}

func compareOwnedOmissions(left, right OwnedOmission) int {
	if order := cmp.Compare(left.Kind, right.Kind); order != 0 {
		return order
	}
	return cmp.Compare(left.ID, right.ID)
}

func validOwnedOmissions(value EnvironmentOwnedIdentities) bool {
	if len(value.PendingOmissions) > 4096 {
		return false
	}
	selected := make(map[OwnedOmission]bool)
	for _, target := range ownedIdentityTargets(value) {
		selected[target] = true
	}
	for index, target := range value.PendingOmissions {
		switch target.Kind {
		case ids.KindService, ids.KindNetwork, ids.KindEnvEntry,
			ids.KindRoute, ids.KindDNSRecord, ids.KindAttach, ids.KindComponent, ids.KindScript:
		default:
			return false
		}
		if ids.Validate(target.Kind, target.ID) != nil ||
			ids.Validate(ids.KindTask, target.SourceRevisionID) != nil || selected[omissionIdentity(target)] ||
			index > 0 && compareOwnedOmissions(value.PendingOmissions[index-1], target) >= 0 {
			return false
		}
	}
	return true
}
