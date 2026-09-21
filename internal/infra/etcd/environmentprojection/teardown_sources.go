package environmentprojection

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func CloneManagedComponentRuntimeSources(sources []ManagedComponentRuntimeSource) []ManagedComponentRuntimeSource {
	return append([]ManagedComponentRuntimeSource(nil), sources...)
}

// ValidateManagedComponentTeardownSources validates the closed source shape
// stored on a Task. Artifact and Service ownership remain checked when the
// historical artifact is resolved for teardown.
func ValidateManagedComponentTeardownSources(sources []ManagedComponentRuntimeSource) error {
	if len(sources) > MaximumManagedComponentRuntimeSources {
		return errs.New(errs.KindValidationFailed, "managed Component teardown source count is invalid")
	}
	seenComponents := make(map[string]struct{}, len(sources))
	previousKind := core.ComponentKind("")
	for _, source := range sources {
		switch source.ComponentKind {
		case core.ComponentKindIngressCaddy, core.ComponentKindEdgeCloudflare, core.ComponentKindCoreDNS:
		default:
			return errs.New(errs.KindValidationFailed, "managed Component teardown source kind is invalid")
		}
		if source.ComponentKind <= previousKind ||
			ids.Validate(ids.KindComponent, source.ComponentID) != nil ||
			ids.Validate(ids.KindService, source.ServiceID) != nil ||
			source.ComposeName == "" || !utf8.ValidString(source.ComposeName) ||
			ids.Validate(ids.KindTask, source.RevisionID) != nil ||
			ids.Validate(ids.KindConfig, source.ArtifactID) != nil ||
			!recordcodec.ValidSHA256(source.ArtifactSHA256) {
			return errs.New(errs.KindValidationFailed, "managed Component teardown source is invalid or unsorted")
		}
		if _, duplicate := seenComponents[source.ComponentID]; duplicate {
			return errs.New(errs.KindValidationFailed, "managed Component teardown source Component is duplicated")
		}
		seenComponents[source.ComponentID] = struct{}{}
		previousKind = source.ComponentKind
	}
	return nil
}
