package etcd

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ProjectManagedComponentRuntimeSources binds possible managed runtime to an
// immutable desired revision. Failed attempts therefore remain removable by a
// later Task without depending on the short-lived Component candidate record.
func ProjectManagedComponentRuntimeSources(
	preparation ComponentTaskPreparation,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]projectionrecord.ManagedComponentRuntimeSource, error) {
	sources := append([]projectionrecord.ManagedComponentRuntimeSource(nil), projection.ManagedComponentRuntimeSources...)
	if !componentTaskPreparationIsZero(preparation) {
		if err := validateComponentTaskPreparation(preparation); err != nil {
			return nil, err
		}
		sources = append(sources[:0], preparation.managedRuntimeSources...)
	}

	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "Blueprint managed Component artifact is corrupt")
	}
	for _, candidate := range preparation.Intent.Candidates {
		if !candidate.Candidate.Desired.Enabled {
			continue
		}
		if len(candidate.Candidate.Runtime.GeneratedServices) != 1 {
			return nil, errs.New(errs.KindInternal, "enabled Component candidate runtime identity is incomplete")
		}
		source, err := projectionrecord.ManagedComponentRuntimeSource(
			projection,
			candidate.Candidate,
			artifact,
		)
		if err != nil {
			return nil, err
		}
		sources = projectionrecord.ReplaceManagedComponentRuntimeSource(sources, source)
	}
	sort.Slice(sources, func(left, right int) bool {
		return sources[left].ComponentKind < sources[right].ComponentKind
	})
	projected := projection
	projected.ManagedComponentRuntimeSources = sources
	if err := projectionrecord.ValidateManagedComponentRuntimeSources(projected); err != nil {
		return nil, err
	}
	return append([]projectionrecord.ManagedComponentRuntimeSource(nil), sources...), nil
}
