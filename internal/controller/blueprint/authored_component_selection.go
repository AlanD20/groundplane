package blueprint

import (
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func authoredComponentSpec(
	identities []projectionrecord.OwnedIdentity,
	specs map[string]core.ComponentSpec,
	componentID string,
) (core.ComponentCapability, core.ComponentSpec, bool) {
	for _, identity := range identities {
		if identity.ID == componentID {
			spec, found := specs[identity.Name]
			return core.ComponentCapability(identity.Name), spec, found
		}
	}
	return "", core.ComponentSpec{}, false
}

func omittedComponentSelection(record componentrecord.Record) (core.ComponentCapability, core.ComponentSpec, error) {
	capability, supported := record.Desired.Kind.EnvironmentCapability()
	if !supported {
		return "", core.ComponentSpec{}, errs.New(
			errs.KindStateConflict,
			"omitted Component is not an Environment singleton",
		)
	}
	return capability, core.ComponentSpec{Implementation: record.Desired.Kind, Enabled: false}, nil
}
