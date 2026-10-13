package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Existing bindings have already passed authored consumer/backing/credential
// validation. Adopt their stable IDs rather than allocate a second Attach for
// the same name. Pending declarations without a binding retain their own IDs.
func selectAuthoredAttachIdentities(
	specs map[string]core.AttachmentSpec,
	previous []projectionrecord.OwnedIdentity,
	current []etcdstore.Versioned[attachrecord.Record],
	allocate func(ids.Kind, string) string,
) ([]composeidentity.Resource, error) {
	bindings := make(map[string]string, len(current))
	for _, binding := range current {
		bindings[binding.Record.Name] = binding.Record.ID
	}
	prior := make(map[string]string, len(previous))
	for _, identity := range previous {
		prior[identity.Name] = identity.ID
	}
	result := make([]composeidentity.Resource, 0, len(specs))
	for _, name := range sortedAuthoredMapKeys(specs) {
		id := prior[name]
		if bound := bindings[name]; bound != "" {
			if id != "" && id != bound {
				return nil, errs.New(errs.KindStateConflict,
					"authored Attach identity differs from its existing binding")
			}
			id = bound
		}
		if id == "" {
			id = allocate(ids.KindAttach, "attach:"+name)
		}
		result = append(result, composeidentity.Resource{ID: id, Name: name})
	}
	return result, nil
}
