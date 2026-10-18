package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Existing Attaches must still match the authored consumer and backing
// topology. Select only retained Attaches: this validates candidates without
// preparing another publication or generating credentials.
func (service *Service) validateAuthoredExistingAttaches(
	ctx context.Context,
	environmentID, parentID string,
	specs map[string]core.AttachmentSpec,
	desiredServices []core.Service,
	current []etcdstore.Versioned[attachrecord.Record],
) error {
	if len(current) == 0 || len(specs) == 0 {
		return nil
	}
	retained := make(map[string]core.AttachmentSpec)
	for _, attach := range current {
		if spec, present := specs[attach.Record.Name]; present {
			retained[attach.Record.Name] = spec
		}
	}
	if len(retained) == 0 {
		return nil
	}
	changes := make([]blueprints.EnvironmentBlueprintServiceChange, len(desiredServices))
	for index, desired := range desiredServices {
		changes[index].Record = servicerecord.ServiceRecord{
			EnvironmentID: environmentID, Desired: desired,
		}
	}
	prepared, err := service.prepareBlueprintAttachCandidates(
		ctx, environmentID, parentID, retained, changes, current,
		func(ids.Kind, string) string { return "" }, service.now().UTC(),
	)
	defer prepared.clear()
	return err
}
