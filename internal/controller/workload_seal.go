package controller

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func applySealedWorkload(service *composetypes.ServiceConfig, seal domain.WorkloadSeal) error {
	if err := domain.ValidateWorkloadSeal(seal); err != nil {
		return err
	}
	replicas := int(seal.ReplicaCount)
	if replicas < 1 || uint64(replicas) != uint64(seal.ReplicaCount) {
		return errs.New(errs.KindValidationFailed, "sealed replica count exceeds host representation")
	}
	deploy := composetypes.DeployConfig{}
	if service.Deploy != nil {
		deploy = *service.Deploy
	}
	deploy.Replicas = &replicas
	service.Deploy = &deploy
	service.Image = seal.LocalImageID
	service.PullPolicy = "never"
	return nil
}
