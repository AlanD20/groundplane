package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/app/gitdeliveryplatform"
	"github.com/AlanD20/groundplane/internal/app/publicingressplatform"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/publicingressoperator"
	"github.com/AlanD20/groundplane/internal/controller/remoteimage"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
)

func DeploymentWork(
	git *gitdeliveryplatform.Runtime,
	ingress publicingressplatform.Controller,
	images *remoteimage.Service,
	channel *channeltransport.Runtime,
	preparations []taskdependencies.Registration,
) ([]taskdependencies.Registration, WorkExtensions) {
	var work WorkExtensions
	if owner := git.RunnerMigrationWork; owner != nil {
		operation := workexecution.RunnerScopedMigration
		preparations = append(preparations, owner.AcceptedPreparation())
		work.Admission = append(
			work.Admission,
			workdispatch.Registration{Operation: operation, Executor: workexecution.Controller, Owner: owner},
		)
		work.Controller = append(
			work.Controller,
			workdispatch.ControllerRegistration{Operation: operation, Owner: owner},
		)
		work.Retry = append(work.Retry, workdispatch.RetryRegistration{Operation: operation, Owner: owner})
		work.Completion = append(
			work.Completion,
			workdispatch.CompletionRegistration{Action: workexecution.ActionUpdate, Operation: operation, Owner: owner},
		)
		work.Retention = append(work.Retention, operationretention.WorkRegistration{Operation: operation, Owner: owner})
	}
	if ingress.Work != nil {
		owner, operation := ingress.Work, workexecution.PublicIngress
		preparations = append(preparations, owner.AcceptedPreparation())
		work.Admission = append(
			work.Admission,
			workdispatch.Registration{Operation: operation, Executor: workexecution.Agent, Owner: owner},
		)
		work.AgentStart = append(
			work.AgentStart,
			workdispatch.AgentStartRegistration{Operation: operation, Owner: owner},
		)
		work.Retry = append(work.Retry, workdispatch.RetryRegistration{Operation: operation, Owner: owner})
		work.Completion = append(
			work.Completion,
			workdispatch.CompletionRegistration{
				Action:    publicingressoperator.TaskAction,
				Operation: operation,
				Owner:     owner,
			},
		)
		work.Retention = append(work.Retention, operationretention.WorkRegistration{Operation: operation, Owner: owner})
		work.AgentDelivery = func(deliveries *workdispatch.AgentDeliveries) error {
			channel.ConfigurePublicIngress(owner)
			return deliveries.ConfigurePublicIngress(owner)
		}
	}
	if images != nil {
		operation := workexecution.RemoteImage
		preparations = append(preparations, images.AcceptedPreparation())
		work.Admission = append(
			work.Admission,
			workdispatch.Registration{Operation: operation, Executor: workexecution.Agent, Owner: images},
		)
		work.AgentStart = append(
			work.AgentStart,
			workdispatch.AgentStartRegistration{Operation: operation, Owner: images},
		)
		work.Retry = append(work.Retry, workdispatch.RetryRegistration{Operation: operation, Owner: images})
		work.Completion = append(
			work.Completion,
			workdispatch.CompletionRegistration{Action: workexecution.ActionFetch, Operation: operation, Owner: images},
		)
		work.Retention = append(
			work.Retention,
			operationretention.WorkRegistration{Operation: operation, Owner: images},
		)
		prior := work.AgentDelivery
		work.AgentDelivery = func(deliveries *workdispatch.AgentDeliveries) error {
			if prior != nil {
				if err := prior(deliveries); err != nil {
					return err
				}
			}
			channel.ConfigureRemoteImage(images)
			return deliveries.ConfigureRemoteImage(images)
		}
	}
	return preparations, work
}
