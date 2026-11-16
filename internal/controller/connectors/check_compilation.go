package connectors

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedConnectorCheck struct {
	Request   apiTypes.ConnectorCheckRequest `json:"request"`
	SecretIDs map[string]string              `json:"secret_ids"`
}

func (service *CheckService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"connector-check",
			workexecution.ActionCheck,
			ids.KindConnector,
			service.prepareAcceptedCheck,
		),
	}
}

func (service *CheckService) checkSecretIDs(
	ctx context.Context,
	projectID string,
	candidate connectorrecord.Record,
) (map[string]string, []string, error) {
	result := make(map[string]string)
	var retained []string
	for _, credential := range candidate.Connector.Credentials {
		if credential.Kind != core.ConnectorCredentialSecretRef {
			continue
		}
		secret, err := service.secrets.ResolveSecret(ctx, projectID, credential.SecretRef)
		if err != nil {
			return nil, nil, err
		}
		if secret.Record.Secret.Kind != core.SecretKindEnvVar {
			return nil, nil, errs.New(errs.KindValidationFailed, "Connector credentials require variable Secrets")
		}
		result[credential.SecretRef] = secret.Record.Secret.ID
		retained = append(retained, secret.Record.Secret.ID)
	}
	sort.Strings(retained)
	return result, slices.Compact(retained), nil
}

func (service *CheckService) prepareAcceptedCheck(ctx context.Context, task workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedConnectorCheck,
) (taskdependencies.Prepared, error) {
	environment, err := service.hierarchy.GetEnvironment(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidate, replacements, captureRevision, err := service.checkCandidate(
		ctx,
		environment.Record.ID,
		task.Task.TargetID,
		input.Request,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clearConnectorDirectValues(replacements)
	source, err := service.connectors.LoadConnectorCheckSource(ctx, environment, project, candidate, captureRevision,
		input.Request.Draft != nil || input.Request.Changes != nil)
	if err != nil {
		if input.Request.Revision == 0 && input.Request.Draft == nil &&
			errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			current, readErr := service.connectors.GetConnector(ctx, task.Task.TargetID)
			if readErr == nil && current.Revision != captureRevision {
				return taskdependencies.Prepared{Pending: &taskdependencies.Pending{Reason: workexecution.WaitConflict,
					Detail: "The Connector changed while its inputs were being captured; GP will capture the current definition again."}}, nil
			}
		}
		return taskdependencies.Prepared{}, err
	}
	defer source.Clear()
	owner := connectorchecks.Owner{TaskID: task.Task.ID, EnvironmentID: task.Task.Scope.EnvironmentID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, task.Task.CreatedAt, task.Task.ID, "connector-check"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, task.Task.ID, "connector-check"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.Task.CreatedAt, task.Task.ID, "connector-check")}
	credentials, sources, err := service.captureCredentials(ctx, source, owner.OperationID, replacements)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(credentials.Ciphertext)
	for _, reference := range sources {
		if (input.Request.Draft != nil || input.Request.Changes != nil || input.Request.Revision != 0) &&
			input.SecretIDs[reference.Reference] != reference.Pin.SecretID {
			return taskdependencies.Prepared{}, errs.New(
				errs.KindStateConflict,
				"A Connector credential no longer refers to the accepted Secret",
			)
		}
	}
	record, err := connectorchecks.NewRecord(
		owner,
		source.Connector(),
		source.Revision(),
		source.Draft(),
		credentials,
		sources,
		at,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer record.Clear()
	hash, err := connectorchecks.PlanHash(record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared := workexecution.CloneTask(task.Task)
	prepared.Intent, prepared.ExpectedItems, prepared.UpdatedAt = workexecution.Input{
		ID:     owner.PlanID,
		SHA256: hash,
	}, 1, at
	item, authority, err := service.connectors.PrepareConnectorCheck(ctx, source, record, prepared)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	item.Deadline = acceptedaction.ExecutionDeadline(prepared, at, time.Minute)
	return taskdependencies.Prepared{Task: prepared, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
