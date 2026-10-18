package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetEnvironmentDNS reads desired targets and their resolver acknowledgement at
// one MVCC revision. Saved metadata is never presented as an applied DNS answer.
func (repository *EnvironmentBlueprintRepository) GetEnvironmentDNS(
	ctx context.Context,
	environmentID string,
) (dnsrecords.EnvironmentStatus, error) {
	var result dnsrecords.EnvironmentStatus
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return result, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return result, errs.New(errs.KindValidationFailed, "DNS requires a stable Environment id")
	}
	desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, 0)
	if err != nil {
		return result, err
	}
	selection, err := dnsrecords.NewReader(repository.store).
		SelectAuthoredEnvironmentRecords(ctx, environmentID, desired.ReadRevision)
	if err != nil {
		return result, err
	}
	result.HeadRevision = desired.Revision
	if found {
		result.RevisionID = desired.Record.RevisionID
	}
	ack, ready, enabled, activeTask, err := repository.environmentDNSAcknowledgement(ctx, desired.ReadRevision)
	if err != nil {
		return result, err
	}
	result.ResolverEnabled, result.TaskID, result.State = enabled, activeTask, "pending"
	if !enabled {
		result.State = "resolver_disabled"
	}
	failedTask, _, err := failedResolverConfigurationRequest(ctx, repository.store, desired.ReadRevision, ack)
	if err != nil {
		return result, err
	}
	if ready {
		result.State, result.TaskID = "applied", ""
	} else if enabled && failedTask != "" {
		result.State, result.TaskID = "failed", failedTask
	}
	bindings := selection.Bindings()
	for _, binding := range bindings {
		state, taskID := result.State, result.TaskID
		if ready {
			switch {
			case blueprintDNSBindingAcknowledged(*ack.state.Applied, binding.Record.ID, &binding):
				state, taskID = "applied", ""
				if binding.Address == "" {
					state = "waiting_for_service"
				}
			case failedTask != "":
				state, taskID = "failed", failedTask
			default:
				state, taskID = "pending", ""
			}
			if state == "pending" || state == "failed" {
				result.State, result.TaskID = state, taskID
			}
		}
		result.Records = append(result.Records, dnsrecords.EnvironmentRecordStatus{
			Binding: binding, Spec: desired.Record.Input.DNS[binding.Record.Label], State: state, TaskID: taskID,
		})
	}
	if ready {
		for _, previous := range ack.state.Applied.EnvironmentDNS {
			if previous.Binding.Record.EnvironmentID != environmentID {
				continue
			}
			retained := false
			for _, current := range bindings {
				if current.Record.ID == previous.Binding.Record.ID &&
					current.Record.Hostname() == previous.Binding.Record.Hostname() {
					retained = true
					break
				}
			}
			if !retained {
				result.Withdrawals = append(result.Withdrawals, dnsrecords.EnvironmentWithdrawal{
					RecordID: previous.Binding.Record.ID, Hostname: previous.Binding.Record.Hostname(),
				})
			}
		}
		if len(result.Withdrawals) != 0 {
			result.State, result.TaskID = "pending", ""
			if failedTask != "" {
				result.State, result.TaskID = "failed", failedTask
			}
		}
	}
	return result, nil
}

func (repository *EnvironmentBlueprintRepository) environmentDNSAcknowledgement(ctx context.Context, revision int64) (resolverAcknowledgement, bool, bool, string, error) {
	// Reads can display the last applied answers while a replacement is pending;
	// execution still uses the strict serving/withdrawal purposes.
	ack, ready, err := readResolverStateAcknowledgement(ctx, repository.store, revision, resolverWithdrawalFinalization)
	if err != nil {
		return ack, false, false, "", err
	}
	enabled := ack.component.Desired.Enabled
	serving := ready && ack.state.Applied != nil && ack.state.Applied.Desired.Enabled
	return ack, serving, enabled, ack.pendingTaskID, nil
}
