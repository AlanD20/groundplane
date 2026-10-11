package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
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
	failedTask := ""
	if ready {
		failedTask, _, err = failedResolverConfigurationRequest(ctx, repository.store, desired.ReadRevision, ack)
		if err != nil {
			return result, err
		}
		result.State, result.TaskID = "applied", ack.observation.TaskID
	}
	bindings := selection.Bindings()
	for _, binding := range bindings {
		state, taskID := result.State, result.TaskID
		if ready {
			switch {
			case blueprintDNSBindingAcknowledged(ack.observation, binding.Record.ID, &binding):
				state, taskID = "applied", ack.observation.TaskID
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
		for _, previous := range ack.observation.EnvironmentDNS {
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

func (repository *EnvironmentBlueprintRepository) environmentDNSAcknowledgement(
	ctx context.Context,
	revision int64,
) (resolverAcknowledgement, bool, bool, string, error) {
	var empty resolverAcknowledgement
	index, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS)}, Revision: revision,
	})
	if err != nil {
		return empty, false, false, "", err
	}
	if index == nil || len(index.Values) != 1 || index.ReadRevision != revision {
		return empty, false, false, "", errs.New(errs.KindInternal, "DNS resolver index read is incomplete")
	}
	defer keyvalue.ClearValues(index.Values)
	if index.Values[0] == nil {
		return empty, false, false, "", nil
	}
	componentID := string(index.Values[0].Value)
	if ids.Validate(ids.KindComponent, componentID) != nil {
		return empty, false, false, "", errs.New(errs.KindInternal, "DNS resolver identity is corrupt")
	}
	state, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			components.RecordKey(componentID),
			platformcomponents.ActiveTaskKey(componentID),
		}, Revision: revision,
	})
	if err != nil {
		return empty, false, false, "", err
	}
	if state == nil || len(state.Values) != 2 || state.ReadRevision != revision || state.Values[0] == nil {
		return empty, false, false, "", errs.New(errs.KindInternal, "DNS resolver read is incomplete")
	}
	defer keyvalue.ClearValues(state.Values)
	component, err := components.DecodeRecord(state.Values[0].Value)
	if err != nil {
		return empty, false, false, "", err
	}
	if component.Desired.ID != componentID || component.Desired.Kind != core.ComponentKindCoreDNS {
		return empty, false, false, "", errs.New(errs.KindInternal, "DNS resolver ownership is corrupt")
	}
	if !component.Desired.Enabled {
		return empty, false, false, "", nil
	}
	if state.Values[1] != nil {
		active := string(state.Values[1].Value)
		if ids.Validate(ids.KindTask, active) != nil {
			return empty, false, true, "", errs.New(errs.KindInternal, "DNS resolver Task identity is corrupt")
		}
		return empty, false, true, active, nil
	}
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, revision)
	return ack, ready, true, "", err
}
