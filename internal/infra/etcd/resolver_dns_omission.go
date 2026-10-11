package etcd

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The caller proves the resolver's completed Task, sealed input and observation
// match the complete current DNS selection, and joins those comparisons with
// this plan. One receipt per pass bounds writes; completed or claimed records
// are skipped so they cannot starve later metadata-only omissions.
func (repository *TaskRepository) prepareResolverDNSOmission(ctx context.Context,
	task TaskRecord, observation platformcomponents.ComponentObservationRecord,
	observationRevision, revision int64,
) (blueprintunits.MutationPlan, error) {
	if task.Status != taskjournal.TaskStatusCompleted || task.ID != observation.TaskID ||
		task.Target != observation.ComponentID || !observation.Enabled || !observation.Healthy ||
		observationRevision <= 0 || revision < observationRevision {
		return blueprintunits.MutationPlan{}, errs.New(errs.KindInternal, "DNS withdrawal acknowledgement is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	start := ""
	for {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{
			Prefix:         environmentqueries.EnvironmentDesiredHeadScanPrefix,
			StartExclusive: start, Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return blueprintunits.MutationPlan{}, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return blueprintunits.MutationPlan{}, errs.New(errs.KindInternal, "DNS omission scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(strings.TrimPrefix(value.Key,
				environmentqueries.EnvironmentDesiredHeadScanPrefix), "/current")
			plan, err := repository.prepareEnvironmentDNSOmission(
				ctx,
				ledger,
				environmentID,
				task,
				observation,
				observationRevision,
				revision,
			)
			if err != nil || len(plan.Mutations()) != 0 {
				keyvalue.ClearRangeValues(page.Values)
				return plan, err
			}
		}
		keyvalue.ClearRangeValues(page.Values)
		if !page.More {
			return blueprintunits.MutationPlan{}, nil
		}
	}
}

func (repository *TaskRepository) prepareEnvironmentDNSOmission(ctx context.Context,
	ledger *blueprintunits.Repository, environmentID string, task TaskRecord,
	observation platformcomponents.ComponentObservationRecord, observationRevision, revision int64,
) (blueprintunits.MutationPlan, error) {
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, revision)
	if err != nil || !found {
		return blueprintunits.MutationPlan{}, err
	}
	if !slices.ContainsFunc(owned.Record.PendingOmissions, func(value environmentprojection.OwnedOmission) bool {
		return value.Kind == ids.KindDNSRecord
	}) {
		return blueprintunits.MutationPlan{}, nil
	}
	snapshot, err := ledger.LoadAtRevision(ctx, environmentID, revision)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	if snapshot.HeadTaskID != owned.Record.RevisionID || snapshot.HeadRevision != owned.Revision {
		return blueprintunits.MutationPlan{}, errs.New(errs.KindStateConflict, "DNS omission head changed")
	}
	if snapshot.Desired != nil && snapshot.Desired.Record.ParentTaskID == snapshot.HeadTaskID {
		return blueprintunits.MutationPlan{}, nil // The visible Apply owns its unit settlement.
	}
	for _, omission := range owned.Record.PendingOmissions {
		if omission.Kind != ids.KindDNSRecord {
			continue
		}
		target := blueprintunits.ResourceKey{Kind: omission.Kind, ID: omission.ID}
		settled := slices.ContainsFunc(
			snapshot.Applied,
			func(value keyvalue.Versioned[blueprintunits.AppliedRecord]) bool {
				return value.Record.Target == target && value.Record.State == blueprintunits.Absent &&
					(value.Record.SourceTaskID != "" || value.Record.ControllerOperationID != "")
			},
		)
		claimed := slices.ContainsFunc(
			snapshot.Executions,
			func(value keyvalue.Versioned[blueprintunits.ExecutionRecord]) bool {
				return slices.Contains(value.Record.Unit.Reads, target) ||
					slices.Contains(value.Record.Unit.Writes, target)
			},
		)
		if settled || claimed || !blueprintDNSBindingAcknowledged(observation, omission.ID, nil) {
			continue
		}
		// Claims are retired first by the shared resolver reconciliation. Do
		// not record absence while even one old name still belongs to this id.
		_, mutations, more, err := dnsrecords.NewReader(repository.store).PrepareHostnameRetirement(
			ctx, environmentID, omission.ID, "", observation.ResolvedDNSHosts, revision,
		)
		if err != nil {
			return blueprintunits.MutationPlan{}, err
		}
		if more || len(mutations) != 0 {
			continue
		}
		return blueprintunits.PrepareNativeRemovalReceipt(snapshot, blueprintunits.NativeRemoval{
			Target: target, OperationID: task.OperationID, TaskID: task.ID, PlanID: task.PlanID,
			DNS: &blueprintunits.DNSReceipt{ComponentID: observation.ComponentID, TaskID: task.ID,
				ObservationRevision: observationRevision, CorefileSHA256: observation.CorefileSHA256},
		})
	}
	return blueprintunits.MutationPlan{}, nil
}
