package network

import (
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CascadeDetachPlan is prepared from an Attach's captured source and exact hook
// inputs. Every input is sealed before the graph can be published.
type CascadeDetachPlan struct {
	AttachID  string
	Placement workexecution.Placement
	Input     *workexecution.Input
}

type BackingZoneWorkGraph struct {
	Task             workexecution.Task
	Items            []workexecution.Item
	Sources          []CascadeAttachSource
	SnapshotRevision int64
	ImpactSHA256     string
}

type CascadeAttachSource struct {
	AttachID string
	Revision int64
}

// BuildBackingZoneWorkGraph replaces polling child Tasks with native dependency
// work. Detaches sharing one Backing Service remain sequential, as before.
// This keeps credential deletion and custom hooks ordered and bounds fan-in
// without introducing synthetic barrier work.
func BuildBackingZoneWorkGraph(task workexecution.Task, bridge workexecution.Item,
	frozenRevision int64, impactSHA256 string, captured []keyvalue.Versioned[attachments.Record],
	plans []CascadeDetachPlan,
) (BackingZoneWorkGraph, error) {
	if task.Validate() != nil || bridge.Validate() != nil || frozenRevision <= 0 ||
		task.Action != workexecution.ActionRemove || task.PublicationComplete || task.ExpectedItems != 1 ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		ids.Validate(ids.KindNetwork, task.TargetID) != nil ||
		task.Scope.TenantID != "" || task.Scope.ProjectID == "" || task.Scope.EnvironmentID == "" ||
		task.TargetID != bridge.TargetID || bridge.TaskID != task.ID || bridge.Operation != workexecution.Network ||
		bridge.Input == nil || bridge.Placement.Executor != workexecution.Agent ||
		bridge.Phase != workexecution.Ready || len(bridge.Dependencies) != 0 || bridge.AttemptNumber != 0 ||
		bridge.ForwardRevoked || bridge.ActiveAttemptID != "" || bridge.Result != nil ||
		bridge.Cleanup != workexecution.CleanupNone ||
		len(captured) != len(plans) || len(captured) >= int(workexecution.MaximumTaskItems) {
		return BackingZoneWorkGraph{}, errs.New(errs.KindValidationFailed, "Backing Zone removal requires one captured bridge and its consumer work")
	}
	impact, err := hex.DecodeString(impactSHA256)
	if err != nil || len(impact) != 32 || strings.ToLower(impactSHA256) != impactSHA256 {
		return BackingZoneWorkGraph{}, errs.New(errs.KindValidationFailed, "Backing Zone removal requires its exact impact digest")
	}
	byAttach := make(map[string]CascadeDetachPlan, len(plans))
	for _, plan := range plans {
		if ids.Validate(ids.KindAttach, plan.AttachID) != nil || plan.Placement.Validate() != nil ||
			(plan.Placement.Executor != workexecution.Agent && plan.Placement.Executor != workexecution.Controller) ||
			plan.Placement.HostID != bridge.Placement.HostID ||
			plan.Input == nil || plan.Input.Validate() != nil {
			return BackingZoneWorkGraph{}, errs.New(errs.KindValidationFailed, "Backing Zone Detach requires captured input and the Backing host")
		}
		if _, duplicate := byAttach[plan.AttachID]; duplicate {
			return BackingZoneWorkGraph{}, errs.New(errs.KindValidationFailed, "Backing Zone Detach plan is duplicated")
		}
		byAttach[plan.AttachID] = plan
	}
	for _, source := range captured {
		attach := source.Record
		if attachments.ValidateAttachRecord(attach) != nil || source.Revision <= 0 ||
			source.ReadRevision != frozenRevision || source.Revision > frozenRevision ||
			attach.BackingProjectID != task.Scope.ProjectID ||
			attach.BackingEnvironmentID != task.Scope.EnvironmentID || attach.BackingNetworkID != task.TargetID {
			return BackingZoneWorkGraph{}, errs.New(errs.KindStateConflict, "Backing Zone consumer differs from its frozen removal snapshot")
		}
		if _, found := byAttach[attach.ID]; !found {
			return BackingZoneWorkGraph{}, errs.New(errs.KindValidationFailed, "Backing Zone consumer has no captured Detach plan")
		}
	}
	ordered, err := orderBackingZoneCascadeAttaches(captured)
	if err != nil {
		return BackingZoneWorkGraph{}, err
	}
	task.Deadline = task.CreatedAt.Add(time.Duration(backingZoneCascadeTimeoutSeconds) * time.Second)
	task.ExpectedItems = uint32(len(ordered) + 1)
	items := make([]workexecution.Item, 0, len(ordered)+1)
	sources := make([]CascadeAttachSource, 0, len(ordered))
	previous := ""
	for _, source := range ordered {
		attach, plan := source.Record, byAttach[source.Record.ID]
		sources = append(sources, CascadeAttachSource{AttachID: attach.ID, Revision: source.Revision})
		item := workexecution.Item{ID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "backing-zone-detach/"+attach.ID),
			TaskID: task.ID, Operation: workexecution.Detach, TargetID: attach.ID,
			Placement: plan.Placement, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
			CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
			Claims: attachremoval.RemovalClaims(attach, plan.Placement.HostID)}
		input := *plan.Input
		item.Input = &input
		if previous != "" {
			item.Dependencies = []string{previous}
		}
		previous = item.ID
		items = append(items, item)
	}
	bridge.Dependencies = nil
	bridge.Claims = slices.Clone(bridge.Claims)
	input := *bridge.Input
	bridge.Input = &input
	bridge.Deadline = task.Deadline
	if previous != "" {
		bridge.Dependencies = []string{previous}
	}
	items = append(items, bridge)
	if err := workexecution.ValidateGraph(task, items); err != nil {
		return BackingZoneWorkGraph{}, err
	}
	return BackingZoneWorkGraph{Task: task, Items: items, Sources: sources, SnapshotRevision: frozenRevision, ImpactSHA256: impactSHA256}, nil
}
