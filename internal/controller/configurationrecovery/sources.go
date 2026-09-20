// Package configurationrecovery selects a Task's exact retained file sources
// and derives its closed execution identities without resolving plaintext.
package configurationrecovery

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type snapshotReader interface {
	LoadRetained(context.Context, runtimeconfiguration.Reference) (runtimeconfiguration.Snapshot, error)
}

type Sources struct{ snapshots snapshotReader }

func NewSources(snapshots snapshotReader) (*Sources, error) {
	if snapshots == nil {
		return nil, errs.New(errs.KindInternal, "configuration recovery source repository is required")
	}
	return &Sources{snapshots: snapshots}, nil
}

type File struct {
	Original   taskmaterialization.Record
	Probe      taskmaterialization.Record
	Compensate taskmaterialization.Record
}

type Prepared struct {
	Procedure *agentpb.ConfigurationRestoration
	Files     []File
}

// Prepare uses retained immutable references, never the current configuration
// head. It is identical during initial sealing and restart reconstruction.
func (sources *Sources) Prepare(ctx context.Context, task etcd.TaskRecord) (Prepared, error) {
	if len(task.Materializations) == 0 {
		return Prepared{}, nil
	}
	if ctx == nil || sources == nil || sources.snapshots == nil || task.Configuration == nil ||
		task.CreatedAt.IsZero() || ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil || task.RenderGeneration <= 0 {
		return Prepared{}, errs.New(errs.KindStateConflict, "Task configuration recovery authority is missing")
	}
	configuration := task.Configuration
	if runtimeconfiguration.ValidateReference(configuration.Current) != nil ||
		configuration.Current.EnvironmentID != task.Owner.EnvironmentID ||
		configuration.Current.Generation > uint64(task.RenderGeneration) ||
		(configuration.Prior == nil) != (configuration.PriorRevision == 0) || configuration.PriorRevision < 0 {
		return Prepared{}, errs.New(errs.KindStateConflict, "Task configuration recovery authority is inconsistent")
	}
	procedure := &agentpb.ConfigurationRestoration{}
	var previous *runtimeconfiguration.Snapshot
	if configuration.Prior != nil {
		retained, err := sources.snapshots.LoadRetained(ctx, *configuration.Prior)
		if err != nil {
			return Prepared{}, err
		}
		previous = &retained
		procedure.PriorSnapshotId = configuration.Prior.ID
		procedure.PriorSnapshotSha256, err = hex.DecodeString(configuration.Prior.SHA256)
		if err != nil {
			return Prepared{}, errs.New(errs.KindStateConflict, "Task prior configuration digest is invalid")
		}
	}
	selected, err := runtimeconfiguration.SelectRestorations(task.Owner.EnvironmentID,
		uint64(task.RenderGeneration), previous, task.Materializations)
	if err != nil {
		return Prepared{}, err
	}
	prepared := Prepared{Procedure: procedure, Files: make([]File, 0, len(selected))}
	for _, selected := range selected {
		cloned := taskmaterialization.Clone([]taskmaterialization.Record{selected.Prior, selected.Prior})
		probe, compensate := cloned[0], cloned[1]
		probe.StepID = ids.DeriveAt(
			ids.KindStep,
			task.CreatedAt,
			task.PlanID,
			"configuration-probe/"+selected.ForwardStepID,
		)
		probe.MaterializationID = ids.DeriveAt(ids.KindConfig, task.CreatedAt, task.PlanID, probe.StepID)
		compensate.StepID = ids.DeriveAt(
			ids.KindStep,
			task.CreatedAt,
			task.PlanID,
			"configuration-restore/"+selected.ForwardStepID,
		)
		compensate.MaterializationID = ids.DeriveAt(ids.KindConfig, task.CreatedAt, task.PlanID, compensate.StepID)
		procedure.Files = append(procedure.Files, &agentpb.ConfigurationFileRestoration{
			ForwardStepId: selected.ForwardStepID, ProbeStepId: probe.StepID, CompensateStepId: compensate.StepID,
		})
		prepared.Files = append(prepared.Files, File{Original: selected.Prior, Probe: probe, Compensate: compensate})
	}
	return prepared, nil
}

// Resolve checks that the requested recovery step belongs to the Task's sealed
// procedure. The original record remains the immutable content-store key.
func (sources *Sources) Resolve(ctx context.Context, task etcd.TaskRecord, plan *agentpb.ExecutionPlan,
	stepID string) (taskmaterialization.Record, taskmaterialization.Record, error) {
	prepared, err := sources.Prepare(ctx, task)
	if err != nil {
		return taskmaterialization.Record{}, taskmaterialization.Record{}, err
	}
	configuration := plan.GetCandidateReleaseProcedure().GetConfigurationRestoration()
	if prepared.Procedure == nil || !executionplan.ConfigurationSnapshotMatches(configuration,
		prepared.Procedure.PriorSnapshotId, prepared.Procedure.PriorSnapshotSha256) {
		return taskmaterialization.Record{}, taskmaterialization.Record{}, errs.New(
			errs.KindStateConflict,
			"recovery plan configuration source differs from Task",
		)
	}
	pair := executionplan.ConfigurationFilePair(plan, stepID)
	for index, file := range prepared.Files {
		expected := prepared.Procedure.Files[index]
		if pair == nil || pair.ForwardStepId != expected.ForwardStepId || pair.ProbeStepId != expected.ProbeStepId ||
			pair.CompensateStepId != expected.CompensateStepId {
			continue
		}
		if stepID == file.Probe.StepID {
			return file.Original, file.Probe, nil
		}
		if stepID == file.Compensate.StepID {
			return file.Original, file.Compensate, nil
		}
	}
	return taskmaterialization.Record{}, taskmaterialization.Record{}, errs.New(
		errs.KindStateConflict,
		"recovery file is not selected by the Task",
	)
}
