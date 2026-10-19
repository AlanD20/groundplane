package backingprovision

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Stage string

const (
	PrepareDirectories Stage = "prepare_directories"
	WriteConfiguration Stage = "write_configuration"
	ReconcileRuntime   Stage = "reconcile_runtime"
	WaitReady          Stage = "wait_ready"
	AfterStart         Stage = "after_start"
	VerifyRuntime      Stage = "verify_runtime"
	Complete           Stage = "complete"
)

// Layout is the compact journal binding. It carries no Compose, file content or
// hook values and cannot itself authorize an effect without the full input.
type Layout struct {
	Input         workexecution.Input `json:"input"`
	ServiceID     string              `json:"service_id"`
	ManagedVolume bool                `json:"managed_volume"`
	WriteFiles    bool                `json:"write_files"`
	Healthcheck   bool                `json:"healthcheck"`
	Hook          bool                `json:"hook"`
}

func CaptureLayout(attempt workexecution.Attempt, input *agentpb.BackingProvisionInput) (Layout, error) {
	reference, err := Reference(attempt.Input.ID, input)
	if err != nil {
		return Layout{}, err
	}
	if attempt.Validate() != nil || reference != attempt.Input || input.TaskId != attempt.TaskID ||
		input.WorkItemId != attempt.WorkItemID || input.HostId != attempt.Executor.HostID {
		return Layout{}, invalid("backing creation progress has a different captured assignment")
	}
	layout := Layout{Input: reference, ServiceID: input.ServiceId, ManagedVolume: len(input.ManagedVolumeIds) != 0,
		WriteFiles: len(
			input.Files,
		) != 0, Healthcheck: input.Runtime.Services[0].HasHealthcheck, Hook: input.AfterStart != nil}
	return layout, layout.Validate(attempt)
}

func (layout Layout) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || layout.Input != attempt.Input ||
		ids.Validate(ids.KindService, layout.ServiceID) != nil {
		return invalid("backing creation journal layout differs from its assignment")
	}
	return nil
}

func (layout Layout) stages() []Stage {
	result := []Stage{PrepareDirectories}
	if layout.WriteFiles {
		result = append(result, WriteConfiguration)
	}
	result = append(result, ReconcileRuntime, WaitReady)
	if layout.Hook {
		result = append(result, AfterStart)
	}
	return append(result, VerifyRuntime)
}

// Observation is bounded effect evidence from the ownership-checking helper.
// The Controller also joins file/hook receipts before accepting their stages.
type Observation struct {
	ServiceID         string              `json:"service_id"`
	Input             workexecution.Input `json:"input"`
	Stage             Stage               `json:"stage"`
	DirectoryReady    bool                `json:"directory_ready"`
	VolumeReady       bool                `json:"volume_ready"`
	FilesWritten      bool                `json:"files_written"`
	RuntimeReconciled bool                `json:"runtime_reconciled"`
	RuntimeReady      bool                `json:"runtime_ready"`
	Healthy           bool                `json:"healthy"`
	HookResult        workexecution.Input `json:"hook_result"`
	RuntimeVerified   bool                `json:"runtime_verified"`
	ContainerID       string              `json:"container_id,omitempty"`
	At                time.Time           `json:"at"`
}

func (observation Observation) Satisfies(stage Stage, layout Layout) bool {
	if observation.Stage != stage || observation.Input != layout.Input || observation.ServiceID != layout.ServiceID {
		return false
	}
	expected := Observation{ServiceID: layout.ServiceID, Input: layout.Input, Stage: stage, At: observation.At}
	switch stage {
	case PrepareDirectories:
		expected.DirectoryReady, expected.VolumeReady = true, layout.ManagedVolume
	case WriteConfiguration:
		if !layout.WriteFiles {
			return false
		}
		expected.FilesWritten = true
	case ReconcileRuntime:
		expected.RuntimeReconciled, expected.ContainerID = true, observation.ContainerID
	case WaitReady:
		expected.RuntimeReady, expected.Healthy, expected.ContainerID = true, layout.Healthcheck, observation.ContainerID
	case AfterStart:
		if !layout.Hook || observation.HookResult.Validate() != nil {
			return false
		}
		expected.HookResult, expected.ContainerID = observation.HookResult, observation.ContainerID
	case VerifyRuntime:
		expected.RuntimeVerified, expected.RuntimeReady, expected.Healthy, expected.ContainerID = true, true, layout.Healthcheck, observation.ContainerID
	default:
		return false
	}
	if stage == ReconcileRuntime || stage == WaitReady || stage == AfterStart || stage == VerifyRuntime {
		if len(observation.ContainerID) != 64 {
			return false
		}
		decoded, err := hex.DecodeString(observation.ContainerID)
		if err != nil || hex.EncodeToString(decoded) != observation.ContainerID {
			return false
		}
	}
	return expected == observation
}

type Progress struct {
	AttemptID string       `json:"attempt_id"`
	Sequence  uint64       `json:"sequence"`
	Stage     Stage        `json:"stage"`
	InFlight  bool         `json:"in_flight"`
	BeganAt   *time.Time   `json:"began_at,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
	Last      *Observation `json:"last,omitempty"`
}

func NewProgress(attempt workexecution.Attempt, layout Layout, at time.Time) (Progress, error) {
	progress := Progress{AttemptID: attempt.ID, Stage: PrepareDirectories, UpdatedAt: at.UTC()}
	if !at.Before(attempt.Deadline) {
		return Progress{}, invalid("backing creation deadline expired before execution")
	}
	return progress, progress.Validate(attempt, layout)
}

func (progress Progress) Validate(attempt workexecution.Attempt, layout Layout) error {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		progress.AttemptID != attempt.ID || progress.UpdatedAt.IsZero() || progress.UpdatedAt.Location() != time.UTC ||
		progress.UpdatedAt.Before(*attempt.StartedAt) || progress.InFlight != (progress.Sequence%2 == 1) ||
		progress.InFlight != (progress.BeganAt != nil) {
		return invalid("backing creation checkpoint envelope is invalid")
	}
	stages := layout.stages()
	if progress.Sequence > uint64(len(stages)*2) {
		return invalid("backing creation checkpoint exceeds its captured procedure")
	}
	index, expected := progress.Sequence/2, Complete
	if index < uint64(len(stages)) {
		expected = stages[index]
	}
	if progress.Stage != expected || (progress.Last != nil) != (index != 0) {
		return invalid("backing creation checkpoint has an inconsistent execution stage")
	}
	if began := progress.BeganAt; began != nil && (began.IsZero() || began.Location() != time.UTC ||
		began.Before(*attempt.StartedAt) || !began.Before(attempt.Deadline) || began.After(progress.UpdatedAt)) {
		return invalid("backing creation active stage has invalid timing")
	}
	if last := progress.Last; last != nil && (last.At.IsZero() || last.At.Location() != time.UTC ||
		last.At.Before(*attempt.StartedAt) || last.At.After(progress.UpdatedAt) || !last.Satisfies(stages[index-1], layout)) {
		return invalid("backing creation checkpoint has no matching completed-stage evidence")
	}
	return nil
}

func (progress Progress) Begin(attempt workexecution.Attempt, layout Layout, at time.Time) (Progress, error) {
	if progress.Validate(attempt, layout) != nil || progress.InFlight || progress.Stage == Complete ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalid("backing creation cannot begin this stage")
	}
	next, began := progress, at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, layout)
}

func (progress Progress) Confirm(
	attempt workexecution.Attempt,
	layout Layout,
	observation Observation,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, layout) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		!at.Before(attempt.Deadline) || observation.At.Before(*progress.BeganAt) || observation.At.After(at) ||
		!observation.Satisfies(progress.Stage, layout) || progress.Last != nil && progress.Last.ContainerID != "" &&
		observation.ContainerID != progress.Last.ContainerID {
		return Progress{}, invalid("backing creation observation does not confirm its active stage")
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt, next.Last = false, nil, at.UTC(), &observation
	next.Stage = Complete
	if stages := layout.stages(); next.Sequence/2 < uint64(len(stages)) {
		next.Stage = stages[next.Sequence/2]
	}
	return next, next.Validate(attempt, layout)
}

func StageID(workID string, stage Stage) (string, error) {
	at, err := ids.Timestamp(ids.KindWorkItem, workID)
	if err != nil {
		return "", err
	}
	switch stage {
	case PrepareDirectories, WriteConfiguration, ReconcileRuntime, WaitReady, AfterStart, VerifyRuntime:
		return ids.DeriveAt(ids.KindStep, at, workID, "backing-creation/"+string(stage)), nil
	default:
		return "", invalid("backing creation has no executable stage with this name")
	}
}
