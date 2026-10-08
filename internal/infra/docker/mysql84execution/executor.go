// Package mysql84execution is the Docker-side port for the closed managed
// MySQL 8.4 backup protocol. It accepts only validated protocol requests and
// re-attests the exact sealed singleton before and after every Docker Exec.
package mysql84execution

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	dockerSocketPath = "/var/run/docker.sock"
	mysqlDataPath    = "/var/lib/mysql"
	streamChunkBytes = 32 * 1024
)

type Engine interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	VolumeInspect(context.Context, string, client.VolumeInspectOptions) (client.VolumeInspectResult, error)
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
	Close() error
}

type StartRecorder interface {
	RecordDumpStart(context.Context, Start) error
	RecordRestoreApplyStart(context.Context, Start) error
}

type Start struct {
	Request        mysql84protocol.Request
	Container      Container
	ExecID         string
	ImageReference string
}

type Selection struct {
	ImageReference string
	ImageID        string
	ImageOS        string
	Architecture   string
	Variant        string
	Labels         map[string]string
	VolumeName     string
	VolumeLabels   map[string]string
}

type Mount struct {
	Type        mount.Type
	Name        string
	Source      string
	Destination string
	Driver      string
	Mode        string
	RW          bool
	Propagation mount.Propagation
}

type Container struct {
	ID              string
	ImageID         string
	Name            string
	NetworkMode     string
	Runtime         string
	AppArmorProfile string
	UsernsMode      string
	CgroupnsMode    string
	Labels          map[string]string
	Mounts          []Mount
}

type Result struct {
	ExecID  string
	Stdin   mysql84protocol.StreamEvidence
	Stdout  mysql84protocol.StreamEvidence
	Stderr  mysql84protocol.StreamEvidence
	Proof   []byte
	Inspect client.ExecInspectResult
}

type RecoveryRecord struct {
	Nonce       mysql84protocol.Nonce
	Operation   mysql84protocol.Operation
	Active      bool
	HasEvidence bool
}

type Executor struct {
	engine        Engine
	selection     Selection
	startRecorder StartRecorder
	mu            sync.Mutex
	spent         map[mysql84protocol.Nonce]struct{}
}

func New(selection Selection, recorder StartRecorder) (*Executor, error) {
	engine, err := client.New(client.WithHost("unix://" + dockerSocketPath))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	executor, err := NewWithEngine(engine, selection, recorder)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	return executor, nil
}

func NewWithEngine(engine Engine, selection Selection, recorder StartRecorder) (*Executor, error) {
	if engine == nil || selection.ImageReference == "" || !workloadimage.LocalIDValid(selection.ImageID) ||
		selection.ImageOS == "" || selection.Architecture == "" || len(selection.Labels) == 0 ||
		selection.VolumeName == "" || strings.Contains(selection.VolumeName, "/") ||
		len(selection.VolumeLabels) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "managed MySQL executor selection is invalid")
	}
	return &Executor{engine: engine, selection: selection, startRecorder: recorder,
		spent: make(map[mysql84protocol.Nonce]struct{})}, nil
}

func (executor *Executor) Close() error {
	if executor == nil || executor.engine == nil {
		return nil
	}
	if err := executor.engine.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (executor *Executor) ResolveContainer(ctx context.Context) (Container, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return Container{}, invalidExecution("managed MySQL executor is unavailable")
	}
	keys := make([]string, 0, len(executor.selection.Labels))
	for key := range executor.selection.Labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	filters := client.Filters{}
	for _, key := range keys {
		filters = filters.Add("label", key+"="+executor.selection.Labels[key])
	}
	listed, err := executor.engine.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
	if err != nil || len(listed.Items) != 1 {
		return Container{}, invalidExecution("managed MySQL Service container is not unique")
	}
	candidate := listed.Items[0]
	if !validDockerID(candidate.ID) || candidate.ImageID != executor.selection.ImageID ||
		!labelsMatch(candidate.Labels, executor.selection.Labels) {
		return Container{}, invalidExecution("managed MySQL Service identity changed")
	}
	volume, err := executor.engine.VolumeInspect(ctx, executor.selection.VolumeName, client.VolumeInspectOptions{})
	if err != nil || volume.Volume.Name != executor.selection.VolumeName || volume.Volume.Driver != "local" ||
		volume.Volume.Scope != "local" || !validMountPath(volume.Volume.Mountpoint) ||
		!labelsMatch(volume.Volume.Labels, executor.selection.VolumeLabels) {
		return Container{}, invalidExecution("managed MySQL data volume changed")
	}
	inspected, err := executor.engine.ContainerInspect(ctx, candidate.ID, client.ContainerInspectOptions{})
	if err != nil {
		return Container{}, errs.Wrap(errs.KindInternal, err)
	}
	observed := inspected.Container
	if observed.ID != candidate.ID || observed.Image != candidate.ImageID || observed.HostConfig == nil ||
		observed.Config == nil || len(observed.Mounts) != 1 {
		return Container{}, invalidExecution("managed MySQL container mounts changed")
	}
	data := observed.Mounts[0]
	if data.Type != mount.TypeVolume || data.Name != executor.selection.VolumeName ||
		data.Source != volume.Volume.Mountpoint || data.Destination != mysqlDataPath || !data.RW {
		return Container{}, invalidExecution("managed MySQL container mounts changed")
	}
	expected := Container{ID: observed.ID, ImageID: observed.Image, Name: observed.Name,
		NetworkMode: string(observed.HostConfig.NetworkMode), Runtime: observed.HostConfig.Runtime,
		AppArmorProfile: observed.AppArmorProfile, UsernsMode: string(observed.HostConfig.UsernsMode),
		CgroupnsMode: string(observed.HostConfig.CgroupnsMode), Labels: executor.selection.Labels,
		Mounts: []Mount{{Type: data.Type, Name: data.Name, Source: data.Source, Destination: data.Destination,
			Driver: data.Driver, Mode: data.Mode, RW: data.RW, Propagation: data.Propagation}}}
	if err := executor.attest(ctx, expected); err != nil {
		return Container{}, err
	}
	return expected, nil
}

func (executor *Executor) attest(ctx context.Context, expected Container) error {
	if !validDockerID(expected.ID) || expected.ImageID != executor.selection.ImageID || expected.Name == "" ||
		expected.NetworkMode == "" || len(expected.Labels) == 0 || len(expected.Mounts) != 1 {
		return invalidExecution("managed MySQL container authority is invalid")
	}
	image, err := executor.engine.ImageInspect(ctx, expected.ImageID)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if image.ID != expected.ImageID || image.Os != executor.selection.ImageOS ||
		image.Architecture != executor.selection.Architecture || image.Variant != executor.selection.Variant {
		return invalidExecution("managed MySQL image identity changed")
	}
	inspected, err := executor.engine.ContainerInspect(ctx, expected.ID, client.ContainerInspectOptions{})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	observed := inspected.Container
	if observed.ID != expected.ID || observed.Name != expected.Name || observed.Image != expected.ImageID ||
		observed.Config == nil || observed.HostConfig == nil || observed.Config.Tty ||
		(observed.Config.Image != executor.selection.ImageReference && observed.Config.Image != expected.ImageID) ||
		!labelsMatch(observed.Config.Labels, expected.Labels) || observed.State == nil || !observed.State.Running ||
		observed.State.Paused || observed.State.Restarting || observed.State.Dead ||
		observed.AppArmorProfile != expected.AppArmorProfile || !validHostProfile(observed.HostConfig, expected) ||
		!sameMounts(observed.Mounts, expected.Mounts) {
		return invalidExecution("managed MySQL container identity changed")
	}
	return nil
}

func validHostProfile(host *container.HostConfig, expected Container) bool {
	return !host.Privileged && !host.ReadonlyRootfs && string(host.NetworkMode) == expected.NetworkMode &&
		host.Runtime == expected.Runtime && string(host.UsernsMode) == expected.UsernsMode &&
		string(host.CgroupnsMode) == expected.CgroupnsMode && len(host.GroupAdd) == 0 && len(host.VolumesFrom) == 0 &&
		expected.NetworkMode != "host" && !strings.HasPrefix(expected.NetworkMode, "container:") &&
		string(host.PidMode) != "host" && !strings.HasPrefix(string(host.PidMode), "container:") &&
		string(host.IpcMode) != "host" && !strings.HasPrefix(string(host.IpcMode), "container:")
}

func (executor *Executor) Execute(ctx context.Context, expected Container, request mysql84protocol.Request,
	source io.ReadCloser, artifact io.WriteCloser,
) (Result, error) {
	if executor == nil || executor.engine == nil || ctx == nil || request.Validate() != nil ||
		(request.Operation == mysql84protocol.OperationRestoreApply) != (source != nil) ||
		(request.Operation == mysql84protocol.OperationDump ||
			request.Operation == mysql84protocol.OperationRecoverDump) != (artifact != nil) ||
		request.DeadlineUnixNano > uint64(^uint64(0)>>1) {
		return Result{}, errs.New(errs.KindValidationFailed, "managed MySQL execution request is invalid")
	}
	deadline := time.Unix(0, int64(request.DeadlineUnixNano))
	if !time.Now().Before(deadline) {
		return Result{}, errs.New(errs.KindValidationFailed, "managed MySQL execution deadline elapsed")
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := executor.attest(ctx, expected); err != nil {
		return Result{}, err
	}
	command, err := request.DockerExecCommand()
	if err != nil {
		return Result{}, err
	}
	created, err := executor.engine.ExecCreate(ctx, expected.ID, client.ExecCreateOptions{User: "0:0",
		Privileged: false, TTY: false, AttachStdin: source != nil, AttachStdout: true, AttachStderr: true,
		WorkingDir: "/", Cmd: command})
	if err != nil || !validDockerID(created.ID) {
		return Result{}, invalidExecution("managed MySQL Exec creation is unavailable")
	}
	result := Result{ExecID: created.ID}
	if err := executor.attest(ctx, expected); err != nil {
		return result, err
	}
	if request.Operation == mysql84protocol.OperationDump ||
		request.Operation == mysql84protocol.OperationRestoreApply {
		if err := executor.acknowledgeStart(ctx, request, expected, created.ID); err != nil {
			return result, err
		}
	}
	return executor.attachAndInspect(ctx, expected, request, source, artifact, result)
}

func (executor *Executor) acknowledgeStart(ctx context.Context, request mysql84protocol.Request,
	expected Container, execID string,
) error {
	if executor.startRecorder == nil {
		return invalidExecution("managed MySQL start recorder is unavailable")
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if _, exists := executor.spent[request.Nonce]; exists {
		return invalidExecution("managed MySQL attempt is spent")
	}
	executor.spent[request.Nonce] = struct{}{}
	start := Start{Request: request, Container: expected, ExecID: execID,
		ImageReference: executor.selection.ImageReference}
	if request.Operation == mysql84protocol.OperationDump {
		return executor.startRecorder.RecordDumpStart(ctx, start)
	}
	return executor.startRecorder.RecordRestoreApplyStart(ctx, start)
}

func (executor *Executor) RecoverExecution(ctx context.Context, expected Container,
	original mysql84protocol.Request, execID string, artifact io.WriteCloser,
) (Result, error) {
	if !validDockerID(execID) {
		return Result{}, invalidExecution("MySQL original Exec identity is invalid")
	}
	inspected, err := executor.engine.ExecInspect(ctx, execID, client.ExecInspectOptions{})
	if err != nil || inspected.ID != execID || inspected.ContainerID != expected.ID || inspected.Running ||
		inspected.ExitCode != 0 {
		return Result{}, invalidExecution("MySQL original Exec success is unproven")
	}
	recovery := mysql84protocol.Request{Nonce: original.Nonce, DeadlineUnixNano: original.DeadlineUnixNano}
	var source io.ReadCloser
	switch original.Operation {
	case mysql84protocol.OperationDump:
		recovery.Operation, recovery.MaximumBytes = mysql84protocol.OperationRecoverDump, original.MaximumBytes
	case mysql84protocol.OperationRestoreApply:
		recovery.Operation = mysql84protocol.OperationRecoverEvidence
		artifact = nil
	default:
		return Result{}, invalidExecution("MySQL original execution cannot be recovered")
	}
	result, err := executor.Execute(ctx, expected, recovery, source, artifact)
	if err != nil {
		return result, err
	}
	if original.Operation == mysql84protocol.OperationRestoreApply &&
		!retainedEvidenceMatches(result.Proof, original.SourceSize, original.SourceSHA256) {
		return Result{}, invalidExecution("MySQL retained Restore evidence changed")
	}
	return result, nil
}

func retainedEvidenceMatches(proof []byte, size uint64, digest mysql84protocol.Digest) bool {
	lines := strings.Split(strings.TrimSpace(string(proof)), "\n")
	if len(lines) != 2 {
		return false
	}
	parsedSize, err := strconv.ParseUint(lines[0], 10, 64)
	parsedDigest, digestErr := hex.DecodeString(lines[1])
	return err == nil && digestErr == nil && parsedSize == size && len(parsedDigest) == len(digest) &&
		bytes.Equal(parsedDigest, digest[:])
}

func (executor *Executor) RetireExecution(ctx context.Context, expected Container,
	original mysql84protocol.Request,
) error {
	_, err := executor.Execute(ctx, expected, mysql84protocol.Request{Operation: mysql84protocol.OperationRetire,
		Nonce: original.Nonce, DeadlineUnixNano: original.DeadlineUnixNano}, nil, nil)
	return err
}

func (executor *Executor) InspectRecoveryInventory(ctx context.Context, expected Container) ([]RecoveryRecord, error) {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := bounded.Deadline()
	var nonce mysql84protocol.Nonce
	if _, err := rand.Read(nonce[:]); err != nil || nonce == (mysql84protocol.Nonce{}) {
		return nil, errs.New(errs.KindInternal, "MySQL inventory identity is unavailable")
	}
	result, err := executor.Execute(bounded, expected, mysql84protocol.Request{
		Operation: mysql84protocol.OperationRecoveryInventory, Nonce: nonce,
		DeadlineUnixNano: uint64(deadline.UnixNano())}, nil, nil)
	if err != nil {
		return nil, err
	}
	return parseInventory(result.Proof)
}

func (executor *Executor) RetireInventoriedExecution(ctx context.Context, expected Container,
	record RecoveryRecord,
) error {
	if record.Active {
		return invalidExecution("MySQL retained execution is not safe to retire")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := bounded.Deadline()
	_, err := executor.Execute(bounded, expected, mysql84protocol.Request{Operation: mysql84protocol.OperationRetire,
		Nonce: record.Nonce, DeadlineUnixNano: uint64(deadline.UnixNano())}, nil, nil)
	return err
}

func parseInventory(proof []byte) ([]RecoveryRecord, error) {
	lines := strings.Split(strings.TrimSpace(string(proof)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}
	result := make([]RecoveryRecord, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 4 || len(fields[0]) != 64 || fields[2] != "0" && fields[2] != "1" ||
			fields[3] != "0" && fields[3] != "1" {
			return nil, invalidExecution("MySQL recovery inventory is invalid")
		}
		decoded, err := hex.DecodeString(fields[0])
		if err != nil || len(decoded) != sha256.Size {
			return nil, invalidExecution("MySQL recovery inventory is invalid")
		}
		var nonce mysql84protocol.Nonce
		copy(nonce[:], decoded)
		var operation mysql84protocol.Operation
		switch fields[1] {
		case "dump":
			operation = mysql84protocol.OperationDump
		case "restore-apply":
			operation = mysql84protocol.OperationRestoreApply
		default:
			return nil, invalidExecution("MySQL recovery inventory is invalid")
		}
		result = append(result, RecoveryRecord{Nonce: nonce, Operation: operation,
			Active: fields[2] == "1", HasEvidence: fields[3] == "1"})
	}
	return result, nil
}

func sameMounts(observed []container.MountPoint, expected []Mount) bool {
	if len(observed) != len(expected) {
		return false
	}
	for index, item := range observed {
		claim := expected[index]
		if item.Type != claim.Type || item.Name != claim.Name || item.Source != claim.Source ||
			item.Destination != claim.Destination || item.Driver != claim.Driver || item.Mode != claim.Mode ||
			item.RW != claim.RW || item.Propagation != claim.Propagation {
			return false
		}
	}
	return true
}

func labelsMatch(observed, expected map[string]string) bool {
	if len(expected) == 0 {
		return false
	}
	for key, value := range expected {
		if observed[key] != value {
			return false
		}
	}
	return true
}

func validMountPath(value string) bool {
	return strings.HasPrefix(value, "/") && value == path.Clean(value)
}

func validDockerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, letter := range value {
		if letter < '0' || letter > '9' && (letter < 'a' || letter > 'f') {
			return false
		}
	}
	return true
}

func invalidExecution(message string) error { return errs.New(errs.KindStateConflict, message) }
