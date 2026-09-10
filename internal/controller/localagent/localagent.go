package localagent

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const initialGeneration uint64 = 1

// Dependencies are the complete internal seams required by the lifecycle module.
type Dependencies struct {
	Repository Repository
	Runtime    Runtime
	Container  Container
	Sessions   Sessions
	Tasks      Tasks
	Clock      Clock
}

// Manager serializes lifecycle mutations and hides crash-resumption ordering from
// callers. Health remains a lock-free projection of durable and session state.
type Manager struct {
	repository Repository
	runtime    Runtime
	container  Container
	sessions   Sessions
	tasks      Tasks
	clock      Clock
	gate       chan struct{}
	pending    *pendingReplacement
}

// EnrollRequest contains only decisions made by the calling task. The caller owns
// stable ID generation and image selection; this module never invents either.
type EnrollRequest struct {
	AgentID          string
	EnrollmentTaskID string
	Image            string
	Config           Config
}

// UpdateRequest pins both sides of one restart-resumable Agent replacement.
// The Controller Task persists these values before invoking the lifecycle.
type UpdateRequest struct {
	AgentID            string
	PreviousImage      string
	DesiredImage       string
	StartingGeneration uint64
}

// Agent is the non-secret durable lifecycle projection returned to callers.
type Agent struct {
	ID               string
	EnrollmentTaskID string
	Image            string
	Generation       uint64
	Phase            Phase
	Config           Config
	CreatedAt        time.Time
	ReadyAt          time.Time
}

// Health combines durable intent with the latest matching authenticated session.
type Health struct {
	Agent      Agent
	Online     bool
	Healthy    bool
	LastReady  time.Time
	StaleAfter time.Time
	Capacity   int32
	Version    string
}

// New validates and constructs the local Agent lifecycle module.
func New(dependencies Dependencies) (*Manager, error) {
	if dependencies.Repository == nil {
		return nil, errs.New(errs.KindInternal, "local agent repository is required")
	}
	if dependencies.Runtime == nil {
		return nil, errs.New(errs.KindInternal, "local agent runtime is required")
	}
	if dependencies.Container == nil {
		return nil, errs.New(errs.KindInternal, "local agent container lifecycle is required")
	}
	if dependencies.Sessions == nil {
		return nil, errs.New(errs.KindInternal, "local agent session registry is required")
	}
	if dependencies.Tasks == nil {
		return nil, errs.New(errs.KindInternal, "local agent task aborter is required")
	}
	if dependencies.Clock == nil {
		return nil, errs.New(errs.KindInternal, "local agent clock is required")
	}
	return &Manager{
		repository: dependencies.Repository,
		runtime:    dependencies.Runtime,
		container:  dependencies.Container,
		sessions:   dependencies.Sessions,
		tasks:      dependencies.Tasks,
		clock:      dependencies.Clock,
		gate:       make(chan struct{}, 1),
	}, nil
}

// Enroll atomically creates the singleton durable record, materializes its
// runtime, converges the container, and waits for authenticated readiness.
func (manager *Manager) Enroll(ctx context.Context, request EnrollRequest) (Agent, error) {
	if err := manager.enter(ctx); err != nil {
		return Agent{}, err
	}
	defer manager.leave()

	if err := validateEnrollRequest(request); err != nil {
		return Agent{}, err
	}
	createdAt := manager.clock.Now()
	if !isNonzeroUTC(createdAt) {
		return Agent{}, errs.New(errs.KindInternal, "local agent clock returned a non-UTC time")
	}
	credential, err := manager.runtime.GenerateCredential(ctx, request.AgentID)
	if err != nil {
		return Agent{}, safePortError(ctx, err, "local agent credential generation failed")
	}
	if err := validateCredential(credential, false); err != nil {
		return Agent{}, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("local agent runtime returned an invalid credential: %w", err),
		)
	}
	record := Record{
		ID:               request.AgentID,
		EnrollmentTaskID: request.EnrollmentTaskID,
		Image:            request.Image,
		Generation:       initialGeneration,
		Phase:            PhaseProvisioning,
		Config:           cloneConfig(request.Config),
		Credential:       cloneCredential(credential),
		CreatedAt:        createdAt,
	}
	stored, err := manager.repository.CreateSingleton(ctx, cloneRecord(record))
	if err != nil {
		return Agent{}, safePortError(ctx, err, "local agent durable creation failed")
	}
	if err := validateStored(stored); err != nil {
		return Agent{}, err
	}
	ready, err := manager.provision(ctx, stored)
	if err != nil {
		return Agent{}, err
	}
	return projectAgent(ready.Record), nil
}

// UpdateConfig replaces the complete Controller-owned runtime config. The
// repository atomically commits desired state and exact replay evidence before
// host materialization. A replay never requires the target Agent to still exist.
func (manager *Manager) UpdateConfig(
	ctx context.Context,
	agentID string,
	config Config,
	idempotencyKey string,
) ([]byte, error) {
	if err := manager.enter(ctx); err != nil {
		return nil, err
	}
	defer manager.leave()
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "agent runtime config limits must be positive")
	}
	if !validLabels(config.Labels) {
		return nil, errs.New(errs.KindValidationFailed, "agent labels must contain valid NUL-free UTF-8")
	}
	result, err := manager.repository.UpdateConfig(
		ctx,
		agentID,
		cloneConfig(config),
		idempotencyKey,
	)
	if err != nil {
		return nil, safePortError(ctx, err, "local agent durable config replacement failed")
	}
	if len(result.ResponseBody) == 0 {
		return nil, errs.New(errs.KindInternal, "local agent config replacement response is empty")
	}
	if !result.Applied {
		return append([]byte(nil), result.ResponseBody...), nil
	}
	updated := result.Stored
	if err := validateStored(updated); err != nil {
		return nil, err
	}
	if updated.Record.ID != agentID || updated.Record.Phase == PhaseDeleting ||
		!equalConfig(updated.Record.Config, config) {
		return nil, errs.New(
			errs.KindInternal,
			"local agent repository replaced config on the wrong lifecycle record",
		)
	}
	if err := manager.runtime.Materialize(ctx, RuntimeMaterial{
		AgentID:        updated.Record.ID,
		Generation:     updated.Record.Generation,
		Config:         cloneConfig(updated.Record.Config),
		EncryptedToken: append([]byte(nil), updated.Record.Credential.EncryptedToken...),
	}); err != nil {
		return nil, safePortError(ctx, err, "local agent runtime materialization failed")
	}
	return append([]byte(nil), result.ResponseBody...), nil
}

func equalConfig(left Config, right Config) bool {
	if left.PullIntervalSeconds != right.PullIntervalSeconds ||
		left.MaxConcurrentTasks != right.MaxConcurrentTasks || len(left.Labels) != len(right.Labels) {
		return false
	}
	for key, value := range left.Labels {
		if right.Labels[key] != value {
			return false
		}
	}
	return true
}

// ListHealth returns the singleton durable Agent with matching live-session
// telemetry, or an empty list when no Agent has been enrolled.
func (manager *Manager) ListHealth(ctx context.Context) ([]Health, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "local agent context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if errors.Is(err, errs.New(errs.KindAgentNotFound, "")) {
		return []Health{}, nil
	}
	if err != nil {
		return nil, safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return nil, err
	}
	return []Health{manager.projectHealth(stored.Record)}, nil
}

// Health projects online state from a matching generation's authenticated Ready
// heartbeat. Transport keepalive and a mismatched generation never count.
func (manager *Manager) Health(ctx context.Context, agentID string) (Health, error) {
	if ctx == nil {
		return Health{}, errs.New(errs.KindInternal, "local agent context is required")
	}
	if err := ctx.Err(); err != nil {
		return Health{}, err
	}
	if err := validateAgentID(agentID); err != nil {
		return Health{}, err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return Health{}, safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return Health{}, err
	}
	if stored.Record.ID != agentID {
		return Health{}, agentNotFound(agentID)
	}

	return manager.projectHealth(stored.Record), nil
}

func (manager *Manager) projectHealth(record Record) Health {
	health := Health{Agent: projectAgent(record)}
	snapshot, ok := manager.sessions.Snapshot(record.ID)
	if !ok || snapshot.Generation != record.Generation {
		return health
	}
	health.LastReady = snapshot.LastReady
	health.Capacity = snapshot.Capacity
	health.Version = snapshot.Version
	if !snapshot.LastReady.IsZero() {
		health.StaleAfter = snapshot.LastReady.Add(StaleWindow(record.Config.PullIntervalSeconds))
	}
	health.Online = record.Phase != PhaseDeleting && snapshot.Online && !snapshot.Revoked &&
		!snapshot.LastReady.IsZero() && !manager.clock.Now().After(health.StaleAfter)
	health.Healthy = record.Phase == PhaseReady && health.Online
	return health
}

// Remove fences work and credentials before deleting runtime state. The durable
// record is always deleted last, making every preceding failure resumable.
func (manager *Manager) Remove(ctx context.Context, agentID string) error {
	if err := manager.enter(ctx); err != nil {
		return err
	}
	defer manager.leave()

	if err := validateAgentID(agentID); err != nil {
		return err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if isAgentNotFound(err) {
		return nil
	}
	if err != nil {
		return safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return err
	}
	if stored.Record.ID != agentID {
		return agentNotFound(agentID)
	}
	return manager.resumeDelete(ctx, stored)
}

func (manager *Manager) provision(ctx context.Context, stored StoredRecord) (StoredRecord, error) {
	if stored.Record.Phase != PhaseProvisioning {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "local agent is not provisioning")
	}
	if err := manager.convergeRuntime(ctx, stored.Record); err != nil {
		return StoredRecord{}, err
	}
	readyContext, cancelReady := context.WithCancel(ctx)
	defer cancelReady()
	ready, err := manager.sessions.Ready(readyContext, stored.Record.ID, stored.Record.Generation)
	if err != nil {
		return StoredRecord{}, safePortError(ctx, err, "local agent readiness subscription failed")
	}
	if ready == nil {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent readiness subscription is nil")
	}
	timer := manager.clock.NewTimer(ReadyTimeout)
	if timer == nil {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent readiness timer is nil")
	}
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return StoredRecord{}, ctx.Err()
	case <-timer.C():
		return StoredRecord{}, errs.New(
			errs.KindTaskTimedOut,
			"agent did not report authenticated Ready within 120 seconds",
		)
	case <-ready:
	}
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	updated, err := manager.repository.MarkReady(
		ctx,
		stored.Record.ID,
		stored.Record.Generation,
		stored.Revision,
		manager.clock.Now(),
	)
	if err != nil {
		return StoredRecord{}, safePortError(ctx, err, "local agent ready transition failed")
	}
	if err := validateStored(updated); err != nil {
		return StoredRecord{}, err
	}
	if updated.Record.Phase != PhaseReady {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent repository did not mark the record ready")
	}
	return updated, nil
}

func (manager *Manager) convergeRuntime(ctx context.Context, record Record) error {
	material := RuntimeMaterial{
		AgentID:        record.ID,
		Generation:     record.Generation,
		Config:         cloneConfig(record.Config),
		EncryptedToken: append([]byte(nil), record.Credential.EncryptedToken...),
	}
	if err := manager.runtime.Materialize(ctx, material); err != nil {
		return safePortError(ctx, err, "local agent runtime materialization failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	desired := ContainerDesired{
		AgentID:    record.ID,
		Image:      record.Image,
		Generation: record.Generation,
	}
	if err := manager.container.Converge(ctx, desired); err != nil {
		return safePortError(ctx, err, "local agent container convergence failed")
	}
	return ctx.Err()
}

func (manager *Manager) resumeDelete(ctx context.Context, stored StoredRecord) error {
	record := stored.Record
	if err := manager.sessions.StopAssignments(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent assignment fencing failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.tasks.AbortActive(
		ctx,
		record.ID,
		record.Generation,
		record.Config.MaxConcurrentTasks,
		RemovedTaskReason,
	); err != nil {
		return safePortError(ctx, err, "local agent active task abortion failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if record.Phase != PhaseDeleting {
		deleting, err := manager.repository.BeginDelete(ctx, record.ID, record.Generation, stored.Revision)
		if err != nil {
			return safePortError(ctx, err, "local agent durable revocation failed")
		}
		if err := validateStored(deleting); err != nil {
			return err
		}
		if deleting.Record.Phase != PhaseDeleting || deleting.Record.Credential.Digest != "" {
			return errs.New(errs.KindInternal, "local agent repository did not durably revoke the credential")
		}
		stored = deleting
		record = deleting.Record
	}

	if err := manager.sessions.Revoke(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent session revocation failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.sessions.WaitOffline(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent offline wait failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.container.Remove(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent container removal failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.runtime.Remove(ctx, record.ID); err != nil {
		return safePortError(ctx, err, "local agent runtime removal failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.repository.Delete(ctx, record.ID, record.Generation, stored.Revision); err != nil {
		if isAgentNotFound(err) {
			return nil
		}
		return safePortError(ctx, err, "local agent durable deletion failed")
	}
	return nil
}

func (manager *Manager) enter(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "local agent context is required")
	}
	select {
	case manager.gate <- struct{}{}:
		if err := manager.recoverPending(ctx); err != nil {
			manager.leave()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (manager *Manager) leave() {
	<-manager.gate
}

func validateEnrollRequest(request EnrollRequest) error {
	if err := validateAgentID(request.AgentID); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindTask, request.EnrollmentTaskID); err != nil {
		return errs.New(errs.KindValidationFailed, "agent enrollment Task id is invalid")
	}
	if !imageref.IsDigestPinned(request.Image) {
		return errs.New(errs.KindValidationFailed, "agent image must be a non-empty digest-pinned reference")
	}
	if request.Config.PullIntervalSeconds <= 0 {
		return errs.New(errs.KindValidationFailed, "agent pull interval must be positive")
	}
	if request.Config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindValidationFailed, "agent maximum concurrent tasks must be positive")
	}
	if !validLabels(request.Config.Labels) {
		return errs.New(errs.KindValidationFailed, "agent labels must contain valid NUL-free UTF-8")
	}
	return nil
}

func validateUpdateRequest(request UpdateRequest) error {
	if err := validateAgentID(request.AgentID); err != nil {
		return err
	}
	if !imageref.IsDigestPinned(request.PreviousImage) ||
		!imageref.IsDigestPinned(request.DesiredImage) {
		return errs.New(errs.KindValidationFailed, "agent update images must be digest-pinned references")
	}
	if request.PreviousImage == request.DesiredImage {
		return errs.New(errs.KindStateConflict, "agent already runs the configured image")
	}
	if request.StartingGeneration == 0 || request.StartingGeneration > ^uint64(0)-2 {
		return errs.New(errs.KindValidationFailed, "agent update starting generation is invalid")
	}
	return nil
}

func validateAgentID(agentID string) error {
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	return nil
}

func validateStored(stored StoredRecord) error {
	if stored.Revision <= 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid revision")
	}
	if err := ids.Validate(ids.KindAgent, stored.Record.ID); err != nil {
		return errs.New(errs.KindInternal, "local agent record has an invalid id")
	}
	if err := ids.Validate(ids.KindTask, stored.Record.EnrollmentTaskID); err != nil {
		return errs.New(errs.KindInternal, "local agent record has an invalid enrollment Task id")
	}
	if !imageref.IsDigestPinned(stored.Record.Image) {
		return errs.New(errs.KindInternal, "local agent record has an invalid image")
	}
	if stored.Record.Generation == 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid generation")
	}
	if !isNonzeroUTC(stored.Record.CreatedAt) {
		return errs.New(errs.KindInternal, "local agent record has an invalid creation time")
	}
	switch stored.Record.Phase {
	case PhaseProvisioning:
		if !stored.Record.ReadyAt.IsZero() {
			return errs.New(errs.KindInternal, "provisioning local agent has a Ready timestamp")
		}
	case PhaseReady, PhaseUpdating, PhaseDeleting:
		if !isNonzeroUTC(stored.Record.ReadyAt) || stored.Record.ReadyAt.Before(stored.Record.CreatedAt) {
			return errs.New(errs.KindInternal, "local agent record has an invalid Ready timestamp")
		}
	}
	if stored.Record.Config.PullIntervalSeconds <= 0 || stored.Record.Config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid runtime config")
	}
	if !validLabels(stored.Record.Config.Labels) {
		return errs.New(errs.KindInternal, "local agent record has invalid labels")
	}
	switch stored.Record.Phase {
	case PhaseProvisioning, PhaseReady, PhaseUpdating:
		if err := validateCredential(stored.Record.Credential, false); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("local agent record has an invalid credential: %w", err))
		}
	case PhaseDeleting:
		if err := validateCredential(stored.Record.Credential, true); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("local agent record has an invalid credential: %w", err))
		}
	default:
		return errs.New(errs.KindInternal, "local agent record has an invalid lifecycle phase")
	}
	return nil
}

func validateCredential(credential Credential, revoked bool) error {
	if len(credential.EncryptedToken) == 0 {
		return errors.New("encrypted token is empty")
	}
	if revoked {
		if credential.Digest != "" {
			return errors.New("revoked token digest is present")
		}
		return nil
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(credential.Digest)
	if err != nil || len(decoded) != agentprotocol.RawTokenBytes {
		return errors.New("token digest is not canonical unpadded base64url")
	}
	if base64.RawURLEncoding.EncodeToString(decoded) != credential.Digest {
		return errors.New("token digest is not canonical unpadded base64url")
	}
	return nil
}

// StaleWindow is the locked Ready-heartbeat deadline shared by health
// projection and stale-assignment recovery.
func StaleWindow(pullIntervalSeconds int32) time.Duration {
	window := 3 * time.Duration(pullIntervalSeconds) * time.Second
	if window < 30*time.Second {
		return 30 * time.Second
	}
	return window
}

func projectAgent(record Record) Agent {
	return Agent{
		ID:               record.ID,
		EnrollmentTaskID: record.EnrollmentTaskID,
		Image:            record.Image,
		Generation:       record.Generation,
		Phase:            record.Phase,
		Config:           cloneConfig(record.Config),
		CreatedAt:        record.CreatedAt,
		ReadyAt:          record.ReadyAt,
	}
}

func cloneRecord(record Record) Record {
	record.Config = cloneConfig(record.Config)
	record.Credential = cloneCredential(record.Credential)
	return record
}

func cloneConfig(config Config) Config {
	cloned := Config{
		PullIntervalSeconds: config.PullIntervalSeconds,
		MaxConcurrentTasks:  config.MaxConcurrentTasks,
	}
	if config.Labels != nil {
		cloned.Labels = make(map[string]string, len(config.Labels))
		for key, value := range config.Labels {
			cloned.Labels[key] = value
		}
	}
	return cloned
}

func cloneCredential(credential Credential) Credential {
	return Credential{
		EncryptedToken: append([]byte(nil), credential.EncryptedToken...),
		Digest:         credential.Digest,
	}
}

func safePortError(ctx context.Context, err error, message string) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if kind, ok := errs.KindOf(err); ok {
		return errs.New(kind, message)
	}
	return errs.New(errs.KindInternal, message)
}

func validLabels(labels map[string]string) bool {
	for key, value := range labels {
		if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return false
		}
	}
	return true
}

func isNonzeroUTC(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC
}

func isAgentNotFound(err error) bool {
	return errors.Is(err, errs.New(errs.KindAgentNotFound, ""))
}

func agentNotFound(agentID string) error {
	return errs.Newf(errs.KindAgentNotFound, "agent %s was not found", agentID)
}
