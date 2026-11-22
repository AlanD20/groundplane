package agentchannel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const executionQuiescenceTimeout = 10 * time.Second

type executionQuiescenceCommand struct {
	ctx               context.Context
	request           *agentpb.CheckExecutionQuiescence
	processGeneration [16]byte
	result            chan error
}

// executionQuiescenceProof pins the authenticated process after a successful
// observation until its covering assignment pause ends or lifecycle authority
// advances. This closes the response-to-upgrade-journal reconnect race.
type executionQuiescenceProof struct {
	generation        uint64
	fence             uint64
	processGeneration [16]byte
}

// CheckExecutionQuiescence asks the exact authenticated Agent process for a
// fresh execution observation. Assignment admission must already be paused for
// the supplied generation, and no auxiliary Agent operation may overlap it.
func (registry *Registry) CheckExecutionQuiescence(
	ctx context.Context,
	agentID string,
	generation uint64,
) error {
	return registry.checkExecutionQuiescence(ctx, agentID, generation, [16]byte{})
}

// CheckExecutionQuiescenceForProcess additionally binds the fresh response to
// the process nonce read from durable executor registration.
func (registry *Registry) CheckExecutionQuiescenceForProcess(
	ctx context.Context,
	agentID string,
	generation uint64,
	processGeneration [16]byte,
) error {
	if processGeneration == [16]byte{} {
		return errs.New(errs.KindValidationFailed, "Agent execution process identity is invalid")
	}
	return registry.checkExecutionQuiescence(ctx, agentID, generation, processGeneration)
}

func (registry *Registry) checkExecutionQuiescence(
	ctx context.Context,
	agentID string,
	generation uint64,
	expectedProcess [16]byte,
) error {
	if registry == nil {
		return errs.New(errs.KindInternal, "Agent session registry is required")
	}
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}
	requestID, err := newExecutionQuiescenceRequestID()
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, executionQuiescenceTimeout)
	defer cancel()

	registry.mu.Lock()
	state := registry.agents[agentID]
	if state == nil || state.generation != generation || !state.online || state.revoked || closed(state.done) {
		registry.mu.Unlock()
		return executionQuiescenceUnavailable()
	}
	if !registry.assignmentsPausedLocked(agentID, generation) {
		registry.mu.Unlock()
		return errs.New(
			errs.KindStateConflict,
			"Agent assignment admission must be paused before execution qualification",
		)
	}
	if state.processAuthentication.ExecutionPlanSchema == 0 {
		registry.mu.Unlock()
		return executionQuiescenceUnavailable()
	}
	if expectedProcess != [16]byte{} && state.processAuthentication.ProcessGeneration != expectedProcess {
		registry.mu.Unlock()
		return errs.New(errs.KindStateConflict, "Agent execution process differs from durable registration")
	}
	if state.activeQuiescence != nil || registry.hasActiveAuxiliaryLocked(state) {
		registry.mu.Unlock()
		return errs.New(errs.KindResourceInUse, "Agent auxiliary operation is active during execution qualification")
	}
	command := &executionQuiescenceCommand{
		ctx: queryCtx,
		request: &agentpb.CheckExecutionQuiescence{
			RequestId: requestID,
		},
		processGeneration: state.processAuthentication.ProcessGeneration,
		result:            make(chan error, 1),
	}
	state.activeQuiescence = command
	registry.mu.Unlock()
	defer func() {
		registry.mu.Lock()
		if state.activeQuiescence == command {
			state.activeQuiescence = nil
		}
		registry.mu.Unlock()
	}()

	select {
	case state.quiescenceCommands <- command:
	case <-queryCtx.Done():
		return queryCtx.Err()
	case <-state.done:
		return executionQuiescenceUnavailable()
	}
	select {
	case err := <-command.result:
		registry.mu.Lock()
		current := registry.executionQuiescenceCommandCurrentLocked(agentID, state, command)
		if current && err == nil {
			lifecycle := registry.lifecycleFenceLocked(agentID)
			lifecycle.quiescenceProof = &executionQuiescenceProof{
				generation:        state.generation,
				fence:             state.fence,
				processGeneration: command.processGeneration,
			}
		}
		registry.mu.Unlock()
		if queryErr := queryCtx.Err(); queryErr != nil {
			return queryErr
		}
		if !current {
			return executionQuiescenceUnavailable()
		}
		return err
	case <-queryCtx.Done():
		return queryCtx.Err()
	case <-state.done:
		return executionQuiescenceUnavailable()
	}
}

func executionQuiescenceUnavailable() error {
	return errs.New(errs.KindRequestUnavailable, "Agent execution qualification is unavailable")
}

func auxiliaryPaused() error {
	return errs.New(errs.KindResourceInUse, "Agent auxiliary operation is unavailable while assignments are paused")
}

// The caller holds Registry.mu.
func (registry *Registry) auxiliaryAllowedLocked(agentID string, state *sessionState) bool {
	return state != nil && !registry.assignmentsPausedLocked(agentID, state.generation)
}

// The caller holds Registry.mu.
func (registry *Registry) hasActiveAuxiliaryLocked(state *sessionState) bool {
	if state.activeImage != nil || state.activeObservation != nil || state.activeMonitoring != nil || state.activeBackingAccess != nil ||
		state.activeDNSCheck != nil ||
		state.activeEdgeCheck != nil {
		return true
	}
	for _, process := range registry.terminals {
		if process.state == state {
			return true
		}
	}
	return false
}

func (session *Session) executionQuiescenceCommandCurrent(command *executionQuiescenceCommand) bool {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	return session.registry.executionQuiescenceCommandCurrentLocked(session.agentID, session.state, command)
}

// The caller holds Registry.mu.
func (registry *Registry) executionQuiescenceCommandCurrentLocked(
	agentID string,
	state *sessionState,
	command *executionQuiescenceCommand,
) bool {
	current := registry.agents[agentID]
	return command != nil && command.ctx.Err() == nil && current == state && current.online && !current.revoked &&
		!closed(current.done) && current.activeQuiescence == command &&
		current.processAuthentication.ProcessGeneration == command.processGeneration &&
		registry.assignmentsPausedLocked(agentID, current.generation) && !registry.hasActiveAuxiliaryLocked(current)
}

func (session *Session) acceptExecutionQuiescenceResult(message *agentpb.AgentMessage) {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	active := session.state.activeQuiescence
	result := message.GetExecutionQuiescenceResult()
	if !session.registry.executionQuiescenceCommandCurrentLocked(session.agentID, session.state, active) ||
		result == nil || result.RequestId != active.request.RequestId {
		return
	}
	var err error
	if proto.Size(message) > 256 || executionplan.RejectUnknown(message) != nil {
		err = errs.New(errs.KindValidationFailed, "Agent execution qualification result is invalid")
	} else if !result.Quiescent {
		err = errs.New(errs.KindResourceInUse, "Agent process still owns active execution")
	}
	select {
	case active.result <- err:
	default:
	}
}

func newExecutionQuiescenceRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
