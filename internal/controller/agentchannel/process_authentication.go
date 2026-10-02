package agentchannel

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ProcessAuthentication is the immutable, non-secret tuple authenticated for
// one Agent process. ProcessGeneration is independent of durable Agent generation.
// A missing managed release digest does not attest to PostgreSQL support.
type ProcessAuthentication struct {
	ExecutionPlanSchema             uint32
	ProcessGeneration               [16]byte
	Postgres16ManagedReleaseSHA256  [sha256.Size]byte
	Postgres16ManagedReleasePresent bool
}

func processAuthenticationFromWire(authenticate *agentpb.Authenticate) (ProcessAuthentication, error) {
	if authenticate == nil {
		return ProcessAuthentication{}, errs.New(errs.KindValidationFailed, "Agent authentication is required")
	}
	if err := executionplan.RejectUnknown(authenticate); err != nil {
		return ProcessAuthentication{}, err
	}
	if authenticate.ExecutionPlanSchema != executionplan.SchemaVersion {
		return ProcessAuthentication{}, errs.New(
			errs.KindValidationFailed,
			"Agent execution plan schema is unsupported",
		)
	}
	if len(authenticate.ProcessGeneration) != 16 {
		return ProcessAuthentication{}, errs.New(
			errs.KindValidationFailed,
			"Agent process generation must contain 16 bytes",
		)
	}
	if len(authenticate.Postgres16ManagedReleaseSha256) != 0 &&
		len(authenticate.Postgres16ManagedReleaseSha256) != sha256.Size {
		return ProcessAuthentication{}, errs.New(
			errs.KindValidationFailed,
			"Agent PostgreSQL release digest must contain 32 bytes",
		)
	}
	tuple := ProcessAuthentication{
		ExecutionPlanSchema:             authenticate.ExecutionPlanSchema,
		Postgres16ManagedReleasePresent: len(authenticate.Postgres16ManagedReleaseSha256) != 0,
	}
	copy(tuple.ProcessGeneration[:], authenticate.ProcessGeneration)
	if tuple.ProcessGeneration == ([16]byte{}) {
		return ProcessAuthentication{}, errs.New(errs.KindValidationFailed, "Agent process generation is empty")
	}
	copy(tuple.Postgres16ManagedReleaseSHA256[:], authenticate.Postgres16ManagedReleaseSha256)
	return tuple, nil
}

// RecordProcessAuthentication binds the validated Authenticate tuple once,
// before the session can receive configuration or begin normal channel traffic.
func (s *Session) RecordProcessAuthentication(authenticate *agentpb.Authenticate) error {
	if s == nil || s.registry == nil || s.state == nil || authenticate == nil || authenticate.AgentId != s.agentID {
		return errs.New(errs.KindValidationFailed, "Agent authentication does not identify this session")
	}
	tuple, err := processAuthenticationFromWire(authenticate)
	if err != nil {
		return err
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if !s.processAuthenticationCurrentLocked() {
		return errs.New(errs.KindStateConflict, "Agent session is fenced")
	}
	if s.state.processAuthentication.ExecutionPlanSchema != 0 {
		return errs.New(errs.KindStateConflict, "Agent process authentication is already recorded")
	}
	s.state.processAuthentication = tuple
	return nil
}

// ProcessAuthentication returns an owned value only for this current session.
func (s *Session) ProcessAuthentication() (ProcessAuthentication, error) {
	if s == nil || s.registry == nil || s.state == nil {
		return ProcessAuthentication{}, errs.New(errs.KindStateConflict, "Agent session is required")
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if !s.processAuthenticationCurrentLocked() || s.state.processAuthentication.ExecutionPlanSchema == 0 {
		return ProcessAuthentication{}, errs.New(errs.KindStateConflict, "Agent process authentication is not current")
	}
	return s.state.processAuthentication, nil
}

// The caller holds Registry.mu.
func (s *Session) processAuthenticationCurrentLocked() bool {
	current := s.registry.agents[s.agentID]
	return current == s.state && current.fence == s.state.fence &&
		current.online && !current.revoked && !closed(current.done)
}
