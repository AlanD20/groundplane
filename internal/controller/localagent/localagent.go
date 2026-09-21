package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
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
