package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ScopedRunnerRuntime interface {
	ReplaceRegisteredRunner(context.Context, runnerallocation.RuntimePlan, runnerallocation.RuntimePlan,
		runnerallocation.RunnerRuntimeEvidence) (runnerallocation.RunnerRuntimeEvidence, error)
	ObserveScopedCandidate(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeEvidence, error)
	VerifyLegacyGitHubRegistration(context.Context, runnerallocation.RuntimePlan, int64) error
	WaitLegacyIdle(context.Context, runnerallocation.RuntimePlan, runnerallocation.RunnerRuntimeEvidence) error
	ScopedImage(context.Context, runnerallocation.RuntimePlan) (buildversion.ContainerIdentity, error)
}

type ScopedRunnerCredentials interface {
	EnsureScopedCredential(context.Context, keyvalue.Versioned[runnerscoped.Migration]) error
	StageCompletion(
		context.Context,
		keyvalue.Versioned[runnerscoped.Migration],
	) ([]keyvalue.Condition, keyvalue.Mutation, error)
}

// ScopedMigration moves a registered Runner to its next scoped runtime
// generation. It shares Runner lifecycle ownership with native maintenance
// and host execution. Interrupted cutovers only resume the scoped candidate.
type ScopedMigration struct {
	records     *runnerscoped.Repository
	runtime     ScopedRunnerRuntime
	credentials ScopedRunnerCredentials
	executor    *Executor
}

func NewScopedMigration(records *runnerscoped.Repository, runtime ScopedRunnerRuntime,
	credentials ScopedRunnerCredentials, executor *Executor,
) (*ScopedMigration, error) {
	if records == nil || runtime == nil || credentials == nil || executor == nil {
		return nil, errs.New(errs.KindInternal, "Scoped Runner migration dependencies are required")
	}
	return &ScopedMigration{records: records, runtime: runtime, credentials: credentials, executor: executor}, nil
}

func (migration *ScopedMigration) PrepareBegin(ctx context.Context, runnerID, taskID string, revision int64,
	policy corerunner.IsolationPolicy, candidateImage string, githubID int64, request runnerscoped.Request,
	provenance []keyvalue.Condition,
) (runnerscoped.Migration, workqueue.Change, error) {
	legacy, err := migration.records.Predecessor(ctx, runnerID)
	if err != nil {
		return runnerscoped.Migration{}, workqueue.Change{}, err
	}
	if err := corerunner.ValidatePlan(legacy); err != nil {
		return runnerscoped.Migration{}, workqueue.Change{}, err
	}
	if legacy.RuntimeEpoch == ^uint64(0) || legacy.Egress.ScopedEndpoint.IsValid() ||
		!policy.ScopedEndpoint.IsValid() {
		return runnerscoped.Migration{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Runner cannot move to the requested scoped runtime",
		)
	}
	next := legacy.Clone()
	next.RuntimeEpoch++
	next.Egress.RegistrationOnly = false
	if candidateImage != legacy.Container.ImageRef {
		next.ScopedPredecessorImage, next.Container.ImageRef = legacy.Container.ImageRef, candidateImage
	}
	next.Egress.ScopedEndpoint, next.Egress.ScopedRegistry = policy.ScopedEndpoint, policy.ScopedRegistry
	if err := corerunner.ValidatePlan(next); err != nil {
		return runnerscoped.Migration{}, workqueue.Change{}, err
	}
	if err := migration.runtime.VerifyLegacyGitHubRegistration(ctx, legacy, githubID); err != nil {
		return runnerscoped.Migration{}, workqueue.Change{}, err
	}
	software, err := migration.runtime.ScopedImage(ctx, next)
	if err != nil {
		return runnerscoped.Migration{}, workqueue.Change{}, err
	}
	return migration.records.PrepareBegin(
		ctx,
		runnerID,
		taskID,
		revision,
		next,
		software,
		githubID,
		request,
		provenance,
		time.Now().UTC(),
	)
}

// Execute is called only by the currently authorized native Task attempt.
// Reconciliation can adopt an independently observed candidate; it cannot
// issue newly destructive work without a fresh Task attempt.
func (migration *ScopedMigration) Execute(ctx context.Context, runnerID, taskID string,
	forward func(context.Context, runnerscoped.Migration) ([]keyvalue.Condition, error),
) error {
	var guards []keyvalue.Condition
	observeOnly := forward == nil
	if !observeOnly {
		initial, exists, err := migration.records.Load(ctx, runnerID)
		if err != nil {
			return err
		}
		if !exists || initial.Record.ID != taskID {
			return errs.New(errs.KindStateConflict, "Runner migration no longer owns this Task")
		}
		if initial.Record.Phase == runnerscoped.Pending {
			legacy := initial.Record.Legacy
			if err := migration.runtime.WaitLegacyIdle(ctx, legacy.Plan, legacy.Evidence); err != nil {
				return err
			}
		}
		if initial.Record.Phase != runnerscoped.Complete {
			guards, err = forward(ctx, initial.Record)
			if err != nil {
				return err
			}
			image, err := migration.runtime.ScopedImage(ctx, initial.Record.Candidate)
			if err != nil {
				return err
			}
			if image != initial.Record.CandidateSoftware {
				return errs.New(errs.KindStateConflict, "Verified scoped Runner image changed after acceptance")
			}
		}
	}
	migration.executor.mu.Lock()
	defer migration.executor.mu.Unlock()
	current, exists, err := migration.records.Load(ctx, runnerID)
	if err != nil {
		return err
	}
	if !exists || current.Record.ID != taskID {
		return errs.New(errs.KindStateConflict, "Runner migration no longer owns this Task")
	}
	if current.Record.Phase == runnerscoped.Complete {
		return nil
	}
	plan := current.Record.Candidate
	if err := corerunner.ValidatePlan(plan); err != nil {
		return err
	}
	if observeOnly && current.Record.Phase == runnerscoped.Pending {
		return nil
	}
	if !observeOnly {
		current, err = migration.records.Advance(ctx, runnerID, current.Revision, nil, time.Now().UTC(),
			workqueue.Change{Conditions: guards})
		if err != nil {
			return err
		}
	}
	if err := migration.records.CheckCutover(ctx, runnerID, current.Revision); err != nil {
		return err
	}
	var evidence runnerallocation.RunnerRuntimeEvidence
	if observeOnly {
		evidence, err = migration.runtime.ObserveScopedCandidate(ctx, plan)
	} else {
		legacy := current.Record.Legacy
		if err := migration.runtime.VerifyLegacyGitHubRegistration(ctx, legacy.Plan, current.Record.GitHubID); err != nil {
			return err
		}
		if err := migration.credentials.EnsureScopedCredential(ctx, current); err != nil {
			return err
		}
		evidence, err = migration.runtime.ReplaceRegisteredRunner(ctx, legacy.Plan, plan, legacy.Evidence)
	}
	if err != nil {
		return err
	}
	conditions, removal, err := migration.credentials.StageCompletion(ctx, current)
	if err != nil {
		return err
	}
	_, err = migration.records.Advance(ctx, runnerID, current.Revision, &evidence, time.Now().UTC(),
		workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{removal}})
	return err
}
