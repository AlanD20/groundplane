package release

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestCheckpointTransitionsDoNotReopenTerminalState(t *testing.T) {
	t.Parallel()
	for _, state := range []State{StateCompleted, StateFailed, StateTimedOut, StateAborted} {
		if CanTransition(state, StateRunning) || CanTransition(state, StateRecoveryRequired) {
			t.Fatalf("terminal state %q reopened", state)
		}
	}
	if !CanTransition(StateRecoveryRequired, StateRecovering) || !CanTransition(StateRecovering, StateRecoveryRequired) {
		t.Fatal("recovery transition is not closed")
	}
}

func TestGroupExecutorRunsForwardAndCompensatesInReverse(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	manifest := fixtureManifest(now)
	executor, err := NewGroupExecutor(manifest)
	if err != nil {
		t.Fatalf("NewGroupExecutor() error = %v", err)
	}
	progress, err := executor.Begin(ids.NewAt(ids.KindTask, now, 30), now)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	for index := 0; index < 2; index++ {
		member := manifest.Members[index]
		progress, err = executor.RecordServing(progress, MemberResult{
			Ordinal: member.Ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: MemberServing, ServingReleaseID: member.ReleaseID,
		}, now.Add(time.Duration(index+1)*time.Second))
		if err != nil {
			t.Fatalf("RecordServing(%d) error = %v", index, err)
		}
	}
	failed := manifest.Members[2]
	progress, err = executor.RecordFailure(progress, MemberResult{
		Ordinal: failed.Ordinal, ServiceID: failed.ServiceID, ReleaseID: failed.ReleaseID,
		Outcome: MemberFailed, FailureCode: "health.failed", FailureDetail: "candidate unhealthy",
	}, now.Add(3*time.Second))
	if err != nil || progress.NextCompensationOrdinal != 2 {
		t.Fatalf("RecordFailure() = %#v, %v", progress, err)
	}
	for _, ordinal := range []uint32{2, 1} {
		member := manifest.Members[ordinal-1]
		progress, err = executor.RecordCompensation(progress, MemberResult{
			Ordinal: ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: MemberCompensated, CompensationReleaseID: ids.NewAt(ids.KindDeployment, now, int64(100+ordinal)),
		}, now.Add(time.Duration(6-ordinal)*time.Second))
		if err != nil {
			t.Fatalf("RecordCompensation(%d) error = %v", ordinal, err)
		}
	}
	if progress.Compensating || progress.NextCompensationOrdinal != 0 || progress.Results[0].Outcome != MemberCompensated ||
		progress.Results[1].Outcome != MemberCompensated || progress.Results[2].Outcome != MemberFailed {
		t.Fatalf("compensation result = %#v", progress)
	}
}

func TestValidateIntentRejectsMutableStatusEraFieldsByConstruction(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	intent := Intent{
		ID: ids.NewAt(ids.KindDeployment, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		ServiceID: ids.NewAt(ids.KindService, now, 3), OperationID: ids.NewAt(ids.KindOperation, now, 4),
		OperationKind: OperationDeploy, Image: "registry.invalid/app", Tag: "sha-123", Strategy: StrategyRecreate,
		OnFailure: OnFailureSwitchBack, RenderInputID: ids.NewAt(ids.KindConfig, now, 5),
		RenderInputDigest: strings.Repeat("a", 64), CreatedAt: now, Actor: "operator",
		Workspace:         Workspace{Kind: WorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, now, 6), ProjectID: ids.NewAt(ids.KindProject, now, 7), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2)},
		OriginatingTaskID: ids.NewAt(ids.KindTask, now, 8),
	}
	if err := ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent() error = %v", err)
	}
}

// Rationale: Blueprint reconciliation needs a distinct closed Release operation kind, while arbitrary kinds must remain invalid.
func TestValidateIntentOperationKinds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	intent := Intent{
		ID: ids.NewAt(ids.KindDeployment, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		ServiceID: ids.NewAt(ids.KindService, now, 3), OperationID: ids.NewAt(ids.KindOperation, now, 4),
		Image: "registry.invalid/app", Tag: "sha-123", Strategy: StrategyRecreate,
		OnFailure: OnFailureSwitchBack, RenderInputID: ids.NewAt(ids.KindConfig, now, 5),
		RenderInputDigest: strings.Repeat("a", 64), CreatedAt: now, Actor: "controller",
		Workspace:         Workspace{Kind: WorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, now, 6), ProjectID: ids.NewAt(ids.KindProject, now, 7), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2)},
		OriginatingTaskID: ids.NewAt(ids.KindTask, now, 8),
	}
	for _, test := range []struct {
		name    string
		kind    OperationKind
		wantErr bool
	}{
		{name: "blueprint apply", kind: OperationBlueprintApply},
		{name: "unknown", kind: OperationKind("unknown"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			intent.OperationKind = test.kind
			err := ValidateIntent(intent)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateIntent(%q) error = %v, wantErr %t", test.kind, err, test.wantErr)
			}
		})
	}
}

func fixtureManifest(now time.Time) GroupManifest {
	members := make([]GroupMember, 3)
	for index := range members {
		members[index] = GroupMember{
			Ordinal: uint32(index + 1), ServiceID: ids.NewAt(ids.KindService, now, int64(index+10)),
			ReleaseID: ids.NewAt(ids.KindDeployment, now, int64(index+20)),
		}
	}
	return GroupManifest{
		OperationID: ids.NewAt(ids.KindOperation, now, 1), ReleaseGroupID: ids.NewAt(ids.KindReleaseGroup, now, 2),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 3), FailurePolicy: OnFailureSwitchBack,
		Members: members, ConfiguredTimeoutSeconds: 15 * 60 * 60, ComputedBudgetSeconds: 80 * 60,
	}
}
