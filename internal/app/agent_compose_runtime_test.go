package app

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fakeOwnedComposeHelper struct {
	closed bool
}

func (helper *fakeOwnedComposeHelper) Execute(
	context.Context,
	*agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return nil, nil
}

func (helper *fakeOwnedComposeHelper) Close() error {
	helper.closed = true
	return nil
}

type fakeOwnedComposeObserver struct {
	closed bool
}

func (observer *fakeOwnedComposeObserver) Observe(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
) (*agentpb.ObservedProject, error) {
	return nil, nil
}

func (observer *fakeOwnedComposeObserver) Close() error {
	observer.closed = true
	return nil
}

func TestNewAgentComposeRuntimeOwnsHelperAndObserver(t *testing.T) {
	helper := &fakeOwnedComposeHelper{}
	observer := &fakeOwnedComposeObserver{}
	runtime, resources, err := newAgentComposeRuntime(
		"ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		func(string) (ownedComposeHelper, error) { return helper, nil },
		func() (ownedComposeObserver, error) { return observer, nil },
	)
	if err != nil {
		t.Fatalf("newAgentComposeRuntime() error = %v", err)
	}
	if runtime == nil || resources == nil {
		t.Fatal("newAgentComposeRuntime() returned an incomplete runtime")
	}
	if err := resources.Close(); err != nil {
		t.Fatalf("resources.Close() error = %v", err)
	}
	if !helper.closed || !observer.closed {
		t.Fatalf("resources.Close() helper = %t, observer = %t", helper.closed, observer.closed)
	}
}

func TestNewAgentComposeRuntimeClosesHelperWhenObserverFails(t *testing.T) {
	helper := &fakeOwnedComposeHelper{}
	wantErr := errors.New("observer unavailable")
	_, resources, err := newAgentComposeRuntime(
		"ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		func(string) (ownedComposeHelper, error) { return helper, nil },
		func() (ownedComposeObserver, error) { return nil, wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("newAgentComposeRuntime() error = %v, want observer error", err)
	}
	if resources != nil || !helper.closed {
		t.Fatalf("newAgentComposeRuntime() resources = %#v, helper closed = %t", resources, helper.closed)
	}
}

func TestNewAgentComposeRuntimeRequiresManagedImageIdentity(t *testing.T) {
	_, _, err := newAgentComposeRuntime(
		"",
		func(string) (ownedComposeHelper, error) { return &fakeOwnedComposeHelper{}, nil },
		func() (ownedComposeObserver, error) { return &fakeOwnedComposeObserver{}, nil },
	)
	if err == nil {
		t.Fatal("newAgentComposeRuntime() error = nil, want missing image rejection")
	}
}
