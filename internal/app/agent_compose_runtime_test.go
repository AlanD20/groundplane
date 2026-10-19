package app

import (
	"errors"
	"testing"
)

type fakeOwnedComposeHelper struct {
	ownedComposeHelper
	closed bool
}

func (helper *fakeOwnedComposeHelper) Close() error {
	helper.closed = true
	return nil
}

type fakeOwnedComposeObserver struct {
	ownedComposeObserver
	closed bool
}

func (observer *fakeOwnedComposeObserver) Close() error {
	observer.closed = true
	return nil
}

// HOST-02/07: the native resource owner must close both independently owned
// helper clients; no execution method is supplied or allowed in this journey.
func TestAgentComposeResourcesCloseHelperAndObserver(t *testing.T) {
	helper := &fakeOwnedComposeHelper{}
	observer := &fakeOwnedComposeObserver{}
	resources, err := newAgentComposeResources(
		"ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		func(string) (ownedComposeHelper, error) { return helper, nil },
		func() (ownedComposeObserver, error) { return observer, nil },
	)
	if err != nil {
		t.Fatalf("newAgentComposeResources() error = %v", err)
	}
	if resources == nil {
		t.Fatal("newAgentComposeResources() returned no owned resources")
	}
	if err := resources.Close(); err != nil {
		t.Fatalf("resources.Close() error = %v", err)
	}
	if !helper.closed || !observer.closed {
		t.Fatalf("resources.Close() helper = %t, observer = %t", helper.closed, observer.closed)
	}
}

// HOST-02/07: failed observer construction must release the already-created
// helper and preserve the original failure, not leak a partially owned runtime.
func TestAgentComposeResourcesCloseHelperWhenObserverFails(t *testing.T) {
	helper := &fakeOwnedComposeHelper{}
	wantErr := errors.New("observer unavailable")
	resources, err := newAgentComposeResources(
		"ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		func(string) (ownedComposeHelper, error) { return helper, nil },
		func() (ownedComposeObserver, error) { return nil, wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("newAgentComposeResources() error = %v, want observer error", err)
	}
	if resources != nil || !helper.closed {
		t.Fatalf("newAgentComposeResources() resources = %#v, helper closed = %t", resources, helper.closed)
	}
}
