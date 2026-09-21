package serviceobservation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type readerAgents struct {
	agent     testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]
	calls     int
	err       error
	onRecheck func(*testkeyvalue.Versioned[testlocalagents.LocalAgentRecord])
}

func (agents *readerAgents) GetSingleton(
	context.Context,
) (testkeyvalue.Versioned[testlocalagents.LocalAgentRecord], error) {
	agents.calls++
	result := agents.agent
	if agents.calls > 1 && agents.onRecheck != nil {
		agents.onRecheck(&result)
	}
	return result, agents.err
}

// Rationale: the public read must choose the durable singleton, not any online
// Agent, and discard evidence if enrollment changes before presentation.
func TestObservationReaderFencesHostIdentityAndProjectsCompleteSnapshot(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*readerAgents)
		available bool
	}{
		{"ready", func(*readerAgents) {}, true},
		{"not enrolled", func(agents *readerAgents) { agents.err = errs.New(errs.KindAgentNotFound, "not enrolled") }, false},
		{"updating", func(agents *readerAgents) { agents.agent.Record.Phase = testlocalagents.LocalAgentPhaseUpdating }, false},
		{"changed enrollment", func(agents *readerAgents) {
			agents.onRecheck = func(current *testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]) { current.Revision++ }
		}, false},
		{"changed generation", func(agents *readerAgents) {
			agents.onRecheck = func(current *testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]) { current.Record.Generation++ }
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSourceFixture(t, 600, domain.StrategyRecreate, "", nil, 2)
			started := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
			agentID := ids.NewAt(ids.KindAgent, started, 610)
			agents := &readerAgents{agent: testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]{
				Record: testlocalagents.LocalAgentRecord{
					ID:         agentID,
					Generation: 2,
					Phase:      testlocalagents.LocalAgentPhaseReady,
				}, Revision: 20,
			}}
			test.configure(agents)
			channel := &fakeChannel{
				result: func(targets []*agentpb.ServiceObservationTarget) (*agentpb.ServiceObservationResult, error) {
					return availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
						fixture.service.Record.Desired.ID: {Healthy: 1, Failed: 1},
					}), nil
				},
			}
			observer, err := New(
				&fakeServices{pages: []testkeyvalue.Page[testservices.ServiceRecord]{servicePage(60, fixture)}},
				&fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}, channel,
				sequenceClock(started, started.Add(time.Second), started.Add(2*time.Second)),
			)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := NewReader(agents, observer)
			if err != nil {
				t.Fatal(err)
			}
			got := reader.ObserveServices(
				t.Context(),
				[]testkeyvalue.Versioned[testservices.ServiceRecord]{fixture.service},
			)
			if len(got) != 1 {
				t.Fatal(got)
			}
			if test.available {
				if got[0].State != api.ServiceObservationDegraded || got[0].Replicas.Healthy != 1 ||
					got[0].Replicas.Failed != 1 || *got[0].ExpectedReplicas != 2 || *got[0].ServingReleaseID != fixture.serving.Intent.ID ||
					*got[0].ObservedAt != started || *got[0].ExpiresAt != started.Add(15*time.Second) ||
					channel.calls != 1 || channel.agentIDs[0] != agentID {
					t.Fatalf("incomplete or wrong public evidence: %+v", got[0])
				}
			} else {
				encoded, err := json.Marshal(got[0])
				if err != nil || string(encoded) != `{"state":"unavailable"}` {
					t.Fatalf("unavailable leaked snapshot fields: %s, %v", encoded, err)
				}
			}
			if agents.agent.Record.Phase != testlocalagents.LocalAgentPhaseReady || agents.err != nil {
				if channel.calls != 0 {
					t.Fatal("unready singleton reached Agent channel")
				}
			}
		})
	}
}

// Rationale: wire fields are omitted for mutations and unavailable snapshots,
// while a fresh zero count is explicit evidence rather than missing data.
func TestPublicObservationJSONAndExpiry(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	input := Observation{State: "absent", Snapshot: &Snapshot{
		ObservedAt: now, ExpiresAt: now.Add(15 * time.Second), ServingReleaseID: "release", ExpectedReplicas: 2,
		Replicas: &agentpb.ServiceReplicaCounts{},
	}}
	for _, at := range []time.Time{now.Add(-time.Second), now.Add(15 * time.Second)} {
		got := publicObservation(input, at)
		if got.State != api.ServiceObservationUnavailable || got.Replicas != nil {
			t.Fatal("out-of-window evidence escaped")
		}
	}
	encoded, err := json.Marshal(publicObservation(input, now))
	if err != nil || !strings.Contains(string(encoded), `"running":0`) ||
		!strings.Contains(string(encoded), `"failed":0`) {
		t.Fatalf("zero counts disappeared: %s, %v", encoded, err)
	}
	encoded, err = json.Marshal(api.Service{})
	if err != nil || strings.Contains(string(encoded), "observation") {
		t.Fatalf("mutation projection claims an observation: %s, %v", encoded, err)
	}
}
