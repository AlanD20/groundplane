package agentchannel

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeStartIdentityOwner struct{ calls int }

func (owner *nativeStartIdentityOwner) AuthorizeAgentStart(
	context.Context,
	workqueue.ResumeView,
) (workqueue.Change, error) {
	owner.calls++
	return workqueue.Change{}, errs.New(errs.KindInternal, "test has no admitted work to start")
}

// TASK-111: malformed or foreign attempt authority cannot reach an operation
// Start owner or write a grant. A valid identity reaches the missing-Task lookup,
// distinguishing a real decoder refusal from an unconfigured service.
func TestNativeStartRefusesMissingAndForeignAttemptIdentity(t *testing.T) {
	server, session, peer, store := nativeDispatchFixture(t)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	owner := &nativeStartIdentityOwner{}
	starts, err := workdispatch.NewAgentStarts(queue, []workdispatch.AgentStartRegistration{{
		Operation: workexecution.Script, Owner: owner,
	}})
	if err != nil {
		t.Fatal(err)
	}
	server.workStarts = starts
	revision := store.revision
	for _, name := range []string{"nil-request", "missing-attempt", "missing-input", "foreign-host", "foreign-agent", "stale-generation", "valid-missing-task"} {
		t.Run(name, func(t *testing.T) {
			request := &agentpb.WorkStartRequest{Attempt: proto.CloneOf(peer.attempt)}
			kind := errs.KindValidationFailed
			switch name {
			case "nil-request":
				request = nil
			case "missing-attempt":
				request.Attempt = nil
			case "missing-input":
				request.Attempt.Input = nil
			case "foreign-host":
				request.Attempt.HostId = ids.New(ids.KindHost)
				kind = errs.KindStateConflict
			case "foreign-agent":
				request.Attempt.AgentId = ids.New(ids.KindAgent)
				kind = errs.KindStateConflict
			case "stale-generation":
				request.Attempt.ExecutorGeneration++
				kind = errs.KindStateConflict
			case "valid-missing-task":
				kind = errs.KindTaskNotFound
			}
			ack, err := server.startWork(t.Context(), session, 1, request)
			if got, ok := errs.KindOf(err); !ok || got != kind || ack != nil || owner.calls != 0 ||
				store.revision != revision {
				t.Fatalf(
					"Start identity boundary: kind=%v ack=%t owner=%d revision=%d",
					got,
					ack != nil,
					owner.calls,
					store.revision,
				)
			}
		})
	}
}
