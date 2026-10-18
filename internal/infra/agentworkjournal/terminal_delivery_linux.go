package agentworkjournal

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareTerminalDelivery is called before a report can enter the stream.
// A lost response cannot be replaced by a different terminal outcome.
func (journal *Journal) PrepareTerminalDelivery(ctx context.Context, request *agentpb.AgentMessage) (*agentpb.ControllerMessage, error) {
	identity, _, supported, err := workreceipt.Describe(request)
	if err != nil || !supported {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, identity.AttemptId)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, conflict()
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, record.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, record.Attempt) {
		return nil, conflict()
	}
	if prior := record.TerminalDelivery; prior != nil {
		if !sameTerminalRequest(*prior, request) {
			return nil, conflict()
		}
		return prior.Acknowledgement()
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, invalid()
	}
	defer clear(raw)
	record.TerminalDelivery = &TerminalDelivery{Request: raw}
	return nil, journal.writeRecord(ctx, *record)
}

// AcknowledgeTerminalDelivery runs before waking the operation waiting for its
// result. A crash between transport ACK and typed settlement retains the reply.
func (journal *Journal) AcknowledgeTerminalDelivery(ctx context.Context, request *agentpb.AgentMessage, reply *agentpb.ControllerMessage) error {
	if err := workreceipt.Match(request, reply); err != nil {
		return err
	}
	identity, _, _, err := workreceipt.Describe(request)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, identity.AttemptId)
	if err != nil {
		return err
	}
	if record == nil || record.TerminalDelivery == nil || !sameTerminalRequest(*record.TerminalDelivery, request) {
		return conflict()
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(reply)
	if err != nil {
		return invalid()
	}
	defer clear(raw)
	if len(record.TerminalDelivery.Reply) != 0 {
		if !sameTerminalReply(*record.TerminalDelivery, raw) {
			return conflict()
		}
		return nil
	}
	record.TerminalDelivery.Reply = raw
	return journal.writeRecord(ctx, *record)
}

// JournalEntry is a compact startup inventory entry. A committed record without
// a terminal delivery is interrupted work, not an invented failure or success.
type JournalEntry struct {
	Attempt  workexecution.Attempt
	Delivery *TerminalDelivery
}

type JournalPage struct {
	Entries     []JournalEntry
	Uncommitted []string
	After       string
}

// Inventory reads at most 32 committed/partial attempt identities per page.
// Callers must join local workers before opening a new startup inventory.
func (journal *Journal) Inventory(ctx context.Context, after string) (JournalPage, error) {
	if after != "" && ids.Validate(ids.KindAttempt, after) != nil {
		return JournalPage{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return JournalPage{}, err
	}
	defer unlock()
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return JournalPage{}, err
	}
	names := make([]string, 0, len(attempts))
	for id := range attempts {
		if strings.Compare(id, after) > 0 {
			names = append(names, id)
		}
	}
	slices.Sort(names)
	page := JournalPage{}
	for index, id := range names {
		if index == 32 {
			page.After = names[index-1]
			break
		}
		if !attempts[id] {
			page.Uncommitted = append(page.Uncommitted, id)
			continue
		}
		record, err := journal.readRecord(ctx, id)
		if err != nil {
			return JournalPage{}, err
		}
		if record == nil {
			return JournalPage{}, conflict()
		}
		entry := JournalEntry{Attempt: record.Attempt}
		if delivery := record.TerminalDelivery; delivery != nil {
			entry.Delivery = &TerminalDelivery{Request: slices.Clone(delivery.Request), Reply: slices.Clone(delivery.Reply)}
		}
		page.Entries = append(page.Entries, entry)
	}
	return page, nil
}
