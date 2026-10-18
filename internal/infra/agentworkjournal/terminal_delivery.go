package agentworkjournal

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TerminalDelivery retains one exact outbound result and its authenticated
// acknowledgement. It is transport evidence, never permission to execute.
// Secret/configuration/hook delivery messages cannot enter this closed boundary.
type TerminalDelivery struct {
	Request []byte `json:"request"`
	Reply   []byte `json:"reply,omitempty"`
}

// The existing operation record bound remains unchanged. The complete attempt
// permits only this additional bounded request/reply pair, including JSON base64.
const MaximumAttemptRecordBytes = MaximumRecordBytes + 2*((workreceipt.MaximumBytes+2)/3*4) + 256

func (delivery TerminalDelivery) Message() (*agentpb.AgentMessage, error) {
	if len(delivery.Request) == 0 || len(delivery.Request) > workreceipt.MaximumBytes {
		return nil, invalid()
	}
	request := new(agentpb.AgentMessage)
	if err := proto.Unmarshal(delivery.Request, request); err != nil {
		return nil, invalid()
	}
	if _, _, supported, err := workreceipt.Describe(request); err != nil || !supported {
		return nil, invalid()
	}
	return request, nil
}

func (delivery TerminalDelivery) Acknowledgement() (*agentpb.ControllerMessage, error) {
	if len(delivery.Reply) == 0 {
		return nil, nil
	}
	if len(delivery.Reply) > workreceipt.MaximumBytes {
		return nil, invalid()
	}
	request, err := delivery.Message()
	if err != nil {
		return nil, err
	}
	reply := new(agentpb.ControllerMessage)
	if err := proto.Unmarshal(delivery.Reply, reply); err != nil || workreceipt.Match(request, reply) != nil {
		return nil, invalid()
	}
	return reply, nil
}

func (delivery TerminalDelivery) validate(attempt workexecution.Attempt) error {
	request, err := delivery.Message()
	if err != nil {
		return err
	}
	identity, _, _, err := workreceipt.Describe(request)
	if err != nil {
		return err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return invalid()
	}
	if _, err := delivery.Acknowledgement(); err != nil {
		return err
	}
	return nil
}

func sameTerminalRequest(delivery TerminalDelivery, request *agentpb.AgentMessage) bool {
	previous, err := delivery.Message()
	return err == nil && proto.Equal(previous, request)
}

func sameTerminalReply(delivery TerminalDelivery, reply []byte) bool {
	return bytes.Equal(delivery.Reply, reply)
}
