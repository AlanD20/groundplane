package backingprovision

import (
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ConfigurationHeader(input HelperInput, file *agentpb.MaterializeFile) (entrymaterialization.Header, error) {
	if input.Assignment.Recovery != nil || input.Progress == nil || !input.Progress.InFlight ||
		input.Progress.Stage != WriteConfiguration || input.Progress.Validate(input.Assignment.Attempt, input.Layout) != nil ||
		file == nil || executionplan.RejectUnknown(file) != nil || len(file.Sha256) != sha256.Size ||
		file.OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV ||
		!slices.ContainsFunc(
			input.Assignment.Procedure.Files,
			func(expected *agentpb.MaterializeFile) bool { return proto.Equal(expected, file) },
		) {
		return entrymaterialization.Header{}, invalid(
			"backing creation file is not part of its active captured configuration",
		)
	}
	stage, err := StageID(input.Assignment.Attempt.WorkItemID, WriteConfiguration)
	if err != nil {
		return entrymaterialization.Header{}, err
	}
	var digest entrymaterialization.Digest
	copy(digest[:], file.Sha256)
	return entrymaterialization.NewHeader(
		entrymaterialization.HeaderSpec{TaskID: input.Assignment.Attempt.TaskID, StepID: stage,
			EnvironmentID: file.EnvironmentId, Generation: input.Assignment.Procedure.RenderGeneration, Destination: file.Destination,
			ServiceID: file.ServiceId, ServiceName: file.ServiceName, OutputKind: entrymaterialization.OutputGeneratedEnv,
			UID: file.Uid, GID: file.Gid, Mode: entrymaterialization.Mode(file.Mode), Length: file.Length, Digest: digest},
	)
}

func ValidateHookDelivery(attemptInput HelperInput, delivery *agentpb.BackingProvisionHookDelivery) error {
	assignment := attemptInput.Assignment
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || assignment.Recovery != nil ||
		attemptInput.Progress == nil || !attemptInput.Progress.InFlight || attemptInput.Progress.Stage != AfterStart ||
		attemptInput.Progress.Validate(
			assignment.Attempt,
			attemptInput.Layout,
		) != nil || assignment.Procedure.AfterStart == nil {
		return invalid("backing creation hook has no active captured execution boundary")
	}
	identity, err := workexecution.DecodeAgentAttempt(delivery.Attempt, assignment.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(identity, assignment.Attempt) {
		return invalid("backing creation hook delivery belongs to another assignment")
	}
	stage, err := StageID(assignment.Attempt.WorkItemID, AfterStart)
	if err != nil || delivery.StageId != stage || delivery.Hook == nil {
		return invalid("backing creation hook delivery names a different stage")
	}
	hook := delivery.Hook
	expected := assignment.Procedure.AfterStart
	if hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START ||
		hook.BackingServiceId != attemptInput.Layout.ServiceID ||
		!slices.Equal(hook.Command, expected.Command) ||
		hook.TimeoutSeconds != expected.TimeoutSeconds {
		return invalid("backing creation hook delivery differs from its captured command")
	}
	definition, values, schema, err := executionplan.DecodeBackingHookProcedure(hook)
	if err != nil {
		return err
	}
	// Decode borrows the delivery's value slices. Validation must not clear
	// them: the authenticated caller still owns sending and then clearing them.
	return backinghook.Validate(definition, values, schema)
}
