package agentchannel

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// An empty inventory invokes no source or Volume classifier. Any unexpected
// operation callback fails rather than supplying invented recovery authority.
type nativeEmptyBackupRecovery struct{ BackupWorkReporter }

// TASK-108: a new Agent process increments the executor generation without
// rotating its credential. Startup must still reconcile and acknowledge staging
// through the actual channel handler, registrar and delivery repository.
// Empty inventory and sequential memory storage do not prove source recovery.
func TestNativeStagingReconcilesAfterAgentProcessReplacement(t *testing.T) {
	server, original, _, store := nativeDispatchFixture(t)
	agentID := original.agentID
	original.Close()
	repository, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	server.staging, server.backupWorkReports = repository, &nativeEmptyBackupRecovery{}
	var previous workexecution.Identity
	var previousProcess [16]byte
	var previousAck *agentpb.BackupStagingRecoveryAck
	for _, ordinal := range []byte{2, 3} {
		process := [16]byte{ordinal}
		registered, err := server.workIdentities.RegisterWorkProcess(t.Context(), agentID, 1, process,
			1, []workexecution.Operation{workexecution.Script})
		if err != nil {
			t.Fatal(err)
		}
		if registered.Generation != uint64(ordinal) {
			t.Fatal("fixture did not replace the executor process")
		}
		session, err := server.sessions.Open(t.Context(), agentID, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(session.Close)
		authenticate := &agentpb.Authenticate{AgentId: agentID,
			ExecutionPlanSchema: executionplan.SchemaVersion,
			ChannelSchema:       agentprotocol.SchemaVersion,
			ProcessGeneration:   process[:],
		}
		if err := session.RecordProcessAuthentication(authenticate); err != nil {
			t.Fatal(err)
		}
		stream := &nativeDispatchStream{ctx: t.Context()}
		state := &backupStagingSession{}
		handled, err := server.handleBackupStagingDelivery(stream, session, 1,
			&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupStagingInventory{
				BackupStagingInventory: &agentpb.BackupStagingInventory{},
			}}, state)
		if err != nil || !handled || len(stream.messages) != 1 {
			t.Fatalf("executor %d with unchanged credential failed startup: %v", registered.Generation, err)
		}
		plan := stream.messages[0].GetBackupStagingRecoveryPlan()
		planSHA, err := executionplan.BackupStagingRecoveryPlanSHA256(plan)
		if err != nil {
			t.Fatal(err)
		}
		ack := &agentpb.BackupStagingRecoveryAck{InventorySha256: plan.InventorySha256, AppliedPlanSha256: planSHA}
		handled, err = server.handleBackupStagingDelivery(stream, session, 1,
			&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupStagingRecoveryAck{
				BackupStagingRecoveryAck: ack,
			}}, state)
		if err != nil || !handled || !state.ready || len(stream.messages) != 2 {
			t.Fatalf("native staging acknowledgement failed: %v", err)
		}
		receipt := stream.messages[1].GetBackupStagingRecoveryAckReceipt()
		if handled, err := server.handleBackupStagingDelivery(stream, session, 1,
			&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupStagingRecoveryAck{
				BackupStagingRecoveryAck: proto.CloneOf(ack),
			}}, state); err != nil || !handled || len(stream.messages) != 3 ||
			!proto.Equal(stream.messages[2].GetBackupStagingRecoveryAckReceipt(), receipt) {
			t.Fatalf("lost acknowledgement did not replay its exact receipt: %v", err)
		}
		saved, found, err := repository.ReadBackupStagingDelivery(t.Context(), agentID)
		if err != nil || !found || saved.Record.HostID != registered.HostID ||
			saved.Record.AgentGeneration != registered.Generation || saved.Record.ProcessGeneration != process ||
			!proto.Equal(saved.Record.Ack, ack) {
			t.Fatalf("staging did not retain exact executor acknowledgement: %v", err)
		}
		if previousAck != nil {
			if _, err := repository.ApplyBackupStagingDelivery(t.Context(), previous, previousProcess, previousAck); err == nil {
				t.Fatal("replaced executor could acknowledge the current staging delivery")
			}
		}
		previous, previousProcess, previousAck = registered, process, ack
		session.Close()
	}
}
