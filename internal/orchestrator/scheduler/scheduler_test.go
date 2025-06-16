package scheduler

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"qwrap/pkg/qwrappb"
)

// newTestScheduler creates a new scheduler instance for testing.
func newTestScheduler(t *testing.T) Scheduler {
	cfg := SchedulerConfig{
		Logger:                slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		AgentHeartbeatTimeout: 2 * time.Second,
	}
	// NewScheduler now returns the interface type and doesn't return an error.
	s := NewScheduler(cfg)
	require.NotNil(t, s)
	return s
}

func TestCreateTransferPlan_PreferAgentWithPortion(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	// Register two agents using AgentRegistrationRequest
	regReq1 := &qwrappb.AgentRegistrationRequest{AgentId: "agent1", AgentAddress: "host1:1234"}
	_, err := s.RegisterAgent(ctx, regReq1)
	require.NoError(t, err)

	regReq2 := &qwrappb.AgentRegistrationRequest{AgentId: "agent2", AgentAddress: "host2:1234"}
	_, err = s.RegisterAgent(ctx, regReq2)
	require.NoError(t, err)

	// Setup transfer request
	fileMeta := &qwrappb.FileMetadata{FileId: "file1", TotalSize: 2 * 1024 * 1024, ChunkSize: 1024 * 1024} // 2 MiB file, 1MiB chunks
	transferReq := &qwrappb.TransferRequest{
		RequestId:       "req1",
		FilesToTransfer: []*qwrappb.FileMetadata{fileMeta},
	}

	// Agent 2 has chunk 0
	agentMetadata := []*qwrappb.GetFileMetadataResponse{
		{AgentId: "agent1", Found: true, GlobalFileMetadata: fileMeta, AvailablePortions: []*qwrappb.FilePortionInfo{}},
		{AgentId: "agent2", Found: true, GlobalFileMetadata: fileMeta, AvailablePortions: []*qwrappb.FilePortionInfo{{ChunkIndexStart: 0, ChunkIndexEnd: 0}}},
	}

	// Create the plan
	plan, err := s.CreateTransferPlan(ctx, "plan1", transferReq, agentMetadata)

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "plan1", plan.PlanId)
	// With 2MiB total size and 1MiB chunk size, we expect 2 chunks.
	require.Len(t, plan.ChunkAssignments, 2)

	// Chunk 0 should be assigned to agent2
	assignment0 := plan.ChunkAssignments[0]
	assert.Equal(t, uint64(0), assignment0.ChunkInfo.ChunkId)
	// The ChunkAssignment message has a single AgentId field.
	assert.Equal(t, "agent2", assignment0.AgentId)

	// Chunk 1 should be assigned to the other agent (round-robin starts with agent1)
	assignment1 := plan.ChunkAssignments[1]
	assert.Equal(t, uint64(1), assignment1.ChunkInfo.ChunkId)
	assert.Equal(t, "agent1", assignment1.AgentId)
}

func TestCreateTransferPlan_NoPortionsAvailable(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	// Register two agents
	regReq1 := &qwrappb.AgentRegistrationRequest{AgentId: "agent1", AgentAddress: "host1:1234"}
	_, err := s.RegisterAgent(ctx, regReq1)
	require.NoError(t, err)

	regReq2 := &qwrappb.AgentRegistrationRequest{AgentId: "agent2", AgentAddress: "host2:1234"}
	_, err = s.RegisterAgent(ctx, regReq2)
	require.NoError(t, err)

	// Setup transfer request
	fileMeta := &qwrappb.FileMetadata{FileId: "file1", TotalSize: 2 * 1024 * 1024, ChunkSize: 1024 * 1024} // 2 MiB file
	transferReq := &qwrappb.TransferRequest{
		RequestId:       "req1",
		FilesToTransfer: []*qwrappb.FileMetadata{fileMeta},
	}

	// No agent has any portion
	agentMetadata := []*qwrappb.GetFileMetadataResponse{
		{AgentId: "agent1", Found: true, GlobalFileMetadata: fileMeta, AvailablePortions: []*qwrappb.FilePortionInfo{}},
		{AgentId: "agent2", Found: true, GlobalFileMetadata: fileMeta, AvailablePortions: []*qwrappb.FilePortionInfo{}},
	}

	// Create the plan
	plan, err := s.CreateTransferPlan(ctx, "plan1", transferReq, agentMetadata)

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Len(t, plan.ChunkAssignments, 2)

	// Assignments should be round-robin (agent1, then agent2)
	assert.Equal(t, "agent1", plan.ChunkAssignments[0].AgentId)
	assert.Equal(t, "agent2", plan.ChunkAssignments[1].AgentId)
}
