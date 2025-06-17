package chunkserver

import (
	"crypto/tls"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewChunkServer_LoadInventory_Success(t *testing.T) {
	testDir, err := os.MkdirTemp("", "chunk-server-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// 1. Create a dummy inventory.json
	inventoryPath := filepath.Join(testDir, "inventory.json")

	inventoryJSON := []byte(`{
		"file1.dat": {
			"portions": [
				{
					"global_file_id": "file1.dat",
					"path_on_disk": "file1.dat.part_0",
					"chunk_index_start": 0,
					"num_chunks": 10,
					"chunk_size": 1024,
					"offset": 0
				}
			],
			"file_size": 10240
		}
	}`)
	err = os.WriteFile(inventoryPath, inventoryJSON, 0644)
	require.NoError(t, err)

	// 2. Create the ChunkServer
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := ChunkServerConfig{
		ListenAddr:  ":0", // Use a random port for testing
		BaseDataDir: testDir,
		AgentId:     "test-agent",
		TLSConfig:   &tls.Config{ /* Dummy config for test */ },
		Logger:      logger,
	}

	server, err := NewChunkServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)

	// 3. Verify the inventory was loaded correctly
	impl := server.(*chunkServerImpl)
	data, ok := impl.inventory.GetFileMetadata("file1.dat")
	require.True(t, ok)
	portions := data.Portions
	require.Len(t, portions, 1)
	assert.Equal(t, "file1.dat.part_0", portions[0].PathOnDisk)
	assert.Equal(t, uint64(0), portions[0].ChunkIndexStart)
}

func TestNewChunkServer_NoInventoryFile(t *testing.T) {
	testDir, err := os.MkdirTemp("", "chunk-server-no-inventory-*")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// 1. Setup config without creating an inventory.json file
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := ChunkServerConfig{
		ListenAddr:  ":0",
		BaseDataDir: testDir,
		AgentId:     "test-agent-no-inventory",
		TLSConfig:   &tls.Config{},
		Logger:      logger,
	}

	// 2. Create the ChunkServer
	server, err := NewChunkServer(config)

	// 3. Verify server is created without error and inventory is empty
	require.NoError(t, err)
	require.NotNil(t, server)

	impl := server.(*chunkServerImpl)
	assert.NotNil(t, impl.inventory)
	// Check that the internal map is empty
	impl.inventory.mu.RLock()
	defer impl.inventory.mu.RUnlock()
	assert.Empty(t, impl.inventory.fileMetadata)
}

func TestNewChunkServer_MalformedInventory(t *testing.T) {
	testDir, err := os.MkdirTemp("", "chunk-server-malformed-*")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// 1. Create a malformed inventory.json
	inventoryPath := filepath.Join(testDir, "inventory.json")
	malformedJSON := []byte(`{"file1.dat": [{"global_file_id": "file1.dat",]}`)
	err = os.WriteFile(inventoryPath, malformedJSON, 0644)
	require.NoError(t, err)

	// 2. Setup config
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := ChunkServerConfig{
		ListenAddr:  ":0",
		BaseDataDir: testDir,
		AgentId:     "test-agent-malformed",
		TLSConfig:   &tls.Config{},
		Logger:      logger,
	}

	// 3. Attempt to create the ChunkServer and expect an error
	_, err = NewChunkServer(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal inventory JSON")
}
