package datagen

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator_Run_GenerateNew(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "datagen-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := DefaultConfig()
	cfg.OutputDir = tempDir
	cfg.NumAgents = 2
	cfg.GenerationMode.TotalSize = 2 * 1024 * 1024 // 2MB
	cfg.ChunkSize = 1 * 1024 * 1024      // 1MB

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	gen, err := NewGenerator(cfg, logger)
	require.NoError(t, err)

	err = gen.Run()
	require.NoError(t, err)

	// Verify agent directories
	agent1Dir := filepath.Join(tempDir, "agent001")
	agent2Dir := filepath.Join(tempDir, "agent002")
	assert.DirExists(t, agent1Dir)
	assert.DirExists(t, agent2Dir)

	// Verify agent 1 files
	agent1Files, err := os.ReadDir(agent1Dir)
	require.NoError(t, err)
	assert.Len(t, agent1Files, 2) // inventory.json + 1 data part
	assert.FileExists(t, filepath.Join(agent1Dir, "inventory.json"))
	assert.FileExists(t, filepath.Join(agent1Dir, "part_0"))

	// Verify agent 2 files
	agent2Files, err := os.ReadDir(agent2Dir)
	require.NoError(t, err)
	assert.Len(t, agent2Files, 2) // inventory.json + 1 data part
	assert.FileExists(t, filepath.Join(agent2Dir, "inventory.json"))
	assert.FileExists(t, filepath.Join(agent2Dir, "part_1"))
}

func TestGenerator_Run_ExistingFile(t *testing.T) {
	// 1. Setup temp directory and input file
	tempDir, err := os.MkdirTemp("", "datagen-test-file-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	inputFile, err := os.CreateTemp(tempDir, "input-*.dat")
	require.NoError(t, err)

	// Write 2.5MB of data
	fileSize := int64(2.5 * 1024 * 1024)
	err = inputFile.Truncate(fileSize)
	require.NoError(t, err)
	inputFileName := inputFile.Name()
	inputFile.Close() // Close the file so the generator can open it

	// 2. Configure Generator
	outputDir := filepath.Join(tempDir, "output")
	cfg := DefaultConfig()
	cfg.InputPath = inputFileName
	cfg.OutputDir = outputDir
	cfg.NumAgents = 2
	cfg.ChunkSize = 1 * 1024 * 1024 // 1MB

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	gen, err := NewGenerator(cfg, logger)
	require.NoError(t, err)

	// 3. Run Generator
	err = gen.Run()
	require.NoError(t, err)

	// 4. Verify output
	agent1Dir := filepath.Join(outputDir, "agent001")
	agent2Dir := filepath.Join(outputDir, "agent002")
	assert.DirExists(t, agent1Dir)
	assert.DirExists(t, agent2Dir)

	inputBaseName := filepath.Base(inputFileName)

	// Agent 1 should have part 0 and part 2
	assert.FileExists(t, filepath.Join(agent1Dir, "inventory.json"))
	part0Path := filepath.Join(agent1Dir, fmt.Sprintf("%s.part0", inputBaseName))
	part2Path := filepath.Join(agent1Dir, fmt.Sprintf("%s.part2", inputBaseName))
	assert.FileExists(t, part0Path)
	assert.FileExists(t, part2Path)

	part0Info, err := os.Stat(part0Path)
	require.NoError(t, err)
	assert.Equal(t, cfg.ChunkSize, part0Info.Size())

	part2Info, err := os.Stat(part2Path)
	require.NoError(t, err)
	assert.Equal(t, int64(0.5*1024*1024), part2Info.Size())

	// Agent 2 should have part 1
	assert.FileExists(t, filepath.Join(agent2Dir, "inventory.json"))
	part1Path := filepath.Join(agent2Dir, fmt.Sprintf("%s.part1", inputBaseName))
	assert.FileExists(t, part1Path)

	part1Info, err := os.Stat(part1Path)
	require.NoError(t, err)
	assert.Equal(t, cfg.ChunkSize, part1Info.Size())
}

func TestGenerator_Run_Directory(t *testing.T) {
	// 1. Setup temp directory and input directory structure
	tempDir, err := os.MkdirTemp("", "datagen-test-dir-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	inputDir := filepath.Join(tempDir, "input")
	subDir := filepath.Join(inputDir, "subdir")
	err = os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	// Create test files
	err = os.WriteFile(filepath.Join(inputDir, "file1.txt"), []byte("file one content"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("file two content"), 0644)
	require.NoError(t, err)

	// 2. Configure Generator
	outputDir := filepath.Join(tempDir, "output")
	cfg := DefaultConfig()
	cfg.InputPath = inputDir
	cfg.OutputDir = outputDir
	cfg.NumAgents = 2
	cfg.ChunkSize = 1024 // 1KB chunk size, larger than files

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	gen, err := NewGenerator(cfg, logger)
	require.NoError(t, err)

	// 3. Run Generator
	err = gen.Run()
	require.NoError(t, err)

	// 4. Verify output
	agent1Dir := filepath.Join(outputDir, "agent001")
	agent2Dir := filepath.Join(outputDir, "agent002")
	assert.DirExists(t, agent1Dir)
	assert.DirExists(t, agent2Dir)

	// Agent 1 should have file1.txt
	assert.FileExists(t, filepath.Join(agent1Dir, "inventory.json"))
	assert.FileExists(t, filepath.Join(agent1Dir, "file1.txt.part0"))

	// Agent 2 should have subdir/file2.txt
	assert.FileExists(t, filepath.Join(agent2Dir, "inventory.json"))
	assert.DirExists(t, filepath.Join(agent2Dir, "subdir"))
	assert.FileExists(t, filepath.Join(agent2Dir, "subdir", "file2.txt.part0"))
}
