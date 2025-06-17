package datagen

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"qwrap/pkg/qwrappb"
)

// Generator orchestrates the data generation process.
type Generator struct {
	config     *Config
	logger     *slog.Logger
	sourceRoot string // The root directory of the input files
}

// NewGenerator creates a new data generator instance.
func NewGenerator(config *Config, logger *slog.Logger) (*Generator, error) {
	if config.NumAgents <= 0 {
		return nil, fmt.Errorf("number of agents must be positive")
	}
	if config.ChunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}
	return &Generator{config: config, logger: logger}, nil
}

// Run executes the data generation and distribution.
func (g *Generator) Run() error {
	g.logger.Info("Starting data generation process", "output_dir", g.config.OutputDir)

	// 1. Clean and prepare output directory
	if err := os.RemoveAll(g.config.OutputDir); err != nil {
		return fmt.Errorf("failed to clean output directory: %w", err)
	}
	if err := os.MkdirAll(g.config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	var allPortions []*qwrappb.FilePortionInfo
	var err error

	// 2. Decide whether to generate data or use existing files
	if g.config.InputPath != "" {
		g.logger.Info("Processing existing input", "path", g.config.InputPath)
		info, statErr := os.Stat(g.config.InputPath)
		if statErr != nil {
			return fmt.Errorf("could not stat input path %s: %w", g.config.InputPath, statErr)
		}
		if info.IsDir() {
			g.sourceRoot = g.config.InputPath
		} else {
			g.sourceRoot = filepath.Dir(g.config.InputPath)
		}
		allPortions, err = g.processInputPath(g.config.InputPath)
	} else {
		g.logger.Info("Generating new data", "size", g.config.GenerationMode.TotalSize)
		allPortions, err = g.generateAndDistribute()
	}

	if err != nil {
		return err
	}

	// 3. Distribute portions among agents
	agentPortions := g.distributePortions(allPortions)

	// 4. Write the agent data and inventory files
	return g.writeAgentData(agentPortions)
}

// processInputPath checks if the path is a file or directory and processes it.
func (g *Generator) processInputPath(path string) ([]*qwrappb.FilePortionInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("could not stat input path %s: %w", path, err)
	}

	if info.IsDir() {
		return g.processDirectory(path)
	} else {
		return g.processFile(path)
	}
}

// processDirectory walks a directory, processes each file, and returns a combined list of portions.
func (g *Generator) processDirectory(dirPath string) ([]*qwrappb.FilePortionInfo, error) {
	var allPortions []*qwrappb.FilePortionInfo
	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			g.logger.Info("Processing file in directory", "path", path)
			filePortions, procErr := g.processFile(path)
			if procErr != nil {
				return fmt.Errorf("failed to process file %s: %w", path, procErr)
			}
			allPortions = append(allPortions, filePortions...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking directory %s: %w", dirPath, err)
	}
	return allPortions, nil
}

// processFile reads a single file and creates a list of its portions.
func (g *Generator) processFile(filePath string) ([]*qwrappb.FilePortionInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file %s: %w", filePath, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat input file %s: %w", filePath, err)
	}

	relPath, err := filepath.Rel(g.sourceRoot, filePath)
	if err != nil {
		return nil, fmt.Errorf("could not find relative path for %s from root %s: %w", filePath, g.sourceRoot, err)
	}

	totalSize := info.Size()
	if totalSize == 0 {
		g.logger.Info("Skipping empty file", "path", filePath)
		return nil, nil // No portions for empty files
	}

	numChunks := totalSize / g.config.ChunkSize
	if totalSize%g.config.ChunkSize != 0 {
		numChunks++
	}

	filePortions := make([]*qwrappb.FilePortionInfo, 0, numChunks)
	for i := int64(0); i < numChunks; i++ {
		offset := i * g.config.ChunkSize
		portion := &qwrappb.FilePortionInfo{
			GlobalFileId:    relPath,
			Offset:          offset,
			NumChunks:       1,
			ChunkSize:       g.config.ChunkSize,
			ChunkIndexStart: uint64(i),
			ChunkIndexEnd:   uint64(i),
			PathOnDisk:      fmt.Sprintf("%s.part%d", relPath, i),
		}
		filePortions = append(filePortions, portion)
	}

	return filePortions, nil
}

// generateAndDistribute creates new data and returns a list of its portions.
func (g *Generator) generateAndDistribute() ([]*qwrappb.FilePortionInfo, error) {
	fileName := "generated_file.dat"
	totalSize := g.config.GenerationMode.TotalSize
	numChunks := totalSize / g.config.ChunkSize
	if totalSize%g.config.ChunkSize != 0 {
		numChunks++
	}

	allPortions := make([]*qwrappb.FilePortionInfo, 0, numChunks)
	for i := int64(0); i < numChunks; i++ {
		offset := i * g.config.ChunkSize
		portion := &qwrappb.FilePortionInfo{
			GlobalFileId:    fileName,
			Offset:          offset,
			NumChunks:       1,
			ChunkSize:       g.config.ChunkSize,
			ChunkIndexStart: uint64(i),
			ChunkIndexEnd:   uint64(i),
			PathOnDisk:      fmt.Sprintf("part_%d", i),
		}
		allPortions = append(allPortions, portion)
	}

	return allPortions, nil
}

// distributePortions assigns a list of portions to agents.
func (g *Generator) distributePortions(portions []*qwrappb.FilePortionInfo) map[string][]*qwrappb.FilePortionInfo {
	distribution := make(map[string][]*qwrappb.FilePortionInfo)
	for i, p := range portions {
		agentIndex := i % g.config.NumAgents
		agentID := fmt.Sprintf("agent%03d", agentIndex+1)
		distribution[agentID] = append(distribution[agentID], p)
	}
	return distribution
}

// InventoryData wraps the portions and includes the total file size.
type InventoryData struct {
	Portions  []*qwrappb.FilePortionInfo `json:"portions"`
	TotalSize int64                     `json:"file_size"`
}

// writeAgentData creates the directory structure, data files, and inventory for each agent.
func (g *Generator) writeAgentData(agentPortions map[string][]*qwrappb.FilePortionInfo) error {
	// Cache for open source file handles
	sourceFileCache := make(map[string]*os.File)
	defer func() {
		for _, f := range sourceFileCache {
			f.Close()
		}
	}()

	for agentID, portions := range agentPortions {
		agentDir := filepath.Join(g.config.OutputDir, agentID)
		if err := os.MkdirAll(agentDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for agent %s: %w", agentID, err)
		}

		// Group portions by file ID for inventory
		inventory := make(map[string]InventoryData)
		for _, p := range portions {
			data := inventory[p.GlobalFileId]
			data.Portions = append(data.Portions, p)
			inventory[p.GlobalFileId] = data
		}

		// Get total file size for each file in the inventory
		for fileID, data := range inventory {
			var totalSize int64
			if g.sourceRoot != "" {
				sourcePath := filepath.Join(g.sourceRoot, fileID)
				info, err := os.Stat(sourcePath)
				if err != nil {
					return fmt.Errorf("failed to stat source file for size %s: %w", sourcePath, err)
				}
				totalSize = info.Size()
			} else {
				totalSize = g.config.GenerationMode.TotalSize
			}
			data.TotalSize = totalSize
			inventory[fileID] = data
		}

		for _, portion := range portions {
			// Create subdirectory for the file if it doesn't exist
			portionDir := filepath.Dir(portion.PathOnDisk)
			if portionDir != "." {
				if err := os.MkdirAll(filepath.Join(agentDir, portionDir), 0755); err != nil {
					return fmt.Errorf("failed to create sub-directory for portion: %w", err)
				}
			}

			filePath := filepath.Join(agentDir, portion.PathOnDisk)
			destFile, err := os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create data file %s: %w", filePath, err)
			}

			bytesToWrite := portion.ChunkSize
			var dataReader io.Reader

			if g.sourceRoot != "" { // We are processing existing files
				sourcePath := filepath.Join(g.sourceRoot, portion.GlobalFileId)
				sourceFile, ok := sourceFileCache[sourcePath]
				if !ok {
					sourceFile, err = os.Open(sourcePath)
					if err != nil {
						destFile.Close()
						return fmt.Errorf("failed to open source file %s: %w", sourcePath, err)
					}
					sourceFileCache[sourcePath] = sourceFile
				}

				info, _ := sourceFile.Stat()
				remaining := info.Size() - portion.Offset
				if remaining < bytesToWrite {
					bytesToWrite = remaining
				}
				_, err := sourceFile.Seek(portion.Offset, io.SeekStart)
				if err != nil {
					destFile.Close()
					return fmt.Errorf("failed to seek in source file: %w", err)
				}
				dataReader = sourceFile
			} else { // We are generating new data
				remaining := g.config.GenerationMode.TotalSize - portion.Offset
				if remaining < bytesToWrite {
					bytesToWrite = remaining
				}
				if g.config.GenerationMode.Readable {
					dataReader = newPatternReader(g.config.GenerationMode.Pattern)
				} else {
					dataReader = rand.Reader
				}
			}

			if _, err := io.CopyN(destFile, dataReader, bytesToWrite); err != nil {
				destFile.Close()
				return fmt.Errorf("failed to write data to %s: %w", filePath, err)
			}
			destFile.Close()
		}

		// Write inventory.json
		inventoryPath := filepath.Join(agentDir, "inventory.json")
		invFile, err := os.Create(inventoryPath)
		if err != nil {
			return fmt.Errorf("failed to create inventory.json for agent %s: %w", agentID, err)
		}

		encoder := json.NewEncoder(invFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(inventory); err != nil {
			invFile.Close()
			return fmt.Errorf("failed to write inventory.json for agent %s: %w", agentID, err)
		}
		invFile.Close()
		g.logger.Info("Successfully wrote data and inventory", "agent_id", agentID, "inventory_path", inventoryPath)
	}
	return nil
}

// patternReader is an infinite reader that repeats a given pattern.
type patternReader struct {
	pattern []byte
	pos     int
}

func newPatternReader(pattern string) *patternReader {
	return &patternReader{pattern: []byte(pattern)}
}

func (r *patternReader) Read(p []byte) (n int, err error) {
	if len(r.pattern) == 0 {
		return 0, io.EOF
	}
	for i := 0; i < len(p); i++ {
		p[i] = r.pattern[r.pos]
		r.pos = (r.pos + 1) % len(r.pattern)
	}
	return len(p), nil
}
