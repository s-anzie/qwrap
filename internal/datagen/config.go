package datagen

import (
	"encoding/json"
	"os"
)

// Config holds all the configuration for the data generation tool.
type Config struct {
	// InputPath is the path to a single file or a directory to be split among agents.
	// If empty, new data will be generated.
	InputPath string `json:"input_path"`

	// OutputDir is the base directory where agent data and metadata will be created.
	// It will contain subdirectories like 'agent001', 'agent002', etc.
	OutputDir string `json:"output_dir"`

	// NumAgents is the number of agents to distribute the data among.
	NumAgents int `json:"num_agents"`

	// GenerationMode settings are used when InputPath is empty.
	GenerationMode GenerationConfig `json:"generation_mode"`

	// DistributionStrategy defines how to split the data.
	// Can be "round-robin-chunks" or "contiguous-blocks".
	DistributionStrategy string `json:"distribution_strategy"`

	// ChunkSize is the size of each chunk in bytes.
	ChunkSize int64 `json:"chunk_size"`
}

// GenerationConfig specifies how to generate new data.
type GenerationConfig struct {
	// TotalSize is the total size of the data to generate in bytes.
	TotalSize int64 `json:"total_size"`

	// Readable defines if the generated data should be human-readable text.
	// If false, it will be random binary data.
	Readable bool `json:"readable"`

	// Pattern is used when Readable is true. It's a repeating string pattern.
	// Example: "The quick brown fox jumps over the lazy dog. "
	Pattern string `json:"pattern"`
}

// LoadConfig reads a configuration file from the given path.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		InputPath:    "",
		OutputDir:    "agent_data",
		NumAgents:    2,
		ChunkSize:    1 * 1024 * 1024, // 1 MiB
		DistributionStrategy: "contiguous-blocks",
		GenerationMode: GenerationConfig{
			TotalSize: 10 * 1024 * 1024, // 10 MiB
			Readable:  true,
			Pattern:   "This is a sample text pattern for generated data. ",
		},
	}
}
