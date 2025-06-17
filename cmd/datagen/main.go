package main

import (
	"flag"
	"log/slog"
	"os"

	"qwrap/internal/datagen"
)

func main() {
	// --- Configuration & Flags ---
	configPath := flag.String("config", "", "Path to a JSON configuration file.")

	// Flags to override config file settings
	inputPath := flag.String("input", "", "Path to an input file or directory. Overrides config.")
	outputDir := flag.String("output-dir", "", "Output directory for agent data. Overrides config.")
	numAgents := flag.Int("agents", 0, "Number of agents. Overrides config.")
	chunkSize := flag.Int64("chunk-size", 0, "Chunk size in bytes. Overrides config.")
	genSize := flag.Int64("gen-size", 0, "Total size of generated data in bytes. Overrides config.")

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Load config from file or use defaults
	var config *datagen.Config
	var err error
	if *configPath != "" {
		config, err = datagen.LoadConfig(*configPath)
		if err != nil {
			logger.Error("Failed to load configuration file", "path", *configPath, "error", err)
			os.Exit(1)
		}
		logger.Info("Loaded configuration from file", "path", *configPath)
	} else {
		config = datagen.DefaultConfig()
		logger.Info("Using default configuration.")
	}

	// Override config with flags if they were provided
	if *inputPath != "" {
		config.InputPath = *inputPath
	}
	if *outputDir != "" {
		config.OutputDir = *outputDir
	}
	if *numAgents > 0 {
		config.NumAgents = *numAgents
	}
	if *chunkSize > 0 {
		config.ChunkSize = *chunkSize
	}
	if *genSize > 0 {
		config.GenerationMode.TotalSize = *genSize
	}

	// --- Execution ---
	generator, err := datagen.NewGenerator(config, logger)
	if err != nil {
		logger.Error("Failed to initialize data generator", "error", err)
		os.Exit(1)
	}

	if err := generator.Run(); err != nil {
		logger.Error("Failed to run data generation", "error", err)
		os.Exit(1)
	}

	logger.Info("Data generation completed successfully.")
}
