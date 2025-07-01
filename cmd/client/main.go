// cmd/client/main.go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os" // Remplacement pour io/ioutil
	"bufio"
	"encoding/json"
	"os/signal"
	"strings"
	"syscall"

	"qwrap/internal/client/downloader"
	"qwrap/internal/client/orchestratorclient"
	"qwrap/pkg/kuik"
)

// customDestWriter is no longer needed as file handling is internal to kuik
// const alpnClientToAgent is also internal to kuik now

func main() {
	var (
		orchestratorAddr = flag.String("orchestrator", "localhost:7878", "Orchestrator address")
		fileID           = flag.String("file", "testfile.dat", "File ID to download")
		fileSize         = flag.Int64("size", 0, "Total size of the file (optional)")
		destPath         = flag.String("o", "downloaded_testfile.dat", "Destination path")
		insecure         = flag.Bool("insecure", true, "Skip TLS certificate verification")
		caFileOrch       = flag.String("ca-orch", "", "Path to CA certificate for Orchestrator")
		caFileAgent      = flag.String("ca-agent", "", "Path to CA certificate for Agents")
		logLevelStr      = flag.String("loglevel", "debug", "Log level (debug, info, warn, error)")
		concurrency      = flag.Int("concurrency", 10, "Number of concurrent download workers")
	)
	flag.Parse()

	if *fileID == "" {
		fmt.Fprintln(os.Stderr, "Error: File ID must be provided with -file")
		os.Exit(1)
	}

	var logLevel slog.Level
	switch strings.ToLower(*logLevelStr) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	handlerOptions := &slog.HandlerOptions{Level: logLevel, AddSource: true}
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, handlerOptions))
	slog.SetDefault(baseLogger)

	baseLogger.Info("qwrap client starting (kuik API)", "orchestrator", *orchestratorAddr, "file_id", *fileID, "destination", *destPath, "concurrency", *concurrency)

	mainCtx, mainCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer mainCancel()

	orchTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{orchestratorclient.QuicCommsALPN()},
	}
	if *caFileOrch != "" {
		loadCACertFromFile(orchTLSClientConf, *caFileOrch, baseLogger)
	}

	// Note: alpnClientToAgent is now an internal detail of the kuik implementation
	agentTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{"qwrap"}, // Formerly alpnClientToAgent
	}
	if *caFileAgent != "" {
		loadCACertFromFile(agentTLSClientConf, *caFileAgent, baseLogger)
	}

	// 1. Create a kuik Transport
	transport := &kuik.Transport{
		Logger:          baseLogger,
		OrchestratorTLS: orchTLSClientConf,
		AgentTLS:        agentTLSClientConf,
	}

	// 2. Create a kuik Config from flags
	kuikConfig := &kuik.Config{
		QwrapFileID:      *fileID,
		QwrapDestPath:    *destPath,
		QwrapFileSize:    *fileSize,
		QwrapConcurrency: *concurrency,
	}

	// 3. Dial the "connection"
	conn, err := transport.DialContext(mainCtx, *orchestratorAddr, kuikConfig)
	if err != nil {
		baseLogger.Error("Failed to dial connection via kuik", "error", err)
		os.Exit(1)
	}
	defer conn.CloseWithError(0, "closing")

	// 4. Open a "stream"
	stream, err := conn.OpenStreamSync(mainCtx)
	if err != nil {
		baseLogger.Error("Failed to open stream via kuik", "error", err)
		os.Exit(1)
	}
	defer stream.Close() // This closes the underlying file handle

	baseLogger.Info("Download started, reading progress from stream...")
	reader := bufio.NewReader(stream)
	var finalError error

readLoop:
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				finalError = err // Capture non-EOF errors
			}
			break readLoop
		}

		var progress downloader.ProgressInfo
		if jsonErr := json.Unmarshal(line, &progress); jsonErr != nil {
			baseLogger.Warn("Failed to parse progress update from stream", "error", jsonErr, "data", string(line))
			continue
		}
		printProgress(baseLogger, *fileID, progress, false)
	}

	if finalError != nil {
		baseLogger.Error("Download failed", "error", finalError)
		os.Exit(1)
	} else {
		baseLogger.Info("Download completed successfully!")
	}

	baseLogger.Info("All client tasks finished. Exiting.")
	os.Exit(0)
}

func printProgress(logger *slog.Logger, fileID string, p downloader.ProgressInfo, final bool) {
	percent := 0.0
	if p.TotalSizeBytes > 0 {
		percent = (float64(p.DownloadedSizeBytes) / float64(p.TotalSizeBytes)) * 100
	}
	status := "Progress"
	if final && p.FailedChunks > 0 {
		status = "Last progress (with failures)"
	} else if final {
		status = "Final progress"
	}

	logger.Info(status,
		"file", fileID,
		"progress", fmt.Sprintf("%.2f%%", percent),
		"chunks_completed", fmt.Sprintf("%d/%d", p.CompletedChunks, p.TotalChunks),
		"chunks_failed_perm", p.FailedChunks,
		"bytes", fmt.Sprintf("%s/%s", formatBytes(p.DownloadedSizeBytes), formatBytes(p.TotalSizeBytes)),
		"active_workers_approx", p.ActiveDownloads,
	)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func loadCACertFromFile(tlsConfig *tls.Config, caFile string, logger *slog.Logger) { // Renommé
	caCert, err := os.ReadFile(caFile) // Remplacé ioutil.ReadFile
	if err != nil {
		logger.Error("Failed to read CA certificate file, proceeding without it", "ca_file", caFile, "error", err)
		return
	}
	if tlsConfig.RootCAs == nil {
		pool, errSys := x509.SystemCertPool()
		if errSys != nil {
			logger.Warn("Failed to load system cert pool, creating new empty pool.", "error", errSys)
			pool = x509.NewCertPool()
		}
		tlsConfig.RootCAs = pool
	}
	if ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCert); !ok {
		logger.Error("Failed to append CA certificate to pool", "ca_file", caFile)
	} else {
		logger.Info("Successfully loaded CA certificate", "ca_file", caFile)
	}
}
