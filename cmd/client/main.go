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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"qwrap/internal/client/downloader"
	connectionmanager "qwrap/internal/client/manager"
	"qwrap/internal/client/orchestratorclient"
	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb"
)

// ... (const alpnClientToAgent, customDestWriter) ... (inchangés)
const alpnClientToAgent = "qwrap"

type customDestWriter struct {
	*os.File
}

func (cdw *customDestWriter) Sync() error { return cdw.File.Sync() }

func main() {
	// ... (flags inchangés) ...
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
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, handlerOptions)) // Renommé en baseLogger pour éviter conflit
	slog.SetDefault(baseLogger)                                            // Important: définir le logger par défaut AVANT de créer les factories qui pourraient l'utiliser

	baseLogger.Info("qwrap client starting (new pipeline arch)", "orchestrator", *orchestratorAddr, "file_id", *fileID, "destination", *destPath, "concurrency", *concurrency)

	mainCtx, mainCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer mainCancel()

	orchTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{orchestratorclient.QuicCommsALPN()},
	}
	if *caFileOrch != "" {
		loadCACertFromFile(orchTLSClientConf, *caFileOrch, baseLogger)
	}

	agentTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{alpnClientToAgent},
	}
	if *caFileAgent != "" {
		loadCACertFromFile(agentTLSClientConf, *caFileAgent, baseLogger)
	}

	connMgrConfig := connectionmanager.Config{TLSClientConfig: agentTLSClientConf, Logger: baseLogger}
	connMgr := connectionmanager.NewConnectionManager(connMgrConfig)

	// Définir les factories de framing ici
	writerFactory := func(w io.Writer, l *slog.Logger) framing.Writer {
		// Utiliser le logger 'l' passé par le composant appelant (worker, orchComms)
		// ou fallback sur baseLogger si 'l' est nil (ne devrait pas arriver si bien utilisé).
		if l == nil {
			l = baseLogger
		}
		return framing.NewMessageWriter(w, l.With("framing_role", "writer"))
	}
	readerFactory := func(r io.Reader, l *slog.Logger) framing.Reader {
		if l == nil {
			l = baseLogger
		}
		return framing.NewMessageReader(r, l.With("framing_role", "reader"))
	}

	orchComms, err := orchestratorclient.NewQuicComms(*orchestratorAddr, orchTLSClientConf, baseLogger, writerFactory, readerFactory)
	if err != nil {
		baseLogger.Error("Failed to create orchestrator comms", "error", err)
		os.Exit(1)
	}

	// Le Downloader prend maintenant les factories
	dl := downloader.NewDownloader(connMgr, orchComms, baseLogger, *concurrency, writerFactory, readerFactory)

	clientReqId := fmt.Sprintf("client-req-%d", time.Now().UnixNano())
	transferReq := &qwrappb.TransferRequest{
		RequestId: clientReqId,
		FilesToTransfer: []*qwrappb.FileMetadata{
			{FileId: *fileID, TotalSize: *fileSize},
		},
		Options: &qwrappb.TransferOptions{VerifyChunkChecksums: true},
	}

	destFileOS, err := os.OpenFile(*destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		baseLogger.Error("Failed to open destination file", "path", *destPath, "error", err)
		os.Exit(1)
	}
	destFileWriter := &customDestWriter{File: destFileOS}

	baseLogger.Info("Initiating download...", "file_id", *fileID)
	progressChan, finalErrorChan := dl.Download(mainCtx, transferReq, destFileWriter)

	var lastProgress downloader.ProgressInfo
	// Utilisation de la constante exportée depuis le package downloader
	ticker := time.NewTicker(downloader.ProgressReportInterval)
	defer ticker.Stop()
	progressBarUpdate := false
	var downloadErr error
	var exitCode int

	shutdownCompleted := make(chan struct{})
	go func() {
		defer close(shutdownCompleted)
		defer destFileWriter.Close()
		defer connMgr.CloseAll()

		downloadErr = <-finalErrorChan
		mainCancel()

		if downloadErr != nil && !errors.Is(downloadErr, context.Canceled) {
			baseLogger.Error("Download failed", "file_id", *fileID, "error", downloadErr)
			exitCode = 1
		} else if errors.Is(downloadErr, context.Canceled) {
			baseLogger.Info("Download was cancelled.", "file_id", *fileID)
		} else {
			baseLogger.Info("Download completed successfully!", "file_id", *fileID, "destination", *destPath)
			exitCode = 0
		}

		shutdownTimeout := 10 * time.Second
		baseLogger.Info("Attempting to shutdown downloader...", "timeout", shutdownTimeout)
		if err := dl.Shutdown(shutdownTimeout); err != nil {
			baseLogger.Error("Downloader shutdown failed", "error", err)
			if exitCode == 0 {
				exitCode = 1
			}
		} else {
			baseLogger.Info("Downloader shutdown successful.")
		}
	}()

consoleLoop:
	for {
		select {
		case p, ok := <-progressChan:
			if !ok {
				progressChan = nil
				continue
			}
			lastProgress = p
			progressBarUpdate = true
		case <-ticker.C:
			if progressBarUpdate && progressChan != nil {
				printProgress(baseLogger, *fileID, lastProgress, false)
				progressBarUpdate = false
			}
		case <-mainCtx.Done():
			baseLogger.Info("Client main loop received context done signal.")
			break consoleLoop
		}
	}

	baseLogger.Info("Waiting for shutdown tasks to complete...")
	<-shutdownCompleted
	baseLogger.Info("All client tasks finished. Exiting.", "exit_code", exitCode)
	os.Exit(exitCode)
}

func printProgress(logger *slog.Logger, fileID string, p downloader.ProgressInfo, final bool) {
	// ... (inchangé)
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

func formatBytes(b int64) string { // ... (inchangé)
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
