// cmd/client/main.go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509" // Pour charger les CA système ou un CA personnalisé
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil" // Pour ioutil.ReadFile, bien que os.ReadFile soit préféré en Go 1.16+
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"qwrap/internal/client/downloader"
	connectionmanager "qwrap/internal/client/manager" // Renommé en connectionmanager dans les fichiers
	"qwrap/internal/client/orchestratorclient"
	"qwrap/internal/framing"

	"qwrap/pkg/qwrappb"
)

// ALPN constants (pourraient être dans un package partagé `common` ou `protocol`)
// const alpnClientToOrchestrator = "qwrap-orchestrator" // Défini dans orchestratorclient
const alpnClientToAgent = "qwrap"

func main() {
	var (
		orchestratorAddr = flag.String("orchestrator", "localhost:7878", "Orchestrator address")
		fileID           = flag.String("file", "testfile.dat", "File ID to download (must exist on an agent known to orchestrator)")
		fileSize         = flag.Int64("size", 0, "Total size of the file in bytes (required if orchestrator can't determine it)")
		destPath         = flag.String("o", "downloaded_testfile.dat", "Destination path for the downloaded file")
		insecure         = flag.Bool("insecure", true, "Skip TLS certificate verification (FOR TESTING AGAINST SELF-SIGNED CERTS)")
		caFileOrch       = flag.String("ca-orch", "", "Path to CA certificate file for Orchestrator (if not using system CAs or insecure)")
		caFileAgent     = flag.String("ca-agent", "", "Path to CA certificate file for Agents (if not using system CAs or insecure)")
		logLevelStr     = flag.String("loglevel", "debug", "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	if *fileID == "" {
		slog.Error("File ID must be provided with -file")
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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel, AddSource: true}))
	slog.SetDefault(logger)

	logger.Info("qwrap client starting", "orchestrator", *orchestratorAddr, "file_id", *fileID, "destination", *destPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down client...", "signal", sig)
		cancel()
	}()

	// TLS Config pour la connexion à l'Orchestrateur
	orchTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{orchestratorclient.QuicCommsALPN()}, // Utiliser la constante exportée
	}
	if *caFileOrch != "" {
		if err := loadCACert(orchTLSClientConf, *caFileOrch, logger); err != nil {
			logger.Error("Failed to load CA for orchestrator, proceeding with system CAs or insecure", "ca_file", *caFileOrch, "error", err)
		}
	}

	// TLS Config pour la connexion aux Agents (peut être la même ou différente)
	agentTLSClientConf := &tls.Config{
		InsecureSkipVerify: *insecure, // Pourrait être un flag séparé `insecure-agent`
		NextProtos:         []string{alpnClientToAgent},
	}
	if *caFileAgent != "" {
		if err := loadCACert(agentTLSClientConf, *caFileAgent, logger); err != nil {
			logger.Error("Failed to load CA for agents, proceeding with system CAs or insecure", "ca_file", *caFileAgent, "error", err)
		}
	}

	// 1. Créer ConnectionManager pour les connexions aux Agents
	connMgrConfig := connectionmanager.Config{ // Assurez-vous que le nom du package est correct
		TLSClientConfig: agentTLSClientConf, // Utiliser la config TLS spécifique aux agents
		Logger:          logger,
		MaxConnections:  20, // Exemple
		DialTimeout:     5 * time.Second,
	}
	connMgr := connectionmanager.NewConnectionManager(connMgrConfig)
	defer connMgr.CloseAll()

	// 2. Définir les Factories de framing
	writerFactory := func(w io.Writer, l *slog.Logger) framing.Writer {
		if l == nil {
			l = logger
		}
		return framing.NewMessageWriter(w, l.With("role", "writer"))
	}
	readerFactory := func(r io.Reader, l *slog.Logger) framing.Reader {
		if l == nil {
			l = logger
		}
		return framing.NewMessageReader(r, l.With("role", "reader"))
	}

	// 3. Créer OrchestratorComms (en utilisant l'implémentation QUIC)
	orchComms, err := orchestratorclient.NewQuicComms(*orchestratorAddr, orchTLSClientConf, logger, writerFactory, readerFactory)
	if err != nil {
		logger.Error("Failed to create orchestrator comms", "error", err)
		os.Exit(1)
	}

	connectCtx, connectCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := orchComms.Connect(connectCtx, *orchestratorAddr); err != nil { // Passer l'adresse ici
		logger.Error("Failed to connect to orchestrator", "address", *orchestratorAddr, "error", err)
		connectCancel()
		os.Exit(1)
	}
	connectCancel()
	defer orchComms.Close()

	// 4. Créer le Downloader
	// Wrap writerFactory and readerFactory to match the expected signature
	dl := downloader.NewDownloader(connMgr, orchComms, logger, 10, writerFactory, readerFactory)

	// 5. Préparer la TransferRequest
	clientReqId := fmt.Sprintf("client-req-%d", time.Now().UnixNano())
	transferReq := &qwrappb.TransferRequest{
		RequestId: clientReqId,
		FilesToTransfer: []*qwrappb.FileMetadata{
			{
				FileId:    *fileID,
				TotalSize: *fileSize, // Pass the file size if provided
			},
		},
		Options: &qwrappb.TransferOptions{VerifyChunkChecksums: true, Priority: 0},
	}

	if *fileSize > 0 {
		logger.Info("Client prepared TransferRequest with FileID and FileSize", "file_id", *fileID, "file_size", *fileSize, "request_id", clientReqId)
	} else {
		logger.Info("Client prepared TransferRequest, FileID only", "file_id", *fileID, "request_id", clientReqId)
	}

	// 6. Démarrer le téléchargement
	logger.Info("Initiating download process...", "file_id", *fileID)
	progressChan, finalErrorChan := dl.Download(ctx, transferReq, *destPath)

	// 7. Gérer la progression et le résultat final
	var lastProgress downloader.ProgressInfo
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	progressBarUpdate := false

	for {
		select {
		case p, ok := <-progressChan:
			if !ok {
				progressChan = nil
				continue
			} // Channel fermé
			lastProgress = p
			progressBarUpdate = true
		case err := <-finalErrorChan:
			if progressBarUpdate {
				printProgress(logger, *fileID, lastProgress, true)
			} // Afficher la dernière progression avant le message final
			if err != nil {
				logger.Error("Download failed", "file_id", *fileID, "error", err)
				os.Exit(1)
			}
			logger.Info("Download completed successfully!", "file_id", *fileID, "destination", *destPath)
			return // Succès
		case <-ticker.C:
			if progressBarUpdate && progressChan != nil { // Afficher seulement si la progression a changé
				printProgress(logger, *fileID, lastProgress, false)
				progressBarUpdate = false
			}
		case <-ctx.Done():
			logger.Info("Client shutting down due to context cancellation (main).")
			select {
			case errShutdown := <-finalErrorChan:
				if errShutdown != nil && !errors.Is(errShutdown, context.Canceled) && !errors.Is(errShutdown, downloader.ErrDownloadCancelled) {
					logger.Error("Download ended with error during shutdown", "error", errShutdown)
				} else {
					logger.Info("Download ended cleanly or was cancelled during shutdown.")
				}
			case <-time.After(10 * time.Second):
				logger.Warn("Timeout waiting for downloader to finish after cancellation.")
			}
			return
		}
	}
}

func printProgress(logger *slog.Logger, fileID string, p downloader.ProgressInfo, final bool) {
	percent := 0.0
	if p.TotalSizeBytes > 0 {
		percent = (float64(p.DownloadedSizeBytes) / float64(p.TotalSizeBytes)) * 100
	}
	status := "Progress"
	if final && p.FailedChunks > 0 {
		status = "Last progress (failed)"
	} else if final {
		status = "Final progress"
	}

	logger.Info(status,
		"file", fileID,
		"progress", fmt.Sprintf("%.2f%%", percent),
		"chunks", fmt.Sprintf("%d/%d", p.CompletedChunks, p.TotalChunks),
		"bytes", fmt.Sprintf("%s/%s", formatBytes(p.DownloadedSizeBytes), formatBytes(p.TotalSizeBytes)),
		"active", p.ActiveDownloads,
		"failed", p.FailedChunks,
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

// loadCACert charge un certificat CA depuis un fichier et l'ajoute au pool du tls.Config
func loadCACert(tlsConfig *tls.Config, caFile string, logger *slog.Logger) error {
	caCert, err := ioutil.ReadFile(caFile) // os.ReadFile à partir de Go 1.16
	if err != nil {
		return fmt.Errorf("failed to read CA certificate file %s: %w", caFile, err)
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
		return fmt.Errorf("failed to append CA certificate from %s to pool", caFile)
	}
	logger.Info("Successfully loaded CA certificate", "ca_file", caFile)
	return nil
}
