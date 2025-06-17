// cmd/orchestrator/main.go
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"qwrap/internal/framing"
	"qwrap/internal/orchestrator/scheduler"
	"qwrap/internal/orchestrator/server"
	statemanager "qwrap/internal/orchestrator/state"
)

// ALPN constants (pourraient être dans un package partagé `common` ou `protocol`)
const (
	alpnOrchestratorForClients = "qwrap-orchestrator" // Client -> Orchestrator
	alpnOrchestratorForAgents  = "qwrap-agent-orch"   // Agent -> Orchestrator
)

func main() {
	listenAddr := flag.String("listen", "0.0.0.0:7878", "Address for Orchestrator to listen on")
	certFile := flag.String("cert", "", "TLS certificate file (generates self-signed if empty)")
	keyFile := flag.String("key", "", "TLS key file (generates self-signed if empty)")
	dbPath := flag.String("dbpath", "orchestrator.db", "Path for the StateManager database file")
	logLevelStr := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	flag.Parse()

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
	slog.SetDefault(logger) // Définit le logger global pour les packages qui pourraient l'utiliser

	logger.Info("qwrap orchestrator starting...", "listen_addr", *listenAddr, "log_level", logLevel.String())

	orchestratorTLSConf, err := setupOrchestratorTLS(*certFile, *keyFile, logger)
	if err != nil {
		logger.Error("Failed to setup TLS config for Orchestrator", "error", err)
		os.Exit(1)
	}

	// Contexte principal pour l'application, annulé sur SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // S'assurer que cancel est appelé à la fin de main

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down orchestrator...", "signal", sig.String())
		cancel() // Déclenche l'arrêt gracieux
	}()

	// 1. Initialiser StateManager
	smConfig := statemanager.StateManagerConfig{
		Logger: logger.With("subsystem", "statemanager"),
		DBPath: *dbPath,
	}
	stateMgr, err := statemanager.NewStateManager(smConfig)
	if err != nil {
		logger.Error("Failed to initialize StateManager", "error", err)
		os.Exit(1)
	}
	defer func() {
		logger.Info("Closing StateManager...")
		if err := stateMgr.Close(); err != nil {
			logger.Error("Failed to close StateManager cleanly", "error", err)
		}
	}()
	logger.Info("StateManager initialized")
	// TODO: stateMgr.LoadState(ctx) // Pour la persistance future

	// 2. Initialiser Scheduler
	schedConfig := scheduler.SchedulerConfig{
		Logger:                   logger.With("subsystem", "scheduler"),
		DefaultAlgorithm:         scheduler.AlgoLeastLoaded,
		AgentHeartbeatTimeout:    30 * time.Second,
		AgentEvictionGracePeriod: 2 * time.Minute, // Un peu plus court pour les tests
	}
	sched := scheduler.NewScheduler(schedConfig)
	sched.StartBackgroundTasks(ctx) // Démarrer le nettoyage des agents
	logger.Info("Scheduler initialized and background tasks started")

	// 3. Initialiser les factories de framing
	// Le logger passé ici sera le logger de base du serveur, les handlers ajouteront plus de contexte.
	writerFactory := func(w io.Writer, l *slog.Logger) framing.Writer {
		if l == nil {
			l = logger
		} // Fallback
		return framing.NewMessageWriter(w, l)
	}
	readerFactory := func(r io.Reader, l *slog.Logger) framing.Reader {
		if l == nil {
			l = logger
		} // Fallback
		return framing.NewMessageReader(r, l)
	}

	// 4. Initialiser et démarrer OrchestratorServer
	serverConfig := server.OrchestratorServerConfig{
		ListenAddr:           *listenAddr,
		TLSConfig:            orchestratorTLSConf,
		Logger:               logger.With("subsystem", "quic_server"),
		Scheduler:            sched,
		StateManager:         stateMgr,
		MessageWriterFactory: writerFactory,
		MessageReaderFactory: readerFactory,
	}
	orchestratorSrv, err := server.NewOrchestratorServer(serverConfig)
	if err != nil {
		logger.Error("Failed to create orchestrator server", "error", err)
		os.Exit(1)
	}

	logger.Info("Orchestrator server attempting to start...")
	// Start est bloquant et gère son propre arrêt sur ctx.Done()
	err = orchestratorSrv.Start(ctx)
	if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "server closed") {
		// server closed est une erreur normale sur l'arrêt du listener par ctx.Done
		logger.Error("Orchestrator server failed or exited unexpectedly", "error", err)
		// cancel() est déjà appelé par le defer ou le signal handler
		// os.Exit(1) // Quitter si le serveur principal ne peut pas démarrer/tourner
	}

	// Attendre explicitement que le contexte soit annulé si Start retourne nil (arrêt normal)
	// Cela garantit que main ne se termine pas avant que le signal handler ait appelé cancel()
	// et que les tâches de fond aient une chance de se terminer.
	if err == nil || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "server closed") {
		logger.Info("Orchestrator server Start() returned, waiting for context cancellation to exit main...")
		<-ctx.Done() // Attendre ici si ce n'est pas déjà fait
	}

	logger.Info("Orchestrator main process shutting down...")
	// orchestratorSrv.Stop() est appelé implicitement lorsque son propre contexte (passé à Start) est annulé.
	// Les tâches de fond du scheduler s'arrêteront aussi via leur contexte.
	// StateManager n'a pas de tâches de fond pour l'instant.
	logger.Info("Orchestrator shutdown complete.")
}

func setupOrchestratorTLS(certFile, keyFile string, logger *slog.Logger) (*tls.Config, error) {
	if certFile != "" && keyFile != "" {
		logger.Info("Loading TLS certificate for Orchestrator from files", "cert_file", certFile, "key_file", keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load Orchestrator TLS key pair: %w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{alpnOrchestratorForClients, alpnOrchestratorForAgents},
		}, nil
	}

	logger.Warn("No TLS cert/key files provided for Orchestrator, generating self-signed certificate (INSECURE, FOR TESTING ONLY)")
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate RSA key: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()), // Un numéro de série simple
		Subject: pkix.Name{
			Organization: []string{"qwrap-orchestrator-self-signed"},
			CommonName:   "localhost", // Important pour la validation du client si pas InsecureSkipVerify
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // Valide pour 1 an
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, // ServerAuth est crucial
		BasicConstraintsValid: true,
		IsCA:                  true, // Pour pouvoir signer d'autres certs si besoin, ou juste pour être simple
		IPAddresses:           []net.IP{net.ParseIP("0.0.0.0"), net.ParseIP("127.0.0.1"), net.ParseIP("::1"), net.ParseIP("::")},
		DNSNames:              []string{"localhost", "orchestrator.qwrap.test.internal"}, // Noms que les clients peuvent utiliser
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create self-signed certificate: %w", err)
	}

	// Optionnel: sauvegarder les clés générées
	if err := os.WriteFile("orchestrator-selfsigned.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0644); err != nil {
		logger.Warn("Failed to save self-signed orchestrator cert", "error", err)
	}
	privBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	if err := os.WriteFile("orchestrator-selfsigned.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}), 0600); err != nil {
		logger.Warn("Failed to save self-signed orchestrator key", "error", err)
	}
	logger.Info("Saved self-signed orchestrator certificate to orchestrator-selfsigned.crt and orchestrator-selfsigned.key")

	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
		NextProtos:   []string{alpnOrchestratorForClients, alpnOrchestratorForAgents},
	}, nil
}
