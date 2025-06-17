// cmd/agent/main.go
package main // ou le nom de votre package agent

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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
	"sync"
	"syscall"
	"time"

	chunkserver "qwrap/internal/agent"
	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb"

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	alpnAgentToOrchestrator = "qwrap-agent-orch" // ALPN pour comm Agent -> Orchestrateur
	alpnAgentChunkServer    = "qwrap"            // ALPN pour le serveur de chunks de cet agent
	defaultMaxStreamsAgent  = int64(50)          // Capacité par défaut de l'agent
)

// Pour l'enregistrement et les heartbeats, l'agent agit comme un client QUIC vers l'orchestrateur.
// On pourrait réutiliser une structure similaire à OrchestratorComms du client, ou une plus simple.
type orchestratorAgentComms struct {
	orchestratorAddress string
	agentID             string
	tlsConfig           *tls.Config
	quicConfig          *quic.Config
	logger              *slog.Logger
	conn                quic.Connection
	mu                  sync.Mutex
	writerFactory       func(io.Writer, *slog.Logger) framing.Writer
	readerFactory       func(io.Reader, *slog.Logger) framing.Reader
}

func newOrchestratorAgentComms(addr, agentID string, tlsConf *tls.Config, logger *slog.Logger, wf func(io.Writer, *slog.Logger) framing.Writer, rf func(io.Reader, *slog.Logger) framing.Reader) *orchestratorAgentComms {
	orchSpecificTLSConf := tlsConf.Clone() // Éviter de modifier la config partagée
	orchSpecificTLSConf.NextProtos = []string{alpnAgentToOrchestrator}

	return &orchestratorAgentComms{
		orchestratorAddress: addr,
		agentID:             agentID,
		tlsConfig:           orchSpecificTLSConf,
		quicConfig:          &quic.Config{MaxIdleTimeout: 20 * time.Second, HandshakeIdleTimeout: 5 * time.Second},
		logger:              logger.With("subcomponent", "agent_orch_comms"),
		writerFactory:       wf,
		readerFactory:       rf,
	}
}

func (c *orchestratorAgentComms) connect(ctx context.Context) (quic.Connection, error) {
	c.mu.Lock()
	// Si une connexion existe et est active, la réutiliser
	if c.conn != nil && c.conn.Context().Err() == nil {
		conn := c.conn
		c.mu.Unlock()
		c.logger.Debug("Reusing existing connection to orchestrator")
		return conn, nil
	}
	// Si une connexion existait mais est morte, la fermer proprement avant d'en ouvrir une nouvelle
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "stale connection") // Ignorer l'erreur de fermeture ici
		c.conn = nil
	}
	c.mu.Unlock() // Libérer le verrou avant l'appel bloquant DialAddr

	c.logger.Info("Agent attempting to connect to orchestrator", "address", c.orchestratorAddress)
	conn, err := quic.DialAddr(ctx, c.orchestratorAddress, c.tlsConfig, c.quicConfig)
	if err != nil {
		c.logger.Error("Agent failed to dial orchestrator", "address", c.orchestratorAddress, "error", err)
		return nil, err
	}

	c.mu.Lock()
	c.conn = conn // Stocker la nouvelle connexion
	c.mu.Unlock()
	c.logger.Info("Agent successfully connected to orchestrator", "address", c.orchestratorAddress)
	return conn, nil
}

func (c *orchestratorAgentComms) register(ctx context.Context, agentListenAddr string, capabilities []string, maxStreams int64) (*qwrappb.AgentRegistrationResponse, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}

	streamCtx, streamCancel := context.WithTimeout(ctx, 10*time.Second) // Timeout pour l'opération de stream
	defer streamCancel()
	stream, err := conn.OpenStreamSync(streamCtx)
	if err != nil {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.CloseWithError(1, "reg openstream fail")
			c.conn = nil
		}
		c.mu.Unlock()
		return nil, err
	}
	defer stream.Close()

	logger := c.logger.With("stream_id", stream.StreamID(), "operation", "register")
	writer := c.writerFactory(stream, logger)
	reader := c.readerFactory(stream, logger)

	purpose := &qwrappb.StreamPurposeRequest{Purpose: qwrappb.StreamPurposeRequest_AGENT_REGISTRATION}
	if err := writer.WriteMsg(streamCtx, purpose); err != nil {
		return nil, err
	} // Utiliser streamCtx

	req := &qwrappb.AgentRegistrationRequest{AgentId: c.agentID, AgentAddress: agentListenAddr, Capabilities: capabilities, MaxConcurrentStreams: maxStreams}
	if err := writer.WriteMsg(streamCtx, req); err != nil {
		return nil, err
	}

	var resp qwrappb.AgentRegistrationResponse
	if err := reader.ReadMsg(streamCtx, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *orchestratorAgentComms) sendHeartbeat(ctx context.Context, status qwrappb.AgentStatus, cpu float32, mem int64, bw int64, activeConns, activeTransfers int32) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return err
	}

	streamCtx, streamCancel := context.WithTimeout(ctx, 5*time.Second) // Timeout court pour un heartbeat
	defer streamCancel()
	stream, err := conn.OpenStreamSync(streamCtx) // Unidirectionnel est OK pour HB si pas de réponse
	if err != nil {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.CloseWithError(1, "hb openstream fail")
			c.conn = nil
		}
		c.mu.Unlock()
		return err
	}
	defer stream.Close()

	logger := c.logger.With("stream_id", stream.StreamID(), "operation", "heartbeat")
	writer := c.writerFactory(stream, logger)

	purpose := &qwrappb.StreamPurposeRequest{Purpose: qwrappb.StreamPurposeRequest_AGENT_HEARTBEAT}
	if err := writer.WriteMsg(streamCtx, purpose); err != nil {
		return err
	}

	hb := &qwrappb.AgentHeartbeat{AgentId: c.agentID, Timestamp: timestamppb.Now(), Status: status, CpuLoadPercent: cpu, MemoryUsageBytes: mem, AvailableBandwidthBps: bw, ActiveClientConnections: activeConns, ActiveChunkTransfers: activeTransfers}
	if err := writer.WriteMsg(streamCtx, hb); err != nil {
		c.logger.Warn("Agent failed to send heartbeat data", "error", err)
		c.mu.Lock()
		if c.conn != nil {
			c.conn.CloseWithError(1, "hb send fail")
			c.conn = nil
		}
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *orchestratorAgentComms) Close() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "agent comms to orchestrator closing")
		c.conn = nil
	}
	c.mu.Unlock()
	c.logger.Info("Closed agent connection to orchestrator.")
}

func main() {
	agentID := flag.String("id", fmt.Sprintf("agent-%d", time.Now().UnixNano()%10000), "Unique Agent ID")
	listenAddr := flag.String("listen", ":0", "Address for ChunkServer to listen on (e.g., :8080, :0 pour port auto)")
	dataDir := flag.String("data", "./agent_data_files", "Directory containing files to serve")
	orchestratorAddr := flag.String("orchestrator", "localhost:7878", "Orchestrator address")
	insecureOrch := flag.Bool("insecure-orch", true, "Skip TLS cert verification for orchestrator connection (testing - set to true for local tests)")
	certFile := flag.String("cert", "", "TLS certificate file for ChunkServer (generates self-signed if empty)")
	keyFile := flag.String("key", "", "TLS key file for ChunkServer (generates self-signed if empty)")
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)                          // Setting this makes it the default for all slog calls in other packages too.
	agentLogger := logger.With("agent_id", *agentID) // Use this logger for agent-specific messages

	agentLogger.Info("qwrap agent starting up...", "listen_addr_config", *listenAddr, "data_dir", *dataDir, "orchestrator", *orchestratorAddr)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		agentLogger.Error("Failed to create data directory", "path", *dataDir, "error", err)
		os.Exit(1)
	}

	// TLS pour le ChunkServer
	serverTLSConf, err := setupAgentServerTLS(*certFile, *keyFile, agentLogger)
	if err != nil {
		agentLogger.Error("Failed to setup TLS for ChunkServer", "error", err)
		os.Exit(1)
	}
	// serverTLSConf.NextProtos est déjà mis à {"qwrap"} par setupAgentServerTLS

	// TLS pour la connexion de l'Agent à l'Orchestrateur
	orchClientTLSConf := &tls.Config{
		InsecureSkipVerify: *insecureOrch, // Important pour les tests locaux avec certs auto-signés pour l'orchestrateur
		// NextProtos est géré par newOrchestratorAgentComms
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		agentLogger.Info("Received signal, shutting down agent...", "signal", sig)
		cancel()
	}()

	writerFactory := func(w io.Writer, l *slog.Logger) framing.Writer { return framing.NewMessageWriter(w, l) }
	readerFactory := func(r io.Reader, l *slog.Logger) framing.Reader { return framing.NewMessageReader(r, l) }

	agentCommsToOrch := newOrchestratorAgentComms(*orchestratorAddr, *agentID, orchClientTLSConf, agentLogger, writerFactory, readerFactory)
	defer agentCommsToOrch.Close()

	// Démarrer le ChunkServer
	csConfig := chunkserver.ChunkServerConfig{
		ListenAddr:           *listenAddr,
		BaseDataDir:          *dataDir,
		TLSConfig:            serverTLSConf,
		Logger:               agentLogger, // Passer le logger de l'agent
		AgentId:              *agentID,
		MessageWriterFactory: writerFactory, // Passer les factories
		MessageReaderFactory: readerFactory,
	}
	cs, err := chunkserver.NewChunkServer(csConfig)
	if err != nil {
		agentLogger.Error("Failed to create ChunkServer", "error", err)
		os.Exit(1)
	}

	var wgAgent sync.WaitGroup
	wgAgent.Add(1)
	var actualListenAddr string
	var serverErrChan = make(chan error, 1) // Pour récupérer l'erreur de cs.Start

	go func() {
		defer wgAgent.Done()
		agentLogger.Info("ChunkServer attempting to start...")
		if errS := cs.Start(ctx); errS != nil {
			serverErrChan <- errS // Envoyer l'erreur au channel
			if !errors.Is(errS, context.Canceled) && !strings.Contains(errS.Error(), "server closed") && !strings.Contains(errS.Error(), "context canceled") {
				agentLogger.Error("ChunkServer failed or exited unexpectedly", "error", errS)
				cancel() // Arrêter tout l'agent si le chunk server critique échoue
			}
		}
		agentLogger.Info("ChunkServer Start goroutine finished.")
		close(serverErrChan) // Fermer le channel quand la goroutine se termine
	}()

	// Attendre que le serveur démarre ou échoue, ou timeout
	select {
	case errStart := <-serverErrChan:
		if errStart != nil {
			agentLogger.Error("ChunkServer could not start", "error", errStart)
			cancel()
			wgAgent.Wait()
			os.Exit(1)
		}
		// Si errStart est nil mais le channel est fermé, Start a réussi et s'est terminé (ne devrait pas arriver si ctx est ok)
	case <-time.After(2 * time.Second): // Timeout pour le démarrage du serveur
		if cs.Addr() == nil {
			agentLogger.Error("ChunkServer failed to start and provide an address within timeout.")
			cancel()
			wgAgent.Wait()
			os.Exit(1)
		}
	}
	// S'assurer que cs.Addr() n'est pas nil
	if cs.Addr() == nil {
		agentLogger.Error("ChunkServer address is nil after start attempt.")
		cancel()
		wgAgent.Wait()
		os.Exit(1)
	}
	actualListenAddr = cs.Addr().String()
	agentLogger.Info("ChunkServer is listening on", "actual_address", actualListenAddr)

	// Enregistrement
	regCtx, regCancel := context.WithTimeout(ctx, 15*time.Second)
	regResp, err := agentCommsToOrch.register(regCtx, actualListenAddr, []string{"storage_local"}, defaultMaxStreamsAgent)
	regCancel()
	if err != nil {
		agentLogger.Error("Failed to register with orchestrator", "error", err)
		cancel()
		wgAgent.Wait()
		os.Exit(1)
	}
	if !regResp.Success {
		agentLogger.Error("Orchestrator denied registration", "message", regResp.Message)
		cancel()
		wgAgent.Wait()
		os.Exit(1)
	}

	agentLogger.Info("Agent successfully registered with orchestrator", "reported_listen_addr", actualListenAddr)
	heartbeatInterval := 10 * time.Second
	if regResp.HeartbeatInterval != nil && regResp.HeartbeatInterval.CheckValid() == nil {
		sugInterval := regResp.HeartbeatInterval.AsDuration()
		if sugInterval >= 2*time.Second {
			heartbeatInterval = sugInterval
		} // Minimum 2s
	}
	agentLogger.Info("Heartbeat interval set", "interval", heartbeatInterval)

	// Boucle Heartbeat
	wgAgent.Add(1)
	go func() { /* ... (boucle heartbeat comme avant, en utilisant agentLogger) ... */
		defer wgAgent.Done()
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		agentLogger.Info("Heartbeat loop started")
		for {
			select {
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(ctx, heartbeatInterval/2)
				// TODO: Collecter les vraies métriques de l'agent
				errHB := agentCommsToOrch.sendHeartbeat(hbCtx, qwrappb.AgentStatus_AGENT_STATUS_IDLE, 0, 0, 0, 0, 0)
				hbCancel()
				if errHB != nil {
					agentLogger.Warn("Failed to send heartbeat", "error", errHB)
				}
			case <-ctx.Done():
				agentLogger.Info("Heartbeat loop stopping.")
				return
			}
		}
	}()

	agentLogger.Info("Agent is running. Press Ctrl+C to exit.")
	<-ctx.Done() // Attendre le signal d'arrêt

	agentLogger.Info("Agent shutting down main process...")
	// L'annulation du contexte `ctx` devrait déjà signaler au ChunkServer et à la boucle de heartbeat de s'arrêter.
	// cs.Stop() pourrait être appelé ici avec un timeout plus court si nécessaire, mais ctx devrait suffire.

	agentCommsToOrch.Close()
	wgAgent.Wait() // Attendre que le ChunkServer et la boucle de heartbeat se terminent
	agentLogger.Info("Agent shutdown complete.")
}

// setupAgentServerTLS génère un certificat TLS auto-signé temporaire pour le serveur agent
// ou charge à partir de fichiers si spécifié.
// ATTENTION: En production, utilisez des certificats valides !
func setupAgentServerTLS(certFile, keyFile string, logger *slog.Logger) (*tls.Config, error) {
	if certFile != "" && keyFile != "" {
		logger.Info("Loading TLS certificate for Agent's ChunkServer from files", "cert_file", certFile, "key_file", keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS key pair: %w", err)
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{alpnAgentChunkServer}}, nil
	}
	logger.Warn("No TLS cert/key files provided for ChunkServer, generating self-signed (INSECURE)")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()), Subject: pkix.Name{Organization: []string{"qwrap-agent-self"}},
		NotBefore: time.Now(), NotAfter: time.Now().Add(30 * 24 * time.Hour), // 30 jours
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, DNSNames: []string{"localhost"},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
		NextProtos:   []string{alpnAgentChunkServer},
	}, nil
}
