package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"qwrap/internal/framing"
	"qwrap/internal/orchestrator/scheduler" // Assurez-vous que le chemin est correct
	statemanager "qwrap/internal/orchestrator/state"

	"qwrap/pkg/qwrappb" // Assurez-vous que le chemin est correct

	"github.com/quic-go/quic-go"
)

const (
	orchestratorDefaultReadTimeout  = 30 * time.Second
	orchestratorDefaultWriteTimeout = 10 * time.Second
	alpnOrchestratorProtocol        = "qwrap-orchestrator" // Peut être le même "qwrap" ou spécifique
	alpnAgentOrchestratorComms      = "qwrap-agent-orch"
)

var (
	ErrOrchestratorServerClosed = errors.New("orchestrator server is closed")
)

// OrchestratorServerConfig contient la configuration du serveur Orchestrateur.
type OrchestratorServerConfig struct {
	ListenAddr string
	TLSConfig  *tls.Config // Configuration TLS pour le serveur QUIC
	Logger     *slog.Logger

	Scheduler    scheduler.Scheduler       // Dépendance injectée
	StateManager statemanager.StateManager // Dépendance injectée

	MessageWriterFactory func(io.Writer, *slog.Logger) framing.Writer
	MessageReaderFactory func(io.Reader, *slog.Logger) framing.Reader
	AgentClientTLSConfig *tls.Config
	MetadataQueryTimeout time.Duration
}

func (c *OrchestratorServerConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default().With("component", "orchestrator_server")
	}
	if c.TLSConfig == nil {
		panic("TLSConfig is mandatory for OrchestratorServer")
	}
	if c.AgentClientTLSConfig == nil {
		c.Logger.Warn("AgentClientTLSConfig not provided, creating a default one (likely insecure for production).")
		c.AgentClientTLSConfig = &tls.Config{
			InsecureSkipVerify: true,                                 // ATTENTION: Pour tests locaux seulement!
			NextProtos:         []string{alpnAgentOrchestratorComms}, // ALPN que le ChunkServer de l'Agent attend
		}
	} else { // S'assurer que l'ALPN correct est là
		found := false
		for _, p := range c.AgentClientTLSConfig.NextProtos {
			if p == alpnAgentOrchestratorComms {
				found = true
				break
			}
		}
		if !found {
			c.AgentClientTLSConfig.NextProtos = append(c.AgentClientTLSConfig.NextProtos, alpnAgentOrchestratorComms)
		}
	}
	if len(c.TLSConfig.NextProtos) == 0 {
		c.TLSConfig.NextProtos = []string{alpnOrchestratorProtocol}
	} // else: s'assurer que le protocole est présent si NextProtos est déjà défini

	if c.Scheduler == nil {
		panic("Scheduler is mandatory for OrchestratorServer")
	}
	if c.StateManager == nil {
		panic("StateManager is mandatory for OrchestratorServer")
	}
	if c.MessageWriterFactory == nil {
		c.MessageWriterFactory = func(w io.Writer, l *slog.Logger) framing.Writer {
			return framing.NewMessageWriter(w, l.With("subcomponent", "msg_writer_orch"))
		}
	}
	if c.MessageReaderFactory == nil {
		c.MessageReaderFactory = func(r io.Reader, l *slog.Logger) framing.Reader {
			return framing.NewMessageReader(r, l.With("subcomponent", "msg_reader_orch"))
		}
	}
	if c.MetadataQueryTimeout <= 0 {
		c.MetadataQueryTimeout = 10 * time.Second // Timeout par défaut pour une requête de métadonnées à un agent
	}
}

// OrchestratorServer est le serveur principal de l'orchestrateur.
type OrchestratorServer struct {
	config   OrchestratorServerConfig
	listener *quic.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex // Protège listener et closed
	closed   bool

	// Pour gérer les flux de mise à jour des plans clients
	// Clé: PlanID (ou peut-être ClientID si vous en avez un)
	// Valeur: un canal ou un writer direct vers le flux QUIC du client pour les UpdatedTransferPlan
	clientUpdateStreams map[string]quic.Stream // Attention à la gestion de la concurrence et du cycle de vie
	muClientStreams     sync.RWMutex
}

// NewOrchestratorServer crée une nouvelle instance du serveur Orchestrateur.
func NewOrchestratorServer(config OrchestratorServerConfig) (*OrchestratorServer, error) {
	config.setDefaults()
	return &OrchestratorServer{
		config:              config,
		clientUpdateStreams: make(map[string]quic.Stream),
	}, nil
}

// Start démarre le serveur QUIC de l'orchestrateur.
func (s *OrchestratorServer) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("orchestrator server already started")
	}

	quicConf := &quic.Config{
		MaxIdleTimeout:       60 * time.Second, // Les connexions client/agent peuvent être plus longues
		HandshakeIdleTimeout: 10 * time.Second,
		// KeepAlivePeriod: ...
	}

	listener, err := quic.ListenAddr(s.config.ListenAddr, s.config.TLSConfig, quicConf)
	if err != nil {
		s.mu.Unlock()
		s.config.Logger.Error("Failed to start orchestrator QUIC listener", "address", s.config.ListenAddr, "error", err)
		return fmt.Errorf("listen on %s failed: %w", s.config.ListenAddr, err)
	}
	s.listener = listener
	s.closed = false
	s.mu.Unlock()

	s.config.Logger.Info("OrchestratorServer started", "address", listener.Addr().String())

	// Démarrer les tâches de fond du scheduler (si ce n'est pas déjà fait ailleurs)
	// s.config.Scheduler.StartBackgroundTasks(ctx) // Assumer que c'est géré par le main de l'orchestrateur

	s.wg.Add(1)
	go func() { // Goroutine d'arrêt
		defer s.wg.Done()
		<-ctx.Done()
		s.config.Logger.Info("OrchestratorServer context cancelled, stopping listener...")
		s.mu.Lock()
		if s.listener != nil {
			_ = s.listener.Close()
			s.listener = nil
		}
		s.closed = true
		s.mu.Unlock()
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
				s.config.Logger.Info("Orchestrator listener accept loop ending", "reason", err)
				return nil
			}
			s.config.Logger.Error("Failed to accept QUIC connection (orchestrator)", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.config.Logger.Info("Orchestrator accepted new QUIC connection", "remote_addr", conn.RemoteAddr().String())
		s.wg.Add(1)
		// Utiliser le contexte du serveur (ctx) pour la durée de vie de la connexion,
		// mais chaque flux aura son propre sous-contexte.
		go s.handleConnection(ctx, conn)
	}
}

// Stop arrête le serveur.
func (s *OrchestratorServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.config.Logger.Info("OrchestratorServer Stop called but already closed.")
		return nil
	}
	s.closed = true
	listenerToClose := s.listener
	s.listener = nil
	s.mu.Unlock()
	s.config.Logger.Info("Stopping OrchestratorServer...")
	var err error
	if listenerToClose != nil {
		if e := listenerToClose.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
			err = e
		}
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.config.Logger.Info("OrchestratorServer all goroutines finished.")
	case <-ctx.Done():
		s.config.Logger.Warn("OrchestratorServer stop timed out.", "error", ctx.Err())
		return ctx.Err()
	}
	return err
}

func (s *OrchestratorServer) handleConnection(connCtx context.Context, conn quic.Connection) {
	defer s.wg.Done()
	defer conn.CloseWithError(0, "orchestrator connection handler finished")
	s.config.Logger.Debug("Orchestrator handling connection", "remote_addr", conn.RemoteAddr())

	for {
		// Accepter un nouveau flux (requête) de ce client/agent
		// Utiliser un contexte dérivé de celui de la connexion pour ce flux.
		streamCtx, streamCancel := context.WithCancel(connCtx)

		stream, err := conn.AcceptStream(streamCtx)
		if err != nil {
			streamCancel() // Important d'annuler le contexte du stream si Accept échoue
			if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || err == quic.ErrServerClosed || strings.Contains(err.Error(), "connection is closed") {
				s.config.Logger.Debug("Connection closed or context cancelled (orchestrator handleConnection)", "remote_addr", conn.RemoteAddr(), "error", err)
			} else if _, ok := err.(*quic.IdleTimeoutError); ok {
				s.config.Logger.Info("Orchestrator connection idle timeout", "remote_addr", conn.RemoteAddr().String())
			} else {
				s.config.Logger.Error("Failed to accept stream (orchestrator)", "remote_addr", conn.RemoteAddr(), "error", err)
			}
			return // Fin du traitement de cette connexion
		}
		s.config.Logger.Debug("Orchestrator accepted new stream", "remote_addr", conn.RemoteAddr(), "stream_id", stream.StreamID())

		// Laisser la goroutine handleStream gérer la fermeture de son propre streamCtx via streamCancel
		s.wg.Add(1)
		go s.routeStream(streamCtx, streamCancel, stream, conn.RemoteAddr())
	}
}

// routeStream essaie de déterminer le type de message et le route au bon handler.
// C'est une simplification. Un vrai système pourrait utiliser des types de flux QUIC différents
// ou un premier message sur le flux pour indiquer l'intention.
func (s *OrchestratorServer) routeStream(ctx context.Context, streamCancel context.CancelFunc, stream quic.Stream, remoteAddr net.Addr) {
	defer s.wg.Done()
	// Le streamCancel sera appelé ici, SAUF si un handler long-lived prend le relais.
	isLongLived := false
	defer func() {
		if !isLongLived { // Seulement si le handler n'a pas indiqué qu'il gérait le cycle de vie
			streamCancel() // Annuler le contexte spécifique au flux
			// if err := stream.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, context.Canceled) {
			// 	var streamErr *quic.StreamError
			// 	if !errors.As(err, &streamErr) || (streamErr.ErrorCode != 0 && streamErr.Remote) {
			// 		s.config.Logger.Warn("Error closing stream in routeStream defer (non-long-lived)", "remote_addr", remoteAddr, "stream_id", stream.StreamID(), "error", err)
			// 	}
			// }
		}
	}()

	logger := s.config.Logger.With("remote_addr", remoteAddr, "stream_id", stream.StreamID())
	msgReader := s.config.MessageReaderFactory(stream, logger)
	msgWriter := s.config.MessageWriterFactory(stream, logger)

	// 1. Lire le StreamPurposeRequest
	var purposeReq qwrappb.StreamPurposeRequest
	if err := msgReader.ReadMsg(ctx, &purposeReq); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
			logger.Debug("Failed to read StreamPurposeRequest, stream closed or context cancelled", "error", err)
		} else {
			logger.Error("Failed to read StreamPurposeRequest", "error", err)
		}
		return // Ferme le stream via defer
	}
	logger.Info("Received StreamPurposeRequest", "purpose", purposeReq.Purpose, "plan_id_context", purposeReq.PlanIdContext)

	// 2. Router vers le handler approprié
	switch purposeReq.Purpose {
	case qwrappb.StreamPurposeRequest_TRANSFER_REQUEST:
		s.handleTransferRequest(ctx, stream, msgReader, msgWriter, remoteAddr)
	case qwrappb.StreamPurposeRequest_CLIENT_REPORT:
		s.handleClientReport(ctx, stream, msgReader, remoteAddr)
	case qwrappb.StreamPurposeRequest_REGISTER_FOR_PLAN_UPDATES:
		// Ce handler prendra en charge la vie du stream, donc on ne le fermera pas dans le defer de routeStream.
		isLongLived = true
		s.handleRegisterForPlanUpdates(ctx, stream, msgReader, purposeReq.PlanIdContext, remoteAddr, streamCancel) // Passer streamCancel pour que le handler puisse l'appeler
	case qwrappb.StreamPurposeRequest_AGENT_REGISTRATION:
		s.handleAgentRegistration(ctx, stream, msgReader, msgWriter, remoteAddr)
	case qwrappb.StreamPurposeRequest_AGENT_HEARTBEAT:
		s.handleAgentHeartbeat(ctx, stream, msgReader, remoteAddr)

	default:
		logger.Warn("Unknown or unsupported stream purpose", "purpose", purposeReq.Purpose)
		// Envoyer une erreur au client/agent ? Pour l'instant, on ferme juste.
	}
}

// --- Handlers Spécifiques (exemples, à appeler depuis routeStream ou handleConnection) ---
// discoverFileMetadataFromAgents - Version Réelle
func (s *OrchestratorServer) discoverFileMetadataFromAgents(ctx context.Context, fileID string, clientProvidedMeta *qwrappb.FileMetadata) (*qwrappb.FileMetadata, error) {
	logger := s.config.Logger.With("op", "discoverFileMetadata", "file_id", fileID)
	logger.Info("Attempting to discover file metadata by querying agents")

	_, listCancel := context.WithTimeout(ctx, 5*time.Second)    // Timeout pour obtenir la liste des agents
	availableAgents := s.config.Scheduler.ListAvailableAgents() // Renvoie []*scheduler.AgentInfo
	listCancel()

	if len(availableAgents) == 0 {
		logger.Warn("No agents available to query for file metadata")
		return nil, scheduler.ErrNoAgentsAvailable
	}

	responseChan := make(chan *qwrappb.GetFileMetadataResponse, len(availableAgents))
	var wgDiscovery sync.WaitGroup

	// TLS Config pour se connecter aux agents (doit utiliser l'ALPN que les agents attendent, ex: "qwrap")
	agentSpecificTLSConfig := s.config.AgentClientTLSConfig.Clone() // Cloner pour ne pas modifier l'original
	// S'assurer que l'ALPN est celui du service de chunk des agents, pas celui de l'orchestrateur
	// Cet ALPN est celui que le ChunkServer de l'agent utilise.
	// La constante alpnAgentChunkService est définie dans cmd/agent/main.go ou un lieu partagé.
	const alpnForAgentChunkService = "qwrap"
	agentSpecificTLSConfig.NextProtos = []string{alpnForAgentChunkService}

	quicDialConfigToAgent := &quic.Config{
		MaxIdleTimeout:       s.config.MetadataQueryTimeout / 2, // Timeout plus court pour une simple requête de métadonnées
		HandshakeIdleTimeout: 5 * time.Second,
		KeepAlivePeriod:      0, // Pas besoin de keep-alive pour une requête courte
	}

	agentsQueriedCount := 0
	for _, agentInfo := range availableAgents {
		if agentInfo.Status == qwrappb.AgentStatus_AGENT_STATUS_ERROR || agentInfo.Status == qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE {
			continue // Ne pas interroger les agents en erreur ou explicitement indisponibles
		}
		agentsQueriedCount++
		wgDiscovery.Add(1)
		go func(agInfo *scheduler.AgentInfo) {
			defer wgDiscovery.Done()
			l := logger.With("target_agent_id", agInfo.ID, "target_agent_addr", agInfo.Address)
			l.Debug("Querying agent for metadata")

			queryAgentCtx, queryAgentCancel := context.WithTimeout(ctx, s.config.MetadataQueryTimeout)
			defer queryAgentCancel()

			var agentConn quic.Connection
			var errDial error
			agentConn, errDial = quic.DialAddr(queryAgentCtx, agInfo.Address, agentSpecificTLSConfig, quicDialConfigToAgent)
			if errDial != nil {
				l.Warn("Failed to connect to agent for metadata query", "error", errDial)
				responseChan <- &qwrappb.GetFileMetadataResponse{AgentId: agInfo.ID, Found: false, ErrorMessage: "connect error: " + errDial.Error()}
				return
			}
			defer agentConn.CloseWithError(0, "metadata query session finished")

			stream, errStream := agentConn.OpenStreamSync(queryAgentCtx)
			if errStream != nil {
				l.Warn("Failed to open stream to agent for metadata query", "error", errStream)
				responseChan <- &qwrappb.GetFileMetadataResponse{AgentId: agInfo.ID, Found: false, ErrorMessage: "open stream error: " + errStream.Error()}
				return
			}
			defer stream.Close()

			// Utiliser les factories de l'OrchestratorServer, mais avec un logger contextuel
			writer := s.config.MessageWriterFactory(stream, l)
			reader := s.config.MessageReaderFactory(stream, l)

			// L'agent attend maintenant un StreamPurposeRequest
			purposeMsg := &qwrappb.StreamPurposeRequest{Purpose: qwrappb.StreamPurposeRequest_GET_FILE_METADATA_REQUEST}
			if errWritePurpose := writer.WriteMsg(queryAgentCtx, purposeMsg); errWritePurpose != nil {
				l.Warn("Failed to write metadata purpose to agent", "error", errWritePurpose)
				responseChan <- &qwrappb.GetFileMetadataResponse{AgentId: agInfo.ID, Found: false, ErrorMessage: "write purpose error: " + errWritePurpose.Error()}
				return
			}

			metaReq := &qwrappb.GetFileMetadataRequest{RequestId: "orch-meta-" + fileID + "-" + agInfo.ID, FileId: fileID}
			if errWriteReq := writer.WriteMsg(queryAgentCtx, metaReq); errWriteReq != nil {
				l.Warn("Failed to write metadata request to agent", "error", errWriteReq)
				responseChan <- &qwrappb.GetFileMetadataResponse{AgentId: agInfo.ID, Found: false, ErrorMessage: "write request error: " + errWriteReq.Error()}
				return
			}

			var metaResp qwrappb.GetFileMetadataResponse
			if errReadResp := reader.ReadMsg(queryAgentCtx, &metaResp); errReadResp != nil {
				l.Warn("Failed to read metadata response from agent", "error", errReadResp)
				responseChan <- &qwrappb.GetFileMetadataResponse{AgentId: agInfo.ID, Found: false, ErrorMessage: "read response error: " + errReadResp.Error()}
				return
			}
			if metaResp.AgentId == "" {
				metaResp.AgentId = agInfo.ID
			} // S'assurer que l'AgentID est là
			responseChan <- &metaResp
		}(agentInfo) // Passer agentInfo par valeur à la goroutine
	}

	go func() { wgDiscovery.Wait(); close(responseChan) }()

	if agentsQueriedCount == 0 {
		logger.Warn("No agents were actually suitable or available to query for metadata.")
		return nil, errors.New("no agents suitable for metadata query")
	}

	var bestMeta *qwrappb.FileMetadata
	var firstSuccessfulAgentID string
	var allErrors []string

	for resp := range responseChan {
		if resp.Found && resp.Metadata != nil && resp.Metadata.TotalSize > 0 {
			logger.Info("Received valid metadata from agent", "agent_id", resp.AgentId, "file_id", fileID, "size", resp.Metadata.TotalSize, "checksum", resp.Metadata.ChecksumValue)
			if bestMeta == nil { // Prendre la première réponse valide
				bestMeta = resp.Metadata
				firstSuccessfulAgentID = resp.AgentId
				// Pourrait retourner ici pour la rapidité, ou continuer pour vérifier la cohérence / meilleure source
			} else { // Comparer avec la meilleure réponse actuelle
				if bestMeta.TotalSize != resp.Metadata.TotalSize {
					logger.Warn("Metadata size mismatch between agents", "file_id", fileID, "agent1", firstSuccessfulAgentID, "size1", bestMeta.TotalSize, "agent2", resp.AgentId, "size2", resp.Metadata.TotalSize)
					allErrors = append(allErrors, fmt.Sprintf("agent %s size %d vs agent %s size %d", firstSuccessfulAgentID, bestMeta.TotalSize, resp.AgentId, resp.Metadata.TotalSize))
					// Gérer le conflit : qui croire ? Pour l'instant, on garde le premier.
				}
				// Idem pour checksum
			}
		} else {
			logger.Warn("Agent reported file not found or metadata invalid", "agent_id", resp.AgentId, "found_flag", resp.Found, "agent_err_msg", resp.ErrorMessage)
			if resp.ErrorMessage != "" {
				allErrors = append(allErrors, fmt.Sprintf("agent %s: %s", resp.AgentId, resp.ErrorMessage))
			}
		}
	}

	if bestMeta == nil {
		errMsg := fmt.Sprintf("failed to discover valid metadata for '%s' from any agent", fileID)
		if len(allErrors) > 0 {
			errMsg += ". Agent reports: " + strings.Join(allErrors, "; ")
		}
		logger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	// Fusionner avec les infos que le client aurait pu fournir (ex: CustomAttributes)
	// et s'assurer que le FileId est celui de la requête originale.
	finalMeta := &qwrappb.FileMetadata{
		FileId:            fileID, // Utiliser le FileID de la requête initiale
		TotalSize:         bestMeta.TotalSize,
		ChecksumAlgorithm: bestMeta.ChecksumAlgorithm,
		ChecksumValue:     bestMeta.ChecksumValue,
		LastModified:      bestMeta.LastModified,
	}
	if clientProvidedMeta != nil && len(clientProvidedMeta.CustomAttributes) > 0 {
		finalMeta.CustomAttributes = clientProvidedMeta.CustomAttributes
	}
	// On pourrait aussi fusionner les CustomAttributes de `bestMeta` si l'agent en fournit.

	logger.Info("Successfully resolved file metadata via agent query", "file_id", fileID, "size", finalMeta.TotalSize, "checksum", finalMeta.ChecksumValue, "source_agent_id", firstSuccessfulAgentID)
	return finalMeta, nil
}

// Modifier handleTransferRequest pour utiliser le planID généré par StateManager
func (s *OrchestratorServer) handleTransferRequest(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "TransferRequest", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())
	var clientReq qwrappb.TransferRequest

	readReqCtx, readReqCancel := context.WithTimeout(ctx, orchestratorDefaultReadTimeout)
	errReadReq := reader.ReadMsg(readReqCtx, &clientReq)
	readReqCancel()
	if errReadReq != nil {
		logger.Error("Failed to read TransferRequest", "error", errReadReq)
		// Ne pas tenter d'écrire une réponse si la lecture initiale échoue (flux potentiellement cassé)
		return
	}
	logger.Info("Processing TransferRequest", "request_id", clientReq.RequestId, "num_files", len(clientReq.FilesToTransfer))

	if len(clientReq.FilesToTransfer) == 0 || clientReq.FilesToTransfer[0] == nil || clientReq.FilesToTransfer[0].FileId == "" {
		logger.Error("Invalid TransferRequest: missing or empty FileId in FilesToTransfer")
		errResp := &qwrappb.TransferPlan{ClientRequestId: clientReq.RequestId, ErrorMessage: "Invalid request: FileId missing or malformed FilesToTransfer"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp) // Tenter d'envoyer l'erreur
		writeErrCancel()
		return
	}
	// Pour cette version, on ne gère que le premier fichier de la requête.
	requestedFileMetaFromClient := clientReq.FilesToTransfer[0]

	// 1. Découverte des Métadonnées
	resolveCtx, resolveCancel := context.WithTimeout(ctx, 20*time.Second) // Timeout pour la découverte
	resolvedFileMeta, errMeta := s.discoverFileMetadataFromAgents(resolveCtx, requestedFileMetaFromClient.FileId, requestedFileMetaFromClient)
	resolveCancel()
	if errMeta != nil {
		logger.Error("Failed to resolve file metadata by orchestrator", "file_id", requestedFileMetaFromClient.FileId, "error", errMeta)
		errResp := &qwrappb.TransferPlan{ClientRequestId: clientReq.RequestId, SourceFileMetadata: []*qwrappb.FileMetadata{requestedFileMetaFromClient}, ErrorMessage: "Orchestrator failed to get file details: " + errMeta.Error()}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}
	if resolvedFileMeta.TotalSize <= 0 { // L'orchestrateur doit s'assurer que la taille est valide
		errMsg := fmt.Sprintf("File '%s' reported by orchestrator as empty or with invalid size (%d)", resolvedFileMeta.FileId, resolvedFileMeta.TotalSize)
		logger.Error(errMsg)
		errResp := &qwrappb.TransferPlan{ClientRequestId: clientReq.RequestId, SourceFileMetadata: []*qwrappb.FileMetadata{resolvedFileMeta}, ErrorMessage: errMsg}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}

	// Utiliser les métadonnées résolues pour la suite.
	// On crée une requête interne pour le StateManager et le Scheduler qui contient ces métadonnées complètes.
	internalTransferReq := &qwrappb.TransferRequest{
		RequestId:       clientReq.RequestId,
		FilesToTransfer: []*qwrappb.FileMetadata{resolvedFileMeta}, // Utiliser les métadonnées résolues
		Options:         clientReq.Options,
	}

	// 2. Créer l'état initial du transfert dans StateManager
	planID, _, errSMCreate := s.config.StateManager.CreateTransfer(ctx, internalTransferReq)
	if errSMCreate != nil {
		logger.Error("Failed to create transfer state", "request_id", internalTransferReq.RequestId, "error", errSMCreate)
		errResp := &qwrappb.TransferPlan{ClientRequestId: internalTransferReq.RequestId, SourceFileMetadata: internalTransferReq.FilesToTransfer, ErrorMessage: "Orchestrator internal error (state creation): " + errSMCreate.Error()}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}
	logger.Info("Transfer state created in StateManager", "plan_id", planID)

	// 3. Mettre à jour le statut en PLANNING
	if errSMStatus := s.config.StateManager.UpdateTransferStatus(ctx, planID, statemanager.StatusPlanning); errSMStatus != nil {
		logger.Error("Failed to update transfer status to PLANNING", "plan_id", planID, "error", errSMStatus)
		_ = s.config.StateManager.UpdateTransferStatus(ctx, planID, statemanager.StatusFailed, "internal error during planning init") // Tenter de marquer échoué
		errResp := &qwrappb.TransferPlan{PlanId: planID, ClientRequestId: internalTransferReq.RequestId, SourceFileMetadata: internalTransferReq.FilesToTransfer, ErrorMessage: "Orchestrator internal error (status update): " + errSMStatus.Error()}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}

	// 4. Demander au Scheduler de créer le plan d'affectation des chunks
	// Le Scheduler reçoit maintenant internalTransferReq qui contient resolvedFileMeta avec la bonne taille.
	schedulerPlan, errSched := s.config.Scheduler.CreateTransferPlan(ctx, planID, internalTransferReq)
	if errSched != nil {
		logger.Error("Failed to create transfer plan from scheduler", "plan_id", planID, "error", errSched)
		_ = s.config.StateManager.UpdateTransferStatus(ctx, planID, statemanager.StatusFailed, errSched.Error())
		errResp := &qwrappb.TransferPlan{PlanId: planID, ClientRequestId: internalTransferReq.RequestId, SourceFileMetadata: internalTransferReq.FilesToTransfer, ErrorMessage: "Scheduler failed to create plan: " + errSched.Error()}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}
	// Sanity check: le scheduler doit utiliser le planID fourni
	if schedulerPlan.PlanId != planID {
		errMsg := "Internal orchestrator error: PlanID mismatch between StateManager and Scheduler"
		logger.Error(errMsg, "expected_plan_id", planID, "scheduler_plan_id", schedulerPlan.PlanId)
		_ = s.config.StateManager.UpdateTransferStatus(ctx, planID, statemanager.StatusFailed, errMsg)
		errResp := &qwrappb.TransferPlan{PlanId: planID, ClientRequestId: internalTransferReq.RequestId, SourceFileMetadata: internalTransferReq.FilesToTransfer, ErrorMessage: errMsg}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}
	logger.Info("Scheduler created transfer plan successfully", "plan_id", planID, "num_assignments", len(schedulerPlan.ChunkAssignments))

	// 5. Lier le plan du scheduler à l'état dans StateManager
	if errLink := s.config.StateManager.LinkSchedulerPlan(ctx, planID, schedulerPlan); errLink != nil {
		logger.Error("Failed to link scheduler plan to state", "plan_id", planID, "error", errLink)
		_ = s.config.StateManager.UpdateTransferStatus(ctx, planID, statemanager.StatusFailed, errLink.Error())
		errResp := &qwrappb.TransferPlan{PlanId: planID, ClientRequestId: internalTransferReq.RequestId, SourceFileMetadata: internalTransferReq.FilesToTransfer, ErrorMessage: "Orchestrator internal error (linking plan): " + errLink.Error()}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errResp)
		writeErrCancel()
		return
	}
	// Le statut du transfert est maintenant IN_PROGRESS via LinkSchedulerPlan

	// 6. Renvoyer le schedulerPlan (qui est le qwrappb.TransferPlan complet) au client
	// Assurer que le SourceFileMetadata dans le plan envoyé au client est bien le `resolvedFileMeta`
	// schedulerPlan.SourceFileMetadata = []*qwrappb.FileMetadata{resolvedFileMeta} // Normalement déjà fait par CreateTransferPlan

	writePlanCtx, writePlanCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
	defer writePlanCancel()
	if errWrite := writer.WriteMsg(writePlanCtx, schedulerPlan); errWrite != nil {
		logger.Error("Failed to send successful TransferPlan to client", "plan_id", planID, "error", errWrite)
		// Le statut reste IN_PROGRESS dans le StateManager, le client pourrait retenter ? Ou timeout.
	} else {
		logger.Info("TransferPlan sent successfully to client", "plan_id", planID)
	}
}

func (s *OrchestratorServer) handleClientReport(ctx context.Context, stream quic.Stream, reader framing.Reader, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "ClientReport", "stream_id", stream.StreamID(), "remote_addr", remoteAddr)
	var report qwrappb.ClientReport
	if err := reader.ReadMsg(ctx, &report); err != nil {
		logger.Error("Failed to read ClientReport", "error", err)
		return
	}
	logger.Info("Processing ClientReport", "plan_id", report.PlanId, "num_statuses", len(report.ChunkStatuses))

	for _, cs := range report.ChunkStatuses {
		if cs == nil || cs.ChunkInfo == nil {
			continue
		}
		err := s.config.StateManager.UpdateChunkStatus(ctx, report.PlanId, cs.ChunkInfo.FileId, cs.ChunkInfo.ChunkId, cs.AgentId, cs.Event, cs.Details)
		if err != nil {
			logger.Error("Failed to update chunk status in StateManager", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "error", err)
			continue
		}

		isFailureEvent := cs.Event == qwrappb.TransferEvent_DOWNLOAD_FAILED_AGENT_UNREACHABLE ||
			cs.Event == qwrappb.TransferEvent_DOWNLOAD_FAILED_CHECKSUM_MISMATCH ||
			cs.Event == qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR ||
			cs.Event == qwrappb.TransferEvent_REASSIGNMENT_REQUESTED

		if isFailureEvent {
			logger.Info("Failure/Reassignment_Req reported for chunk, notifying scheduler", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "agent_id", cs.AgentId, "event", cs.Event)

			if errSched := s.config.Scheduler.ReportAgentFailure(ctx, cs.AgentId, cs.ChunkInfo, cs.Details); errSched != nil {
				logger.Error("Scheduler failed to process agent failure report", "agent_id", cs.AgentId, "error", errSched)
			}

			needsReplan, newAssignments, errReplan := s.config.Scheduler.MaybeReplanChunk(ctx, report.PlanId, cs.ChunkInfo, cs.AgentId)
			if errReplan != nil {
				logger.Error("Error during chunk replanning decision", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "agent_id", cs.AgentId, "error", errReplan)
				if errors.Is(errReplan, scheduler.ErrNoAgentsAvailable) {
					// Pas d'agents pour replanifier. Marquer le chunk comme échec définitif dans StateManager.
					logger.Warn("No agents available to replan chunk, marking as permanently failed.", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId)
					// On pourrait avoir un événement spécifique pour cela.
					// Pour l'instant, REASSIGNMENT_REQUESTED avec un détail "no_agents" pourrait fonctionner,
					// ou un nouvel événement "DOWNLOAD_FAILED_NO_REPLAN_AGENTS".
					// L'important est que le StateManager comprenne que ce chunk ne sera plus tenté.
					// Alternativement, le client pourrait avoir un timeout pour les chunks en awaitingReassignment.
					// Pour l'instant, on ne fait rien de plus ici, le client attendra.
				}
				// Continuer avec les autres chunks dans le rapport.
			} else if needsReplan { // errReplan est nil ici
				if len(newAssignments) > 0 {
					logger.Info("Chunk replan successful, attempting to reassign in StateManager", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "new_primary_agent", newAssignments[0].AgentId)
					if errState := s.config.StateManager.ReassignChunk(ctx, report.PlanId, cs.ChunkInfo.FileId, cs.ChunkInfo.ChunkId, newAssignments); errState != nil {
						logger.Error("Failed to reassign chunk in StateManager after replan", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "error", errState)
					} else {
						logger.Info("Chunk reassigned in StateManager, sending UpdatedTransferPlan", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId)
						updatedPlan := &qwrappb.UpdatedTransferPlan{
							PlanId:                  report.PlanId,
							NewOrUpdatedAssignments: newAssignments,
							// InvalidatedChunks: Pourrait inclure l'ancienne assignation comme invalidée,
							// mais le client devrait gérer cela en voyant une nouvelle assignation pour le même chunkID.
						}
						s.sendUpdatedPlanToClient(report.PlanId, updatedPlan) // Cette méthode envoie au client via le stream stocké
					}
				} else {
					// needsReplan était true, mais MaybeReplanChunk n'a retourné aucune nouvelle affectation (et pas d'erreur ErrNoAgentsAvailable).
					// Cela pourrait être une condition où le scheduler a décidé qu'il n'y avait rien à faire (ex: l'agent va bien).
					logger.Info("Replanning decided no new assignments were needed for chunk", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId)
				}
			} else { // needsReplan était false
				logger.Info("Scheduler determined replanning was not necessary for chunk", "plan_id", report.PlanId, "chunk_id", cs.ChunkInfo.ChunkId, "failed_agent", cs.AgentId)
			}
		}
	}
}

// Adapter handleRegisterForPlanUpdates pour prendre planID et streamCancel
func (s *OrchestratorServer) handleRegisterForPlanUpdates(
	ctx context.Context, // Contexte du stream/connexion
	stream quic.Stream, // Le stream QUIC dédié
	reader framing.Reader, // Déjà utilisé pour lire StreamPurposeRequest, pourrait être réutilisé si d'autres messages sont attendus
	planID string, // Le planID extrait de StreamPurposeRequest
	remoteAddr net.Addr,
	streamOwnedCancel context.CancelFunc, // Le cancel du contexte de routeStream
) {
	// Ce handler prend possession du stream et de son contexte.
	// Il ne doit PAS appeler streamOwnedCancel immédiatement, seulement à la fin de sa propre logique.
	logger := s.config.Logger.With("handler", "RegisterForPlanUpdates", "stream_id", stream.StreamID(), "remote_addr", remoteAddr, "plan_id", planID)

	if planID == "" {
		logger.Warn("RegisterForPlanUpdates called with empty plan_id context")
		streamOwnedCancel() // Annuler le contexte du flux car il est invalide
		_ = stream.Close()  // Fermer le flux directement
		return
	}

	logger.Info("Client stream registered for plan updates", "plan_id", planID)
	s.muClientStreams.Lock()
	if oldStream, exists := s.clientUpdateStreams[planID]; exists && oldStream != stream {
		logger.Info("Closing pre-existing update stream for plan due to new registration", "plan_id", planID)
		// Informer l'ancien stream qu'il est remplacé. QUIC stream.CancelWrite/Read.
		oldStream.CancelWrite(quic.StreamErrorCode(1)) // Code d'erreur applicatif "Replaced"
		oldStream.CancelRead(quic.StreamErrorCode(1))
		// La goroutine associée à oldStream (dans un précédent appel à handleRegisterForPlanUpdates)
		// devrait se terminer en voyant son contexte annulé ou une erreur sur le stream.
	}
	s.clientUpdateStreams[planID] = stream
	s.muClientStreams.Unlock()

	// Garder ce stream ouvert. Le routeStream ne le fermera PAS (grâce à isLongLived).
	// Cette goroutine se termine lorsque le contexte du stream (ctx) est annulé.
	<-ctx.Done() // Attend que le contexte du stream (passé depuis handleConnection) soit terminé

	s.muClientStreams.Lock()
	// Vérifier si c'est toujours CE stream avant de le supprimer, au cas où il aurait été remplacé rapidement.
	if currentStreamInMap, exists := s.clientUpdateStreams[planID]; exists && currentStreamInMap == stream {
		delete(s.clientUpdateStreams, planID)
		logger.Info("Removed client update stream from map due to context done", "plan_id", planID)
	} else if exists {
		logger.Info("Client update stream for plan was already replaced, not removing the new one.", "plan_id", planID)
	} else {
		logger.Info("Client update stream for plan was already removed.", "plan_id", planID)
	}
	s.muClientStreams.Unlock()
	streamOwnedCancel() // Maintenant que le handler a fini avec le stream, annuler son contexte.
	// Le stream.Close() sera fait par le defer de handleConnection car ctx est maintenant Done.
	logger.Debug("handleRegisterForPlanUpdates finished and stream context cancelled", "plan_id", planID)
}

func (s *OrchestratorServer) handleAgentRegistration(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "AgentRegistration", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String()) // .String() pour le log
	var req qwrappb.AgentRegistrationRequest

	// Utiliser un contexte avec timeout pour la lecture du message, dérivé du contexte du stream
	readCtx, readCancel := context.WithTimeout(ctx, orchestratorDefaultReadTimeout)
	defer readCancel()
	if err := reader.ReadMsg(readCtx, &req); err != nil {
		logger.Error("Failed to read AgentRegistrationRequest", "error", err)
		// Ne pas tenter d'écrire une réponse si la lecture a échoué (le flux est peut-être cassé)
		return
	}
	logger.Info("Processing AgentRegistrationRequest", "agent_id", req.AgentId, "address_provided", req.AgentAddress, "capabilities", req.Capabilities)

	if req.AgentId == "" || req.AgentAddress == "" {
		logger.Warn("Invalid AgentRegistrationRequest: missing AgentId or AgentAddress", "agent_id", req.AgentId, "address", req.AgentAddress)
		errResp := &qwrappb.AgentRegistrationResponse{Success: false, Message: "AgentId and AgentAddress are required"}
		writeCtx, writeCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		defer writeCancel()
		if writeErr := writer.WriteMsg(writeCtx, errResp); writeErr != nil {
			logger.Error("Failed to send error response for invalid AgentRegistration", "error", writeErr)
		}
		return
	}

	// Utiliser un contexte pour l'appel au scheduler
	scheduleCtx, scheduleCancel := context.WithTimeout(ctx, 5*time.Second) // Timeout pour l'opération du scheduler
	defer scheduleCancel()
	resp, err := s.config.Scheduler.RegisterAgent(scheduleCtx, &req)
	if err != nil {
		logger.Error("Failed to register agent with scheduler", "agent_id", req.AgentId, "error", err)
		errResp := &qwrappb.AgentRegistrationResponse{Success: false, Message: "Registration failed internally: " + err.Error()}
		writeCtx, writeCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
		defer writeCancel()
		if writeErr := writer.WriteMsg(writeCtx, errResp); writeErr != nil {
			logger.Error("Failed to send error response for AgentRegistration failure", "agent_id", req.AgentId, "error", writeErr)
		}
		return
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, orchestratorDefaultWriteTimeout)
	defer writeCancel()
	if err := writer.WriteMsg(writeCtx, resp); err != nil {
		logger.Error("Failed to send AgentRegistrationResponse to agent", "agent_id", req.AgentId, "error", err)
	} else {
		logger.Info("AgentRegistrationResponse sent", "agent_id", req.AgentId, "success", resp.Success)
		if resp.HeartbeatInterval != nil {
			logger.Info("-> Suggested heartbeat interval", "seconds", resp.HeartbeatInterval.GetSeconds())
		}
	}
}

func (s *OrchestratorServer) handleAgentHeartbeat(ctx context.Context, stream quic.Stream, reader framing.Reader, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "AgentHeartbeat", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())
	var hb qwrappb.AgentHeartbeat

	readCtx, readCancel := context.WithTimeout(ctx, orchestratorDefaultReadTimeout)
	defer readCancel()
	if err := reader.ReadMsg(readCtx, &hb); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
			logger.Debug("Stream closed or context cancelled while reading AgentHeartbeat", "error", err)
		} else {
			logger.Error("Failed to read AgentHeartbeat", "error", err)
		}
		return
	}
	// Décommenter pour log verbeux
	// logger.Debug("Processing AgentHeartbeat", "agent_id", hb.AgentId, "status", hb.Status)

	if hb.AgentId == "" {
		logger.Warn("Received AgentHeartbeat with empty AgentId")
		return
	}

	// Utiliser un contexte pour l'appel au scheduler
	scheduleCtx, scheduleCancel := context.WithTimeout(ctx, 2*time.Second) // Timeout court pour un heartbeat
	defer scheduleCancel()
	if err := s.config.Scheduler.HandleHeartbeat(scheduleCtx, &hb); err != nil {
		// Ne pas logguer comme une erreur de serveur si c'est ErrAgentNotFound, c'est une condition attendue.
		if errors.Is(err, scheduler.ErrAgentNotFound) {
			logger.Info("Scheduler could not handle heartbeat: agent not found or deregistered", "agent_id", hb.AgentId)
		} else {
			logger.Error("Scheduler failed to handle heartbeat", "agent_id", hb.AgentId, "error", err)
		}
	}

	// Généralement, pas de réponse de contenu pour un heartbeat. Le flux se ferme simplement.
	// Si une réponse était nécessaire (ex: HeartbeatResponse avec des commandes), il faudrait l'écrire ici.
}

func (s *OrchestratorServer) sendUpdatedPlanToClient(planID string, updatedPlan *qwrappb.UpdatedTransferPlan) {
	s.muClientStreams.RLock()
	clientStream, exists := s.clientUpdateStreams[planID]
	s.muClientStreams.RUnlock()

	logger := s.config.Logger.With("plan_id", planID, "subcomponent", "update_sender")

	if exists {
		// Créer un writer pour ce stream spécifique
		// Le logger passé au writer sera celui du serveur avec le contexte du planID
		writer := s.config.MessageWriterFactory(clientStream, logger)

		// Utiliser un contexte avec timeout pour l'envoi
		writeCtx, cancel := context.WithTimeout(context.Background(), orchestratorDefaultWriteTimeout)
		defer cancel()

		err := writer.WriteMsg(writeCtx, updatedPlan)
		if err != nil {
			logger.Error("Failed to send UpdatedTransferPlan to client", "error", err)
			// Le stream est peut-être cassé. Le retirer de la map et le fermer.
			s.muClientStreams.Lock()
			// Revérifier au cas où il aurait été remplacé entre RLock et Lock
			if currentStream, stillExists := s.clientUpdateStreams[planID]; stillExists && currentStream == clientStream {
				delete(s.clientUpdateStreams, planID)
			}
			s.muClientStreams.Unlock()
			clientStream.CancelRead(0)  // Indiquer au client de ne plus attendre sur ce flux
			clientStream.CancelWrite(0) // Annuler toute écriture en cours (ne devrait pas y en avoir)
			clientStream.Close()        // Fermer le flux
		} else {
			logger.Info("Successfully sent UpdatedTransferPlan to client")
		}
	} else {
		logger.Warn("No active update stream found for client to send UpdatedTransferPlan")
	}
}
