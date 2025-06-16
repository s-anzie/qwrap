package chunkserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings" // Pour strings.HasPrefix et la validation de chemin
	"sync"
	"time"

	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb" // Assurez-vous que le chemin est correct

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/types/known/timestamppb"
	// Pour le cache (exemple)
	// lru "github.com/hashicorp/golang-lru/v2/simplelru"
	// Pour la limitation de bande passante (exemple)
	// "golang.org/x/time/rate"
)

var (
	ErrServerClosed            = errors.New("chunk server is closed")
	ErrFileNotFoundOnAgent     = errors.New("file not found on agent")                             // Renommé pour clarté
	ErrChunkRequestOutOfBounds = errors.New("requested chunk range is out of bounds for the file") // Renommé
	ErrInvalidChunkRange       = errors.New("invalid chunk range or size in request")
	ErrReadFailedOnAgent       = errors.New("failed to read chunk data from disk on agent") // Renommé
	ErrPathTraversalAttempt    = errors.New("path traversal attempt detected")
	ErrFileNotFound            = errors.New("file not found on agent")
	ErrChunkOutOfBounds        = errors.New("chunk ID is out of bounds for the file")
	ErrReadFailed              = errors.New("failed to read chunk data from disk")
	ErrRateLimitExceeded       = errors.New("rate limit exceeded")          // Pour le futur
	ErrAuthFailed              = errors.New("client authentication failed") // Pour le futur
	// ErrRateLimitExceeded = errors.New("rate limit exceeded") // Pour le futur
	// ErrAuthFailed        = errors.New("client authentication failed") // Pour le futur
)

const (
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second // Timeout pour l'envoi du chunk
	// defaultStreamCreationTimeout = 5 * time.Second // Moins utilisé directement ici
	alpnProtocol = "qwrap"
)

// ChunkServerConfig (Identique à votre version précédente, je la garde pour la complétude du fichier)
type ChunkServerConfig struct {
	ListenAddr   string
	BaseDataDir  string
	TLSConfig    *tls.Config
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// DefaultChunkSize int64 // <<<< Moins pertinent maintenant que ChunkRequest contient ByteRange
	AgentId              string
	Logger               *slog.Logger
	MessageWriterFactory func(w io.Writer, l *slog.Logger) framing.Writer
	MessageReaderFactory func(r io.Reader, l *slog.Logger) framing.Reader
}

func (c *ChunkServerConfig) setDefaults() {
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default().With("component", "chunkserver")
	}
	if c.TLSConfig == nil {
		panic("TLSConfig is mandatory for ChunkServer")
	}
	if c.AgentId == "" {
		panic("AgentID is mandatory for ChunkServerConfig")
	} // Rendre obligatoire

	// ALPN pour le serveur de chunks
	foundALPN := false
	for _, p := range c.TLSConfig.NextProtos {
		if p == alpnProtocol {
			foundALPN = true
			break
		}
	}
	if !foundALPN {
		c.TLSConfig.NextProtos = append(c.TLSConfig.NextProtos, alpnProtocol)
	}

	if c.MessageWriterFactory == nil {
		c.MessageWriterFactory = func(w io.Writer, l *slog.Logger) framing.Writer { /* ... */
			return framing.NewMessageWriter(w, l.With("sub", "cs_msg_writer"))
		}
	}
	if c.MessageReaderFactory == nil {
		c.MessageReaderFactory = func(r io.Reader, l *slog.Logger) framing.Reader { /* ... */
			return framing.NewMessageReader(r, l.With("sub", "cs_msg_reader"))
		}
	}
}

// ChunkProvider (Identique à votre version précédente)
type ChunkProvider interface {
	GetFileMetadata(fileID string) (size int64, modTime time.Time, err error)
	GetFileReadSeeker(fileID string) (io.ReadSeekCloser, error)
}

type diskFileProvider struct {
	baseDataDir string
	logger      *slog.Logger
}

func NewDiskFileProvider(baseDir string, logger *slog.Logger) ChunkProvider {
	if logger == nil {
		logger = slog.Default().With("component", "disk_file_provider")
	}
	return &diskFileProvider{
		baseDataDir: baseDir,
		logger:      logger,
	}
}

// getSafeFilePath nettoie et valide le chemin du fichier.
func (p *diskFileProvider) getSafeFilePath(fileID string) (string, error) {
	if strings.Contains(fileID, "..") || strings.HasPrefix(fileID, "/") || strings.HasPrefix(fileID, "\\") {
		p.logger.Warn("Potential path traversal attempt or invalid FileID", "raw_file_id", fileID)
		return "", ErrPathTraversalAttempt
	}

	absBaseDir, err := filepath.Abs(p.baseDataDir)
	if err != nil {
		p.logger.Error("Failed to get absolute path for baseDataDir", "base_dir", p.baseDataDir, "error", err)
		return "", fmt.Errorf("internal server error: could not resolve base data directory: %w", err)
	}

	// Joindre et nettoyer
	// filepath.Join nettoie déjà certains aspects, mais Clean est plus explicite.
	unsafePath := filepath.Join(p.baseDataDir, fileID)
	cleanedPath := filepath.Clean(unsafePath)

	absCleanedPath, err := filepath.Abs(cleanedPath)
	if err != nil {
		p.logger.Error("Failed to get absolute path for cleanedPath", "cleaned_path", cleanedPath, "error", err)
		return "", fmt.Errorf("internal server error: could not resolve requested file path: %w", err)
	}

	// Vérifier que le chemin final est bien sous le répertoire de base
	if !strings.HasPrefix(absCleanedPath, absBaseDir) || absCleanedPath == absBaseDir && (fileID == "" || fileID == "." || fileID == string(filepath.Separator)) {
		p.logger.Warn("Path traversal attempt detected or resolved path outside base directory",
			"raw_file_id", fileID, "base_dir", absBaseDir, "resolved_path", absCleanedPath)
		return "", ErrPathTraversalAttempt
	}
	return cleanedPath, nil
}

func (p *diskFileProvider) GetFileMetadata(fileID string) (int64, time.Time, error) {
	safeFilePath, err := p.getSafeFilePath(fileID)
	if err != nil {
		return 0, time.Time{}, err // ErrPathTraversalAttempt ou erreur interne
	}

	info, err := os.Stat(safeFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			p.logger.Warn("File not found for metadata", "file_id", fileID, "path", safeFilePath)
			return 0, time.Time{}, ErrFileNotFoundOnAgent
		}
		p.logger.Error("Failed to stat file for metadata", "file_id", fileID, "path", safeFilePath, "error", err)
		return 0, time.Time{}, fmt.Errorf("failed to stat file %s: %w", fileID, err)
	}
	if info.IsDir() {
		p.logger.Warn("FileID refers to a directory", "file_id", fileID, "path", safeFilePath)
		return 0, time.Time{}, fmt.Errorf("fileID %s refers to a directory, not a file", fileID)
	}
	return info.Size(), info.ModTime(), nil
}

func (p *diskFileProvider) GetFileReadSeeker(fileID string) (io.ReadSeekCloser, error) {
	safeFilePath, err := p.getSafeFilePath(fileID)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(safeFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFoundOnAgent
		}
		return nil, fmt.Errorf("open file %s for read: %w", fileID, err)
	}
	return file, nil
}

// ChunkServer (Interface identique)
type ChunkServer interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Addr() net.Addr
}

// FilePortionInventory gère l'inventaire des portions de fichiers détenues par l'agent.
// Pour l'instant, c'est une implémentation simple en mémoire.
type FilePortionInventory struct {
	mu       sync.RWMutex
	portions map[string][]*qwrappb.FilePortionInfo // file_id -> liste des portions
	logger   *slog.Logger
}

// NewFilePortionInventory crée un nouvel inventaire et le peuple avec des données en dur.
func NewFilePortionInventory(logger *slog.Logger, agentID string) *FilePortionInventory {
	inventory := &FilePortionInventory{
		portions: make(map[string][]*qwrappb.FilePortionInfo),
		logger:   logger.With("component", "portion_inventory"),
	}
	inventory.loadHardcodedPortions(agentID)
	return inventory
}

// loadHardcodedPortions simule le chargement de la configuration des portions.
// NOTE: Ceci est une implémentation temporaire en dur pour les tests.
func (inv *FilePortionInventory) loadHardcodedPortions(agentID string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	const (
		fileID10M = "testfile_10M.dat"
		chunkSize = 1 * 1024 * 1024 // 1 MiB
	)

	// Basé sur todo.md, agent001 obtient la première moitié, agent002 la seconde.
	if agentID == "agent001" {
		inv.portions[fileID10M] = []*qwrappb.FilePortionInfo{
			{
				GlobalFileId:    fileID10M,
				Offset:          0,
				NumChunks:       5,
				ChunkSize:       chunkSize,
				ChunkIndexStart: 0,
				ChunkIndexEnd:   4,
			},
		}
		inv.logger.Info("Loaded hardcoded file portions for agent001", "file_id", fileID10M)
	} else if agentID == "agent002" {
		inv.portions[fileID10M] = []*qwrappb.FilePortionInfo{
			{
				GlobalFileId:    fileID10M,
				Offset:          5 * chunkSize,
				NumChunks:       5,
				ChunkSize:       chunkSize,
				ChunkIndexStart: 5,
				ChunkIndexEnd:   9,
			},
		}
		inv.logger.Info("Loaded hardcoded file portions for agent002", "file_id", fileID10M)
	} else {
		inv.logger.Warn("No hardcoded portions defined for this agent ID", "agent_id", agentID)
	}
}

// GetPortionsForFile renvoie les portions détenues pour un fileID donné.
func (inv *FilePortionInventory) GetPortionsForFile(fileID string) []*qwrappb.FilePortionInfo {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if portions, ok := inv.portions[fileID]; ok {
		// Retourne une copie pour la sécurité des accès concurrents
		result := make([]*qwrappb.FilePortionInfo, len(portions))
		copy(result, portions)
		return result
	}
	return nil
}

type chunkServerImpl struct {
	config           ChunkServerConfig
	listener         *quic.Listener
	fileProvider     ChunkProvider
	portionInventory *FilePortionInventory // Inventaire des portions
	wg               sync.WaitGroup
	mu               sync.Mutex
	closed           bool
}

// NewChunkServer crée une nouvelle instance de ChunkServer.
func NewChunkServer(config ChunkServerConfig, provider ChunkProvider) (ChunkServer, error) {
	config.setDefaults()
	if config.AgentId == "" {
		return nil, errors.New("AgentID is mandatory in ChunkServerConfig")
	}
	if provider == nil {
		if config.BaseDataDir == "" {
			return nil, errors.New("BaseDataDir is required if no custom ChunkProvider")
		}
		provider = NewDiskFileProvider(config.BaseDataDir, config.Logger)
	}

	// Créer et initialiser l'inventaire des portions
	portionInventory := NewFilePortionInventory(config.Logger, config.AgentId)

	return &chunkServerImpl{
		config:           config,
		fileProvider:     provider,
		portionInventory: portionInventory,
	}, nil
}

// Start (Identique à la version précédente, avec le logger configuré)
func (s *chunkServerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("server already started")
	}

	quicConf := &quic.Config{
		MaxIdleTimeout:       s.config.ReadTimeout + s.config.WriteTimeout + (5 * time.Second), // Un peu plus pour la marge
		HandshakeIdleTimeout: 10 * time.Second,
		// KeepAlivePeriod: ...
	}

	listener, err := quic.ListenAddr(s.config.ListenAddr, s.config.TLSConfig, quicConf)
	if err != nil {
		s.mu.Unlock()
		s.config.Logger.Error("Failed to start QUIC listener", "address", s.config.ListenAddr, "error", err)
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}
	s.listener = listener
	s.closed = false
	s.mu.Unlock()

	s.config.Logger.Info("ChunkServer started, listening", "address", listener.Addr().String())

	s.wg.Add(1)
	go func() { // Goroutine pour l'arrêt sur annulation du contexte
		defer s.wg.Done()
		<-ctx.Done()
		s.config.Logger.Info("Context cancelled, stopping ChunkServer listener...")
		s.mu.Lock()
		if s.listener != nil {
			// listener.Close() est thread-safe et idempotent.
			if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.config.Logger.Error("Error closing QUIC listener on context cancel", "error", err)
			}
			s.listener = nil
		}
		s.closed = true
		s.mu.Unlock()
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			s.mu.Lock() // Utiliser RLock car on ne modifie `closed` que via la goroutine ci-dessus
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
				s.config.Logger.Info("ChunkServer listener accept loop ending.", "reason", err)
				return nil
			}
			s.config.Logger.Error("Failed to accept QUIC connection", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.config.Logger.Info("Accepted new QUIC connection", "remote_addr", conn.RemoteAddr().String())
		s.wg.Add(1)
		go s.handleConnection(ctx, conn) // Utiliser le ctx du Start pour les nouvelles connexions
	}
}

// Stop (Identique à la version précédente)
func (s *chunkServerImpl) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.config.Logger.Info("ChunkServer Stop called but already closed.")
		return nil
	}
	s.closed = true
	listenerToClose := s.listener
	s.listener = nil
	s.mu.Unlock()
	s.config.Logger.Info("Stopping ChunkServer...")
	var err error
	if listenerToClose != nil {
		if e := listenerToClose.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
			err = e
			s.config.Logger.Error("Error closing QUIC listener during Stop", "error", e)
		}
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.config.Logger.Info("All active connections handled, ChunkServer stopped.")
	case <-ctx.Done():
		s.config.Logger.Warn("ChunkServer stop timed out", "error", ctx.Err())
		return ctx.Err()
	}
	return err
}

// Addr (Identique)
func (s *chunkServerImpl) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock() // RLock car listener est modif seulement ds Start/Stop
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// handleConnection (Identique, avec des logs plus précis)
func (s *chunkServerImpl) handleConnection(parentCtx context.Context, conn quic.Connection) {
	defer s.wg.Done()
	defer func() {
		// S'assurer que la connexion est fermée proprement à la fin du handler de connexion
		_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Utiliser un code d'erreur applicatif 0 pour une fermeture normale par le handler
		_ = conn.CloseWithError(quic.ApplicationErrorCode(0), "agent connection handler finished")
		s.config.Logger.Debug("Closed QUIC connection post-handling (agent)", "remote_addr", conn.RemoteAddr().String())
	}()
	s.config.Logger.Debug("Agent handling new connection", "remote_addr", conn.RemoteAddr().String())

	for {
		// Accepter un nouveau flux sur cette connexion
		// Utiliser parentCtx (le contexte de la connexion) pour AcceptStream
		streamCtx, streamCancel := context.WithCancel(parentCtx) // Nouveau contexte pour chaque stream

		stream, err := conn.AcceptStream(streamCtx)
		if err != nil {
			streamCancel() // Annuler le contexte du stream si AcceptStream échoue
			// Gérer les erreurs d'AcceptStream
			if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || err == quic.ErrServerClosed || strings.Contains(err.Error(), "connection is closed") {
				s.config.Logger.Debug("Agent connection closed or context cancelled while accepting stream", "remote_addr", conn.RemoteAddr().String(), "error", err)
			} else if _, ok := err.(*quic.IdleTimeoutError); ok {
				s.config.Logger.Info("Agent connection idle timeout", "remote_addr", conn.RemoteAddr().String())
			} else if appErr, ok := err.(*quic.ApplicationError); ok && appErr.Remote {
				s.config.Logger.Info("Agent connection closed by remote with application error", "remote_addr", conn.RemoteAddr().String(), "error_code", appErr.ErrorCode, "error_msg", appErr.ErrorMessage)
			} else {
				s.config.Logger.Error("Agent failed to accept stream on connection", "remote_addr", conn.RemoteAddr().String(), "error", err)
			}
			return // Fin du traitement de cette connexion si AcceptStream échoue ou si le contexte est annulé
		}

		s.config.Logger.Debug("Agent accepted new stream on connection", "remote_addr", conn.RemoteAddr().String(), "stream_id", stream.StreamID())

		s.wg.Add(1)
		// Passer streamCtx et streamCancel à la goroutine qui gère le flux.
		// dispatchStreamByType sera maintenant responsable d'appeler streamCancel via son propre defer.
		go s.dispatchStreamByType(streamCtx, streamCancel, stream, conn.RemoteAddr()) // <<<< APPEL CRUCIAL ICI
	}
}



// handleGetFileMetadataRequest is called by dispatchStreamByType after a StreamPurposeRequest_GET_FILE_METADATA_REQUEST has been read.
func (s *chunkServerImpl) handleGetFileMetadataRequest(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "GetFileMetadataRequest", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())
	var req qwrappb.GetFileMetadataRequest

	// Use a timeout for reading the request.
	readCtx, readCancel := context.WithTimeout(ctx, s.config.ReadTimeout)
	defer readCancel()
	if err := reader.ReadMsg(readCtx, &req); err != nil {
		logger.Error("Failed to read GetFileMetadataRequest payload", "error", err)
		return
	}
	logger.Info("Processing GetFileMetadataRequest", "request_id", req.RequestId, "file_id", req.FileId)

	if req.FileId == "" {
		logger.Warn("Invalid GetFileMetadataRequest: empty FileId")
		// We don't need to send an error response here, the client should handle the stream closing.
		return
	}

	// Get portions from the inventory. This is the main new logic.
	portions := s.portionInventory.GetPortionsForFile(req.FileId)
	logger.Debug("Found portions for file", "file_id", req.FileId, "num_portions", len(portions))

	resp := &qwrappb.GetFileMetadataResponse{
		RequestId:         req.RequestId,
		AgentId:           s.config.AgentId,
		AvailablePortions: portions, // The portions this agent has.
	}

	// Also get global file metadata if available.
	fileSize, modTime, errMeta := s.fileProvider.GetFileMetadata(req.FileId)
	if errMeta == nil {
		resp.Found = true
		resp.GlobalFileMetadata = &qwrappb.FileMetadata{
			FileId:       req.FileId,
			TotalSize:    fileSize,
			LastModified: timestamppb.New(modTime),
		}
	} else {
		logger.Warn("Could not get global file metadata", "file_id", req.FileId, "error", errMeta)
		if len(portions) > 0 {
			// If we have portions but no global metadata, it's a partial success.
			resp.Found = true // We found *something* (portions).
		} else {
			resp.Found = false
			if errors.Is(errMeta, ErrFileNotFoundOnAgent) {
				resp.ErrorMessage = "File not found on agent"
			} else {
				resp.ErrorMessage = "Internal error getting file metadata"
			}
		}
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
	defer writeCancel()
	if err := writer.WriteMsg(writeCtx, resp); err != nil {
		logger.Error("Failed to send GetFileMetadataResponse", "request_id", req.RequestId, "error", err)
	} else {
		logger.Info("GetFileMetadataResponse sent", "request_id", req.RequestId, "found", resp.Found, "num_portions", len(resp.AvailablePortions))
	}
}

func (s *chunkServerImpl) dispatchStreamByType(ctx context.Context, streamCancel context.CancelFunc, stream quic.Stream, remoteAddr net.Addr) {
	defer s.wg.Done()
	defer streamCancel() // Ensure the stream context is always cancelled.
	defer stream.Close()   // Ensure the stream is always closed.

	logger := s.config.Logger.With("remote_addr", remoteAddr.String(), "stream_id", stream.StreamID())
	msgReader := s.config.MessageReaderFactory(stream, logger)
	msgWriter := s.config.MessageWriterFactory(stream, logger)

	var purposeReq qwrappb.StreamPurposeRequest
	// Use a timeout for reading the purpose.
	readPurposeCtx, readPurposeCancel := context.WithTimeout(ctx, s.config.ReadTimeout)
	defer readPurposeCancel()
	if err := msgReader.ReadMsg(readPurposeCtx, &purposeReq); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Debug("Stream closed or timed out before purpose received", "error", err)
		} else {
			logger.Error("Failed to read stream purpose", "error", err)
		}
		return
	}

	logger.Info("Received stream purpose", "purpose", purposeReq.GetPurpose())

	switch purposeReq.GetPurpose() {
	case qwrappb.StreamPurposeRequest_CHUNK_REQUEST:
		s.handleChunkRequestFromClient(ctx, stream, msgReader, msgWriter, remoteAddr)
	case qwrappb.StreamPurposeRequest_GET_FILE_METADATA_REQUEST:
		s.handleGetFileMetadataRequest(ctx, stream, msgReader, msgWriter, remoteAddr)
	default:
		logger.Warn("Agent received unhandled or unexpected stream purpose", "purpose", purposeReq.GetPurpose())
	}
}

// handleChunkRequestFromClient est appelé par dispatchStreamByType après qu'un StreamPurposeRequest_CHUNK_REQUEST a été lu.
// Il lit ensuite le message ChunkRequest et sert le chunk.
func (s *chunkServerImpl) handleChunkRequestFromClient(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	// Le defer pour stream.Close() et streamCancel() est dans dispatchStreamByType car ce handler est court et ne gère pas de flux long-lived.
	logger := s.config.Logger.With("handler", "ClientChunkRequest", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())

	var req qwrappb.ChunkRequest // Le client envoie ChunkRequest après le StreamPurpose

	// Lire le message ChunkRequest lui-même
	readReqCtx, readReqCancel := context.WithTimeout(ctx, s.config.ReadTimeout)
	errReadReq := reader.ReadMsg(readReqCtx, &req)
	readReqCancel()

	if errReadReq != nil {
		logFields := []any{"error", errReadReq}
		if errors.Is(errReadReq, io.EOF) || errors.Is(errReadReq, net.ErrClosed) || errors.Is(errReadReq, context.Canceled) {
			logger.Debug("Failed to read ChunkRequest payload (EOF/Closed/Cancelled)", logFields...)
		} else {
			logger.Error("Failed to read ChunkRequest payload", logFields...)
		}
		// Ne pas tenter d'envoyer une erreur Protobuf ici car le flux pourrait être déjà cassé.
		// Le client devrait gérer le timeout ou la fermeture du flux.
		return
	}

	// Validation de la requête reçue
	if req.ChunkInfoRequested == nil || req.ChunkInfoRequested.Range == nil {
		logger.Warn("Invalid ChunkRequest payload: missing ChunkInfoRequested or its Range field")
		errMsg := &qwrappb.ChunkErrorResponse{
			ErrorCode: qwrappb.ChunkErrorCode_CHUNK_ERROR_UNKNOWN, // Ou un code d'erreur plus spécifique pour "bad request"
			Message:   "Malformed ChunkRequest payload: ChunkInfoRequested or its Range field is missing.",
		}
		if req.ChunkInfoRequested != nil { // Tenter de remplir les IDs pour le contexte si possible
			errMsg.FileId = req.ChunkInfoRequested.FileId
			errMsg.ChunkId = req.ChunkInfoRequested.ChunkId
		}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg) // Tenter d'envoyer une erreur Protobuf
		writeErrCancel()
		return
	}
	requestedCI := req.ChunkInfoRequested
	logger.Info("Processing ChunkRequest from client",
		"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId,
		"offset", requestedCI.Range.Offset, "length", requestedCI.Range.Length)

	if requestedCI.FileId == "" {
		logger.Warn("Invalid ChunkRequest: empty FileId")
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_CHUNK_ERROR_UNKNOWN, Message: "FileId cannot be empty in ChunkInfoRequested"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}
	if requestedCI.Range.Length <= 0 {
		logger.Warn("Invalid ChunkRequest: non-positive length in ByteRange", "file_id", requestedCI.FileId, "length", requestedCI.Range.Length)
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_CHUNK_ERROR_UNKNOWN, Message: "Invalid chunk length in ByteRange (must be > 0)"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}

	// Valider le chemin du fichier (getSafeFilePath est une méthode de diskFileProvider)
	// Pour un ChunkProvider générique, cette logique devrait être dans le provider ou l'agent doit vérifier.
	dp, ok := s.fileProvider.(*diskFileProvider)
	if !ok {
		logger.Error("Internal agent error: fileProvider is not a diskFileProvider; cannot validate path for chunk request.")
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_INTERNAL_AGENT_ERROR, Message: "Internal server configuration error (file provider type)"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}
	safeFilePath, errSafePath := dp.getSafeFilePath(requestedCI.FileId)
	if errSafePath != nil {
		logger.Error("File path validation failed for chunk request", "file_id", requestedCI.FileId, "raw_path_attempt", requestedCI.FileId, "error", errSafePath)
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_CHUNK_NOT_FOUND, Message: "Invalid or forbidden file identifier"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}

	// Obtenir la taille réelle du fichier pour valider le Range demandé
	fileSize, _, errMeta := s.fileProvider.GetFileMetadata(requestedCI.FileId) // FileID est déjà "safe" car getSafeFilePath a été appelé indirectement
	if errMeta != nil {
		errCode := qwrappb.ChunkErrorCode_INTERNAL_AGENT_ERROR
		errMsgText := "Cannot get file metadata for chunk request"
		if errors.Is(errMeta, ErrFileNotFoundOnAgent) { // Utiliser l'erreur exportée
			errCode = qwrappb.ChunkErrorCode_CHUNK_NOT_FOUND
			errMsgText = "File not found on agent"
		}
		logger.Error("Failed to get file metadata for chunk validation", "file_id", requestedCI.FileId, "error", errMeta)
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: errCode, Message: errMsgText}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}

	offset := requestedCI.Range.Offset
	length := requestedCI.Range.Length // C'est la longueur que le client s'attend à recevoir

	if offset < 0 || offset >= fileSize || (offset+length > fileSize) {
		// Si offset+length == fileSize, c'est ok (dernier chunk).
		// Si offset+length > fileSize, length doit être ajusté, mais le client demande un range précis.
		// Donc, si le range demandé dépasse la taille du fichier, c'est une erreur du client ou du plan.
		logger.Warn("Chunk request range is out of bounds for actual file size",
			"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId,
			"req_offset", offset, "req_length", length, "actual_file_size", fileSize)
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_CHUNK_NOT_FOUND, Message: "Requested chunk range is out of bounds for the file"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}
	// Si on arrive ici, le range demandé (offset, length) est valide par rapport à fileSize.

	// Ouvrir le fichier en utilisant le chemin sécurisé déjà validé
	file, errOpen := os.Open(safeFilePath)
	if errOpen != nil {
		logger.Error("Failed to open file to serve chunk", "file_path", safeFilePath, "error", errOpen)
		errMsg := &qwrappb.ChunkErrorResponse{FileId: requestedCI.FileId, ChunkId: requestedCI.ChunkId, ErrorCode: qwrappb.ChunkErrorCode_INTERNAL_AGENT_ERROR, Message: "Could not open source file on agent"}
		writeErrCtx, writeErrCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
		_ = writer.WriteMsg(writeErrCtx, errMsg)
		writeErrCancel()
		return
	}
	defer file.Close()

	// Utiliser NewSectionReader pour lire et servir exactement la portion demandée.
	sectionReader := io.NewSectionReader(file, offset, length)
	expectedLengthToStream := length // C'est ce qu'on s'attend à envoyer

	logger.Debug("Attempting to stream chunk data to client", "file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId, "expected_length", expectedLengthToStream)

	// Contexte pour l'opération de copie avec le WriteTimeout configuré
	streamWriteCtx, streamWriteCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
	defer streamWriteCancel()

	var bytesWritten int64
	var copyErr error
	copyDoneChan := make(chan struct{}) // Pour signaler la fin de io.CopyN

	go func() {
		defer close(copyDoneChan)
		// io.CopyN tentera de copier exactement expectedLengthToStream.
		// Si sectionReader a moins de données (ne devrait pas arriver si offset+length <= fileSize),
		// io.CopyN retournera io.EOF ou io.ErrUnexpectedEOF après avoir copié ce qu'il pouvait.
		bytesWritten, copyErr = io.CopyN(stream, sectionReader, expectedLengthToStream)
	}()

	select {
	case <-copyDoneChan:
		// io.CopyN s'est terminé (avec ou sans copyErr)
	case <-streamWriteCtx.Done(): // Timeout d'écriture spécifique atteint
		copyErr = streamWriteCtx.Err() // Sera context.DeadlineExceeded
		logger.Warn("Streaming chunk data write operation timed out by streamWriteCtx",
			"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId, "timeout", s.config.WriteTimeout, "error", copyErr)
		stream.CancelWrite(quic.StreamErrorCode(1)) // Code d'erreur applicatif pour "write timeout"
	case <-ctx.Done(): // Contexte global du stream/handler annulé
		copyErr = ctx.Err()
		logger.Info("Streaming chunk data cancelled by parent stream context",
			"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId, "error", copyErr)
		// stream.CancelWrite sera fait par le defer de dispatchStreamByType si le stream est fermé.
	}

	if copyErr != nil {
		logFields := []any{
			"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId,
			"bytes_actually_written_to_stream", bytesWritten, "expected_length", expectedLengthToStream, "copy_error", copyErr,
		}
		if errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			logger.Info("Streaming chunk data to client was cancelled or timed out during io.CopyN", logFields...)
		} else if errors.Is(copyErr, io.EOF) && bytesWritten < expectedLengthToStream {
			logger.Error("Streaming chunk data failed: unexpected EOF from source file (sectionReader provided less data than chunk length)", logFields...)
		} else if errors.Is(copyErr, io.EOF) && bytesWritten == expectedLengthToStream {
			// Ceci est considéré comme un succès par io.CopyN, donc copyErr devrait être nil dans ce cas.
			// Mais si copyErr est io.EOF, on le logue pour être sûr.
			logger.Debug("Streaming chunk data completed, io.CopyN returned expected EOF at exact length.", logFields...)
			copyErr = nil // Traiter comme un succès
		} else {
			logger.Error("Streaming chunk data to client failed with an unexpected io.CopyN error", logFields...)
		}
		// À ce stade, il est généralement trop tard pour envoyer un ChunkErrorResponse fiable,
		// car des données partielles ont pu être envoyées ou le stream est dans un état inconnu.
		// Le client devra gérer la réception de données incomplètes ou la fermeture abrupte du flux.
		return
	}

	// Double vérification : io.CopyN sans erreur devrait avoir écrit expectedLengthToStream.
	if bytesWritten != expectedLengthToStream {
		logger.Error("CRITICAL: Mismatch in bytes streamed for chunk (io.CopyN returned no error but wrote different amount)",
			"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId,
			"written", bytesWritten, "expected", expectedLengthToStream)
		// Ceci indique un problème sérieux, potentiellement dans io.CopyN ou la gestion du stream QUIC.
		stream.CancelWrite(quic.StreamErrorCode(2)) // Code d'erreur "internal error"
		return
	}

	logger.Info("Successfully streamed chunk to client",
		"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId, "size_streamed", bytesWritten)

	// L'agent a fini d'écrire les données du chunk.
	// Le flux sera fermé par le defer de la fonction dispatchStreamByType.
	// La fermeture du flux enverra le signal STREAM_FIN au client, lui indiquant que toutes les données pour ce flux ont été envoyées.
}
