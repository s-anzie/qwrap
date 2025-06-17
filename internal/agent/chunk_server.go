package chunkserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
	if strings.Contains(fileID, "..") {
		p.logger.Warn("Potential path traversal attempt with '..'", "raw_file_id", fileID)
		return "", ErrPathTraversalAttempt
	}
	if filepath.IsAbs(fileID) {
		p.logger.Warn("Potential path traversal with absolute path", "raw_file_id", fileID)
		return "", ErrPathTraversalAttempt
	}

	absBaseDir, err := filepath.Abs(p.baseDataDir)
	if err != nil {
		p.logger.Error("Failed to get absolute path for baseDataDir", "base_dir", p.baseDataDir, "error", err)
		return "", fmt.Errorf("internal server error: could not resolve base data directory: %w", err)
	}

	// Join and clean the path.
	cleanedPath := filepath.Join(absBaseDir, fileID)

	// Final check to ensure the path is within the base directory.
	if !strings.HasPrefix(cleanedPath, absBaseDir) {
		p.logger.Warn("Path is outside the base directory after cleaning", "cleaned_path", cleanedPath, "base_dir", absBaseDir)
		return "", ErrPathTraversalAttempt
	}
	return cleanedPath, nil
}

func (p *diskFileProvider) GetFileMetadata(fileID string) (int64, time.Time, error) {
	safeFilePath, err := p.getSafeFilePath(fileID)
	if err != nil {
		return 0, time.Time{}, err
	}
	info, err := os.Stat(safeFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, time.Time{}, ErrFileNotFound
		}
		return 0, time.Time{}, fmt.Errorf("stat file %s: %w", fileID, err)
	}
	return info.Size(), info.ModTime(), nil
}

func (p *diskFileProvider) GetFileReadSeeker(portionPathOnDisk string) (io.ReadSeekCloser, error) {
	safeFilePath, err := p.getSafeFilePath(portionPathOnDisk)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(safeFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFoundOnAgent
		}
		return nil, fmt.Errorf("open file %s for read: %w", portionPathOnDisk, err)
	}
	return file, nil
}

// InventoryData wraps the portions and includes the total file size.
type InventoryData struct {
	Portions  []*qwrappb.FilePortionInfo `json:"portions"`
	TotalSize int64                      `json:"file_size"`
}

type Inventory struct {
	mu           sync.RWMutex
	fileMetadata map[string]InventoryData
	portionMap   map[string]*qwrappb.FilePortionInfo
	logger       *slog.Logger
	filePath     string
}

func NewInventory(logger *slog.Logger, inventoryPath string) (*Inventory, error) {
	inv := &Inventory{
		fileMetadata: make(map[string]InventoryData),
		portionMap:   make(map[string]*qwrappb.FilePortionInfo),
		logger:       logger.With("component", "inventory"),
		filePath:     inventoryPath,
	}

	if err := inv.load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load inventory: %w", err)
		}
		inv.logger.Info("Inventory file not found, starting empty", "path", inventoryPath)
	} else {
		inv.logger.Info("Successfully loaded inventory from disk", "path", inv.filePath)
	}
	return inv, nil
}

func (inv *Inventory) load() error {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	content, err := os.ReadFile(inv.filePath)
	if err != nil {
		return err // os.IsNotExist will be checked by the caller
	}

	if len(content) == 0 {
		inv.fileMetadata = make(map[string]InventoryData)
		return nil
	}

	var loadedInventory map[string]InventoryData
	if err := json.Unmarshal(content, &loadedInventory); err != nil {
		return fmt.Errorf("failed to unmarshal inventory JSON from %s: %w", inv.filePath, err)
	}

	inv.fileMetadata = loadedInventory
	inv.portionMap = make(map[string]*qwrappb.FilePortionInfo)
	for _, data := range inv.fileMetadata {
		for _, p := range data.Portions {
			inv.portionMap[p.PathOnDisk] = p
		}
	}
	return nil
}

func (inv *Inventory) GetFileMetadata(fileID string) (InventoryData, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	data, ok := inv.fileMetadata[fileID]
	return data, ok
}

func (inv *Inventory) GetPortionByPath(pathOnDisk string) (*qwrappb.FilePortionInfo, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	portion, ok := inv.portionMap[pathOnDisk]
	return portion, ok
}

type ChunkServer interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Addr() net.Addr
}

type chunkServerImpl struct {
	config    ChunkServerConfig
	listener  *quic.Listener
	inventory *Inventory
	provider  ChunkProvider
	wg        sync.WaitGroup
	mu        sync.Mutex
	closed    bool
}

func NewChunkServer(config ChunkServerConfig) (ChunkServer, error) {
	config.setDefaults()
	if config.AgentId == "" {
		return nil, errors.New("AgentID is mandatory in ChunkServerConfig")
	}
	if config.BaseDataDir == "" {
		return nil, errors.New("BaseDataDir is required")
	}

	inventoryPath := filepath.Join(config.BaseDataDir, "inventory.json")
	inventory, err := NewInventory(config.Logger, inventoryPath)
	if err != nil {
		return nil, fmt.Errorf("could not initialize inventory: %w", err)
	}

	provider := NewDiskFileProvider(config.BaseDataDir, config.Logger)

	return &chunkServerImpl{
		config:    config,
		inventory: inventory,
		provider:  provider,
	}, nil
}

func (s *chunkServerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("server already started")
	}

	quicConf := &quic.Config{
		MaxIdleTimeout:       s.config.ReadTimeout + s.config.WriteTimeout + (5 * time.Second),
		HandshakeIdleTimeout: 10 * time.Second,
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
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		s.config.Logger.Info("Context cancelled, stopping ChunkServer listener...")
		s.mu.Lock()
		if s.listener != nil {
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
			s.mu.Lock()
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
		go s.handleConnection(ctx, conn)
	}
}

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

func (s *chunkServerImpl) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *chunkServerImpl) handleConnection(parentCtx context.Context, conn quic.Connection) {
	defer s.wg.Done()
	defer func() {
		_ = conn.CloseWithError(quic.ApplicationErrorCode(0), "agent connection handler finished")
		s.config.Logger.Debug("Closed QUIC connection post-handling (agent)", "remote_addr", conn.RemoteAddr().String())
	}()
	s.config.Logger.Debug("Agent handling new connection", "remote_addr", conn.RemoteAddr().String())

	for {
		streamCtx, streamCancel := context.WithCancel(parentCtx)

		stream, err := conn.AcceptStream(streamCtx)
		if err != nil {
			streamCancel()
			if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || err == quic.ErrServerClosed || strings.Contains(err.Error(), "connection is closed") {
				s.config.Logger.Debug("Agent connection closed or context cancelled while accepting stream", "remote_addr", conn.RemoteAddr().String(), "error", err)
			} else {
				s.config.Logger.Error("Agent failed to accept stream on connection", "remote_addr", conn.RemoteAddr().String(), "error", err)
			}
			return
		}

		s.config.Logger.Debug("Agent accepted new stream on connection", "remote_addr", conn.RemoteAddr().String(), "stream_id", stream.StreamID())

		s.wg.Add(1)
		go s.dispatchStreamByType(streamCtx, streamCancel, stream, conn.RemoteAddr())
	}
}

func (s *chunkServerImpl) dispatchStreamByType(ctx context.Context, streamCancel context.CancelFunc, stream quic.Stream, remoteAddr net.Addr) {
	defer s.wg.Done()
	defer streamCancel()
	defer stream.Close()

	logger := s.config.Logger.With("remote_addr", remoteAddr.String(), "stream_id", stream.StreamID())
	msgReader := s.config.MessageReaderFactory(stream, logger)
	msgWriter := s.config.MessageWriterFactory(stream, logger)

	var purposeReq qwrappb.StreamPurposeRequest
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
		s.handleChunkRequest(ctx, stream, msgReader, msgWriter, remoteAddr)
	case qwrappb.StreamPurposeRequest_GET_FILE_METADATA_REQUEST:
		s.handleGetFileMetadataRequest(ctx, stream, msgReader, msgWriter, remoteAddr)
	default:
		logger.Warn("Agent received unhandled or unexpected stream purpose", "purpose", purposeReq.GetPurpose())
	}
}

func (s *chunkServerImpl) handleGetFileMetadataRequest(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "GetFileMetadataRequest", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())
	var req qwrappb.GetFileMetadataRequest

	readCtx, readCancel := context.WithTimeout(ctx, s.config.ReadTimeout)
	defer readCancel()
	if err := reader.ReadMsg(readCtx, &req); err != nil {
		logger.Error("Failed to read GetFileMetadataRequest payload", "error", err)
		return
	}
	logger.Info("Processing GetFileMetadataRequest", "request_id", req.RequestId, "file_id", req.FileId)

	if req.FileId == "" {
		logger.Warn("Invalid GetFileMetadataRequest: empty FileId")
		return
	}

	invData, ok := s.inventory.GetFileMetadata(req.FileId)
	var resp *qwrappb.GetFileMetadataResponse
	if !ok {
		logger.Warn("File not found in inventory for metadata request", "file_id", req.FileId)
		resp = &qwrappb.GetFileMetadataResponse{
			RequestId:    req.RequestId,
			AgentId:      s.config.AgentId,
			Found:        false,
			ErrorMessage: "File not found on agent",
		}
	} else {
		logger.Info("Found file in inventory for metadata request", "file_id", req.FileId)
		resp = &qwrappb.GetFileMetadataResponse{
			RequestId:         req.RequestId,
			AgentId:           s.config.AgentId,
			Found:             true,
			AvailablePortions: invData.Portions,
			GlobalFileMetadata: &qwrappb.FileMetadata{
				FileId:       req.FileId,
				TotalSize:    invData.TotalSize,
				LastModified: timestamppb.New(time.Now()), // Note: ModTime is not stored, using current time
			},
		}
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, s.config.WriteTimeout)
	defer writeCancel()
	if err := writer.WriteMsg(writeCtx, resp); err != nil {
		logger.Error("Failed to send GetFileMetadataResponse", "request_id", req.RequestId, "error", err)
	} else {
		logger.Info("GetFileMetadataResponse sent", "request_id", req.RequestId, "found", resp.Found)
	}
}

func (s *chunkServerImpl) handleChunkRequest(ctx context.Context, stream quic.Stream, reader framing.Reader, writer framing.Writer, remoteAddr net.Addr) {
	logger := s.config.Logger.With("handler", "ClientChunkRequest", "stream_id", stream.StreamID(), "remote_addr", remoteAddr.String())

	var req qwrappb.ChunkRequest

	readReqCtx, readReqCancel := context.WithTimeout(ctx, s.config.ReadTimeout)
	errReadReq := reader.ReadMsg(readReqCtx, &req)
	readReqCancel()

	if errReadReq != nil {
		logger.Error("Failed to read ChunkRequest payload", "error", errReadReq)
		return
	}

	if req.ChunkInfoRequested == nil || req.ChunkInfoRequested.Range == nil {
		logger.Warn("Invalid ChunkRequest payload: missing ChunkInfoRequested or its Range field")
		return
	}
	requestedCI := req.ChunkInfoRequested
	logger.Info("Processing ChunkRequest from client",
		"file_id", requestedCI.FileId, "chunk_id", requestedCI.ChunkId,
		"offset", requestedCI.Range.Offset, "length", requestedCI.Range.Length)

	invData, ok := s.inventory.GetFileMetadata(requestedCI.FileId)
	if !ok {
		logger.Error("File not found in inventory for chunk request", "file_id", requestedCI.FileId)
		// Send error back to client
		return
	}

	offset := requestedCI.Range.Offset
	length := requestedCI.Range.Length

	if offset < 0 || length <= 0 || offset+length > invData.TotalSize {
		logger.Warn("Chunk request range is out of bounds for actual file size",
			"file_id", requestedCI.FileId, "req_offset", offset, "req_length", length, "actual_file_size", invData.TotalSize)
		// Send error back to client
		return
	}

	// Find the specific portion file that contains this byte range.
	// This is a simplified logic; a real implementation might need to span multiple portions.
	var targetPortion *qwrappb.FilePortionInfo
	for _, p := range invData.Portions {
		if offset >= p.Offset && (offset+length) <= (p.Offset+p.ChunkSize*int64(p.NumChunks)) {
			targetPortion = p
			break
		}
	}

	if targetPortion == nil {
		logger.Error("Could not find a portion on this agent that contains the requested byte range", "file_id", requestedCI.FileId, "offset", offset, "length", length)
		// Send error back to client
		return
	}

	file, err := s.provider.GetFileReadSeeker(targetPortion.PathOnDisk)
	if err != nil {
		logger.Error("Failed to open portion file on disk", "path", targetPortion.PathOnDisk, "error", err)
		// Send error back to client
		return
	}
	defer file.Close()

	readerAt, ok := file.(io.ReaderAt)
	if !ok {
		logger.Error("File handle does not support ReadAt", "path", targetPortion.PathOnDisk)
		// Send error back to client
		return
	}

	// The offset within the specific portion file
	relativeOffset := offset - targetPortion.Offset

	sectionReader := io.NewSectionReader(readerAt, relativeOffset, length)

	// Send the chunk data
	bytesWritten, err := io.CopyN(stream, sectionReader, length)
	if err != nil {
		logger.Error("Failed to write chunk data to stream", "bytes_written", bytesWritten, "error", err)
		return
	}

	logger.Info("Successfully sent chunk to client", "bytes_sent", bytesWritten)
}
