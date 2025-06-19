package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb"

	"github.com/quic-go/quic-go"
)

type downloadWorker struct {
	id                  int
	connManager         ConnectionManager
	jobsInChan          <-chan downloadJob
	resultsOutChan      chan<- *chunkResult
	logger              *slog.Logger
	writerFactory       func(io.Writer, *slog.Logger) framing.Writer
	readerFactory       func(io.Reader, *slog.Logger) framing.Reader
	chunkRequestTimeout time.Duration
}

func newDownloadWorker(
	id int,
	connMgr ConnectionManager,
	jobsIn <-chan downloadJob,
	resultsOut chan<- *chunkResult,
	baseLogger *slog.Logger, // Renommé pour clarté
	wf func(io.Writer, *slog.Logger) framing.Writer,
	rf func(io.Reader, *slog.Logger) framing.Reader,
	chunkTimeout time.Duration,
) *downloadWorker {
	if wf == nil || rf == nil { // Vérification explicite
		panic("downloadWorker: writerFactory or readerFactory is nil during construction")
	}
	if baseLogger == nil {
		baseLogger = slog.Default() // Sécurité
	}

	return &downloadWorker{
		id:                  id,
		connManager:         connMgr,
		jobsInChan:          jobsIn,
		resultsOutChan:      resultsOut,
		logger:              baseLogger.With("actor", "DownloadWorker", "worker_id", id),
		writerFactory:       wf, // Stocker directement
		readerFactory:       rf, // Stocker directement
		chunkRequestTimeout: chunkTimeout,
	}
}

func (w *downloadWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	w.logger.Info("Worker started.")
	defer w.logger.Info("Worker stopped.")

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Context cancelled, worker shutting down.")
			return
		case job, ok := <-w.jobsInChan:
			if !ok {
				w.logger.Info("Jobs channel closed, worker shutting down.")
				return
			}
			w.processJob(ctx, job)
		}
	}
}

func (w *downloadWorker) processJob(ctx context.Context, job downloadJob) {
	startTime := time.Now()
	chunkInfo := job.assignment.ChunkInfo // job.assignment et chunkInfo ne devraient pas être nil ici
	if chunkInfo == nil {
		w.logger.Error("CRITICAL: processJob received job with nil chunkInfo", "job_assignment_agent", job.assignment.AgentId)
		// Envoyer un résultat d'erreur
		select {
		case w.resultsOutChan <- &chunkResult{
			job:          job,
			err:          errors.New("internal error: job had nil chunkInfo"),
			agentID:      job.assignment.AgentId,
			agentAddress: job.assignment.AgentAddress,
			chunkInfo:    &qwrappb.ChunkInfo{FileId: "unknown", ChunkId: 0}, // Mettre un placeholder
		}:
		case <-ctx.Done():
		}
		return
	}

	w.logger.Debug("Processing job",
		"chunk_id", chunkInfo.ChunkId,
		"agent", job.assignment.AgentId,
		"attempt_on_assign", job.attemptForThisAssignment,
		"total_attempts_for_chunk", job.totalAttemptsForChunk,
	)

	data, err := w.fetchChunkAttempt(ctx, job.assignment)
	downloadDuration := time.Since(startTime)

	// isVerified := false // Checksum ignoré
	// if err == nil && data != nil {
	// 	isVerified = true
	// }

	result := &chunkResult{
		job:          job,
		data:         data,
		err:          err,
		downloadTime: downloadDuration,
		agentID:      job.assignment.AgentId,
		agentAddress: job.assignment.AgentAddress,
		// isVerified:   isVerified,
		chunkInfo: chunkInfo,
	}

	select {
	case w.resultsOutChan <- result:
	case <-ctx.Done():
		w.logger.Warn("Context cancelled while sending chunk result", "chunk_id", chunkInfo.ChunkId)
	}
}

// fetchChunkWithRetry est une version simplifiée de l'ancienne logique fetchChunk,
// mais sans la gestion complexe des retries qui est maintenant dans ResultAggregator.
// Ce worker fait UNE tentative de téléchargement pour l'assignation donnée.
func (w *downloadWorker) fetchChunkAttempt(
	parentCtx context.Context,
	assignment *qwrappb.ChunkAssignment,
) ([]byte, error) {
	var err error
	var conn quic.Connection
	var stream quic.Stream // Déclarée ici pour être dans le scope du defer

	// Vérifier si assignment ou assignment.ChunkInfo est nil au début
	if assignment == nil || assignment.ChunkInfo == nil {
		w.logger.Error("fetchChunkAttempt called with nil assignment or chunkInfo")
		return nil, errors.New("nil assignment or chunkInfo in fetchChunkAttempt")
	}

	fetchCtx, fetchCancel := context.WithTimeout(parentCtx, w.chunkRequestTimeout)
	defer fetchCancel()

	fetchLogger := w.logger.With(
		"op", "fetchChunkAttempt",
		"agent_addr", assignment.AgentAddress,
		"file_id", assignment.ChunkInfo.FileId,
		"chunk_id", assignment.ChunkInfo.ChunkId,
	)

	defer func() {
		if stream != nil {
			// stream.CancelRead(quic.StreamErrorCode(0)) // Peut causer "closing stream with GOAWAY" si déjà fermé
			// stream.CancelWrite(quic.StreamErrorCode(0))
			_ = stream.Close() // Close est plus sûr et idempotent
		}
	}()

	conn, err = w.connManager.GetOrConnect(fetchCtx, assignment.AgentAddress)
	if err != nil {
		fetchLogger.Warn("Failed to get/connect to agent", "error", err)
		if w.connManager != nil {
			invalidateCtx, invalidateCancel := context.WithTimeout(context.Background(), 2*time.Second)
			w.connManager.Invalidate(invalidateCtx, assignment.AgentAddress)
			invalidateCancel()
		}
		return nil, fmt.Errorf("agent connection to %s failed: %w", assignment.AgentAddress, err)
	}

	stream, err = conn.OpenStreamSync(fetchCtx)
	if err != nil {
		fetchLogger.Warn("Failed to open QUIC stream to agent", "error", err)
		if w.connManager != nil {
			invalidateCtx, invalidateCancel := context.WithTimeout(context.Background(), 2*time.Second)
			w.connManager.Invalidate(invalidateCtx, assignment.AgentAddress)
			invalidateCancel()
		}
		return nil, fmt.Errorf("open stream to %s failed: %w", assignment.AgentAddress, err)
	}
	fetchLogger = fetchLogger.With("stream_id", stream.StreamID())

	// Vérifier si writerFactory est nil AVANT de l'utiliser
	if w.writerFactory == nil {
		fetchLogger.Error("CRITICAL: writerFactory is nil in fetchChunkAttempt")
		return nil, errors.New("internal error: writerFactory not initialized in worker")
	}
	msgWriter := w.writerFactory(stream, fetchLogger) // Potentielle ligne de panique 153

	purposeMsg := &qwrappb.StreamPurposeRequest{Purpose: qwrappb.StreamPurposeRequest_CHUNK_REQUEST}
	if err = msgWriter.WriteMsg(fetchCtx, purposeMsg); err != nil {
		return nil, fmt.Errorf("ctx_err: %v, send CHUNK_REQUEST purpose to %s failed: %w", fetchCtx.Err(), assignment.AgentAddress, err)
	}

	req := &qwrappb.ChunkRequest{ChunkInfoRequested: assignment.ChunkInfo}
	if err = msgWriter.WriteMsg(fetchCtx, req); err != nil {
		return nil, fmt.Errorf("ctx_err: %v, send ChunkRequest payload to %s failed: %w", fetchCtx.Err(), assignment.AgentAddress, err)
	}

	// Fermer le côté écriture du flux client pour signaler à l'agent qu'il peut répondre.
	if errStreamClose := stream.Close(); errStreamClose != nil {
		fetchLogger.Debug("Error closing write-side of stream (normal if agent already closed)", "error", errStreamClose)
		// Cette erreur n'est pas nécessairement fatale si l'agent a déjà commencé à envoyer des données
		// et fermé sa partie du flux, ce qui peut fermer le flux entier.
	}

	if assignment.ChunkInfo.Range == nil || assignment.ChunkInfo.Range.Length <= 0 {
		return nil, fmt.Errorf("invalid range in chunk assignment for chunk %d", assignment.ChunkInfo.ChunkId)
	}
	expectedSize := assignment.ChunkInfo.Range.Length
	chunkData := make([]byte, expectedSize)
	totalBytesRead := 0

	readBufSize := 32 * 1024
	if int(expectedSize) < readBufSize {
		readBufSize = int(expectedSize)
		if readBufSize == 0 && expectedSize > 0 { // Éviter un buffer de taille 0 si expectedSize > 0
			readBufSize = int(expectedSize)
		} else if readBufSize == 0 && expectedSize == 0 { // Fichier/chunk vide
			fetchLogger.Debug("Expected chunk size is 0, returning empty data.")
			return []byte{}, nil
		}
	}
	// S'assurer que readBufSize n'est pas 0 si expectedSize > 0
	if readBufSize == 0 && expectedSize > 0 {
		readBufSize = int(expectedSize)
	} else if expectedSize == 0 { // Cas d'un chunk de taille 0
		fetchLogger.Debug("Expected chunk size is 0 for fetch, returning empty data.")
		return []byte{}, nil
	}
	readBuf := make([]byte, readBufSize)

	for int64(totalBytesRead) < expectedSize {
		if fetchCtx.Err() != nil {
			return nil, fmt.Errorf("context error reading chunk %d from %s: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, fetchCtx.Err())
		}

		// Lire dans le buffer temporaire
		nBytesToReadFromStream := len(readBuf)
		if int64(totalBytesRead)+int64(nBytesToReadFromStream) > expectedSize {
			nBytesToReadFromStream = int(expectedSize - int64(totalBytesRead))
		}

		n, readErr := stream.Read(readBuf[:nBytesToReadFromStream])
		if n > 0 {
			if totalBytesRead+n > len(chunkData) {
				return nil, fmt.Errorf("buffer overflow reading chunk %d: read %d, buffer size %d, total read %d", assignment.ChunkInfo.ChunkId, n, len(chunkData), totalBytesRead)
			}
			copy(chunkData[totalBytesRead:], readBuf[:n])
			totalBytesRead += n
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			var streamErr *quic.StreamError
			var idleTimeout *quic.IdleTimeoutError
			var netClosedError net.Error

			if errors.As(readErr, &streamErr) {
				fetchLogger.Warn("QUIC stream error while reading chunk", "error_code", streamErr.ErrorCode, "remote", streamErr.Remote, "message", streamErr.Error())
			} else if errors.As(readErr, &idleTimeout) {
				fetchLogger.Warn("QUIC idle timeout while reading chunk")
			} else if errors.As(readErr, &netClosedError) && netClosedError.Timeout() {
				fetchLogger.Warn("Network timeout while reading chunk", "error", readErr)
			} else if errors.Is(readErr, net.ErrClosed) {
				fetchLogger.Warn("Connection closed while reading chunk (net.ErrClosed)", "error", readErr)
			}
			return nil, fmt.Errorf("read chunk %d data from %s failed: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, readErr)
		}
	}

	if int64(totalBytesRead) < expectedSize {
		return nil, fmt.Errorf("incomplete chunk %d from %s: got %d, want %d: %w",
			assignment.ChunkInfo.ChunkId, assignment.AgentAddress, totalBytesRead, expectedSize, io.ErrUnexpectedEOF)
	}
	if int64(totalBytesRead) > expectedSize {
		fetchLogger.Error("Read more data than expected for chunk", "expected", expectedSize, "received", totalBytesRead)
		return chunkData[:expectedSize], nil
	}

	fetchLogger.Debug("Successfully fetched chunk data", "size", len(chunkData))
	return chunkData, nil
}

// verifyChunkData (déplacé ici car utilisé par le worker)
func verifyChunkData(data []byte, algo qwrappb.ChecksumAlgorithm, expectedChecksumHex string) bool {
	if expectedChecksumHex == "" {
		return true
	}
	var h hash.Hash
	switch algo {
	case qwrappb.ChecksumAlgorithm_SHA256:
		h = sha256.New()
	// TODO: Ajouter d'autres algos si supportés
	default:
		slog.Warn("Unsupported checksum algorithm for chunk verification in worker", "algorithm", algo)
		return false
	}
	_, _ = h.Write(data) // Écrire les données dans le hasher
	actualChecksumHex := hex.EncodeToString(h.Sum(nil))
	return actualChecksumHex == expectedChecksumHex
}
