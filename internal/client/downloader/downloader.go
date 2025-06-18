package downloader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net" // Pour errors.Is(err, net.ErrClosed)
	"os"
	"sort" // Pour la validation de chemin
	"sync"
	"sync/atomic"
	"time"

	"qwrap/internal/client/orchestratorclient"
	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb" // Assurez-vous que le chemin est correct

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultConcurrency         = 10
	defaultMaxLocalRetries     = 2 // Nombre de tentatives locales avant de compter sur l'orchestrateur
	defaultRetryBaseDelayChunk = 250 * time.Millisecond
	defaultChunkRequestTimeout = 20 * time.Second // Augmenté un peu
	chunkReassignmentTimeout   = 30 * time.Second
	filePermission             = 0644
	orchestratorCommsTimeout   = 10 * time.Second // Timeout pour les opérations de comms avec l'orchestrateur
)

var (
	ErrDownloadCancelled     = errors.New("download cancelled by context")
	ErrChecksumMismatch      = errors.New("checksum mismatch")
	ErrOrchestratorPlan      = errors.New("invalid transfer plan from orchestrator")
	ErrFileWriteFailed       = errors.New("failed to write to destination file")
	ErrChunkDownloadFailed   = errors.New("chunk download failed after all attempts")
	ErrOrchestratorCommsFail = errors.New("communication with orchestrator failed")
)

// chunkToWrite is a struct used to pass data for a completed chunk to the file writer goroutine.
// chunkToWrite is a struct used to pass a completed chunk to the file writer goroutine.
// It contains all information needed for the writer to perform its job.
type chunkToWrite struct {
	chunkID uint64
	data    []byte
	offset  int64
}

// OrchestratorComms définit l'interface pour la communication du Downloader avec l'Orchestrateur.
type OrchestratorComms interface {
	// ReportChunkTransferStatus envoie le statut d'un chunk à l'orchestrateur.
	ReportChunkTransferStatus(ctx context.Context, planID string, status *qwrappb.ChunkTransferStatus) error

	// ListenForUpdatedPlans écoute les mises à jour de plan de l'orchestrateur pour un planID donné.
	// Les mises à jour sont envoyées sur updatedPlanChan.
	// Cette fonction doit être non-bloquante ou exécutée dans une goroutine.
	// Elle doit se terminer lorsque le contexte est annulé.
	ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error

	// (Optionnel) RequestChunkReassignment pourrait être une méthode explicite.
	// RequestChunkReassignment(ctx context.Context, planID string, chunkInfo *qwrappb.ChunkInfo, lastFailedAgentID string) error
}

type downloaderConfig struct {
	Concurrency            int
	MaxLocalRetries        int
	RetryBaseDelay         time.Duration
	ChunkRequestTimeout    time.Duration
	Logger                 *slog.Logger
	ConnManager            ConnectionManager
	OrchestratorComms      orchestratorclient.Comms
	WriterFactory          func(io.Writer, *slog.Logger) framing.Writer
	ReaderFactory          func(io.Reader, *slog.Logger) framing.Reader
	BackpressureStrategy   BackpressureStrategy
	TokenPermits           int
}

func (c *downloaderConfig) setDefaults(logger *slog.Logger) {
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.MaxLocalRetries < 0 {
		c.MaxLocalRetries = defaultMaxLocalRetries
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = defaultRetryBaseDelayChunk
	}
	if c.ChunkRequestTimeout <= 0 {
		c.ChunkRequestTimeout = defaultChunkRequestTimeout
	}

	if logger == nil {
		c.Logger = slog.Default().With("component", "downloader")
	} else {
		c.Logger = logger.With("component", "downloader")
	}

	if c.ConnManager == nil {
		panic("ConnectionManager is required for downloader")
	}

	if c.OrchestratorComms == nil {
		c.OrchestratorComms = orchestratorclient.NewNoOpComms(c.Logger)
	}

	if c.WriterFactory == nil {
		c.WriterFactory = func(w io.Writer, l *slog.Logger) framing.Writer {
			return framing.NewMessageWriter(w, l.With("subcomponent", "dl_msg_writer"))
		}
	}
	if c.ReaderFactory == nil {
		c.ReaderFactory = func(r io.Reader, l *slog.Logger) framing.Reader {
			return framing.NewMessageReader(r, l.With("subcomponent", "dl_msg_reader"))
		}
	}

	if c.BackpressureStrategy == "" {
		c.BackpressureStrategy = TokenBackpressure
	}
	if c.TokenPermits <= 0 {
		c.TokenPermits = c.Concurrency + 2
	}
}

// noopOrchestratorComms est une implémentation factice pour quand la vraie n'est pas fournie.
type noopOrchestratorComms struct{ logger *slog.Logger }

func (n *noopOrchestratorComms) ReportChunkTransferStatus(ctx context.Context, planID string, status *qwrappb.ChunkTransferStatus) error {
	n.logger.Debug("NO-OP: ReportChunkTransferStatus called", "plan_id", planID, "chunk_id", status.ChunkInfo.ChunkId, "event", status.Event)
	return nil
}
func (n *noopOrchestratorComms) ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error {
	n.logger.Debug("NO-OP: ListenForUpdatedPlans called, will block until context done.", "plan_id", planID)
	<-ctx.Done() // Bloque pour simuler une écoute
	return ctx.Err()
}

type downloadWorkerJob struct {
	assignment *qwrappb.ChunkAssignment // L'affectation actuelle pour ce job
	attempt    int                      // Tentative actuelle pour CETTE affectation
}

// chunkTrack garde l'état d'un chunk en cours de téléchargement ou en attente.
type chunkTrack struct {
	info                         *qwrappb.ChunkInfo
	currentAssignment            *qwrappb.ChunkAssignment // La dernière affectation connue pour ce chunk
	currentAttemptOnAssign       int                      // Tentative pour l'affectation actuelle
	data                         []byte
	isCompleted                  bool
	isPermanentlyFailed          bool
	localRetryTotalCount         int // Nombre total de tentatives locales pour ce chunk, toutes assignations confondues
	awaitingReassignment         bool
	lastAwaitingReassignmentTime time.Time
}

type downloaderImpl struct {
	config              downloaderConfig
	currentPlanSnapshot *qwrappb.TransferPlan // Protégé par muCurrentPlan
	muCurrentPlan       sync.RWMutex

	// Shutdown signaling
	shutdownOnce sync.Once
	shutdownChan chan struct{} // Closed to signal shutdown to the main loop
	doneChan     chan struct{} // Closed by the main loop when it has fully exited
}

// NewDownloader crée une nouvelle instance de Downloader.
func NewDownloader(
	connMgr ConnectionManager,
	orchComms orchestratorclient.Comms,
	logger *slog.Logger,
	concurrency int,
	writerFactory func(io.Writer, *slog.Logger) framing.Writer,
	readerFactory func(io.Reader, *slog.Logger) framing.Reader,
) Downloader {
	cfg := downloaderConfig{
		ConnManager:       connMgr,
		OrchestratorComms: orchComms,
		Concurrency:       concurrency,
		WriterFactory:     writerFactory,
		ReaderFactory:     readerFactory,
	}
	cfg.setDefaults(logger)
	return &downloaderImpl{
		config:       cfg,
		shutdownChan: make(chan struct{}),
		doneChan:     make(chan struct{}),
	}
}

// Download (interface)
// Download is the main public method to start a file download.
// It initiates the download process in a separate goroutine and returns channels for progress and final error.
// destinationWriter est une interface qui combine io.WriterAt et Truncate.
// Cela nous permet de pré-allouer de l'espace disque tout en utilisant des mocks dans les tests.
type destinationWriter interface {
	io.WriterAt
	Truncate(size int64) error
}

func (d *downloaderImpl) Download(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	dest DestinationWriter,
) (<-chan ProgressInfo, <-chan error) {
	progressChan := make(chan ProgressInfo, d.config.Concurrency*2)
	errChan := make(chan error, 1)

	go func() {
		defer close(progressChan)
		defer close(errChan)

		if len(initialTransferReq.GetFilesToTransfer()) == 0 || initialTransferReq.GetFilesToTransfer()[0] == nil {
			errChan <- errors.New("invalid transfer request: missing file metadata")
			return
		}
		fileID := initialTransferReq.GetFilesToTransfer()[0].GetFileId()

		l := d.config.Logger.With("op", "Download", "file_id", fileID)
		l.Info("Starting download process")

		// Le chemin de destination est maintenant abstrait par l'io.Writer.
		// La vérification du checksum nécessitera une approche différente si elle est effectuée ici.
				if err := d.downloadToWriter(ctx, initialTransferReq, dest, "", progressChan); err != nil {
			errChan <- err
		}
	}()

	return progressChan, errChan
}

// Shutdown initiates a graceful shutdown of the downloader.
func (d *downloaderImpl) Shutdown(timeout time.Duration) error {
	d.config.Logger.Info("Shutdown requested for downloader.")
	d.shutdownOnce.Do(func() {
		close(d.shutdownChan)
	})

	select {
	case <-d.doneChan:
		d.config.Logger.Info("Downloader shutdown completed gracefully.")
		return nil
	case <-time.After(timeout):
		d.config.Logger.Error("Downloader shutdown timed out.")
		return errors.New("downloader shutdown timed out")
	}
}

// downloadToWriter contains the core logic for running a download to a given writer.
// This is the internal, testable entry point.
func (d *downloaderImpl) downloadToWriter(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	destWriter destinationWriter,
	destPath string, // Pass destPath for lifecycle management
	progressChan chan<- ProgressInfo,
) error {
	if len(initialTransferReq.GetFilesToTransfer()) == 0 || initialTransferReq.GetFilesToTransfer()[0] == nil {
		return errors.New("invalid transfer request: missing file metadata")
	}
	fileID := initialTransferReq.GetFilesToTransfer()[0].GetFileId()

	l := d.config.Logger.With("op", "downloadToWriter", "file_id", fileID)
	l.Info("Starting download to writer")

	finalErrorChan := make(chan error, 1)
	go d.manageDownloadLifecycle(ctx, initialTransferReq, destWriter, destPath, progressChan, finalErrorChan)

	select {
	case err := <-finalErrorChan:
		if err != nil {
			l.Error("Download failed", "error", err)
			return err
		}
		l.Info("Download completed successfully.")
		return nil
	case <-ctx.Done():
		l.Info("Download cancelled by caller.")
		return ctx.Err()
	}
}

// manageDownloadLifecycle is the main goroutine that manages the entire download process.
func (d *downloaderImpl) manageDownloadLifecycle(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	destWriter destinationWriter,
	destPath string, // Add destPath to handle empty file creation
	progressChan chan<- ProgressInfo,
	finalErrorChan chan<- error,
) {
	// Ensure channels are closed on exit to signal completion to the caller.
	defer close(finalErrorChan)
	defer close(d.doneChan) // Signal that this lifecycle goroutine has finished.

	l := d.config.Logger.With("request_id", initialTransferReq.RequestId, "op", "manageDownloadLifecycle")
	l.Info("Downloader lifecycle started")

	// --- Initial Plan Retrieval ---
	planCtx, planCancel := context.WithTimeout(ctx, orchestratorCommsTimeout*2) // Double pour lecture et écriture
	initialPlan, err := d.config.OrchestratorComms.RequestInitialPlan(planCtx, initialTransferReq)
	planCancel()

	if err != nil {
		wrappedErr := fmt.Errorf("%w: %v", ErrOrchestratorPlan, err)
		l.Error("Failed to get initial transfer plan", "error", wrappedErr)
		finalErrorChan <- wrappedErr
		return
	}
	if initialPlan == nil {
		err = fmt.Errorf("%w: received nil plan from orchestrator", ErrOrchestratorPlan)
		l.Error(err.Error())
		finalErrorChan <- err
		return
	}
	if initialPlan.ErrorMessage != "" {
		err = fmt.Errorf("%w: orchestrator error in plan: %s", ErrOrchestratorPlan, initialPlan.ErrorMessage)
		l.Error(err.Error(), "plan_id", initialPlan.PlanId)
		finalErrorChan <- err
		return
	}
	if len(initialPlan.SourceFileMetadata) == 0 || initialPlan.SourceFileMetadata[0] == nil {
		err = fmt.Errorf("%w: initial plan missing source file metadata", ErrOrchestratorPlan)
		l.Error(err.Error(), "plan_id", initialPlan.PlanId)
		finalErrorChan <- err
		return
	}

	mainFileMeta := initialPlan.SourceFileMetadata[0]
	d.muCurrentPlan.Lock()
	d.currentPlanSnapshot = initialPlan
	d.muCurrentPlan.Unlock()
	l = l.With("plan_id", initialPlan.PlanId, "file_id", mainFileMeta.FileId)

	// --- Handle Empty File Case ---
	if len(initialPlan.ChunkAssignments) == 0 {
		if mainFileMeta.TotalSize > 0 {
			err = fmt.Errorf("%w: initial plan has no chunk assignments for a non-empty file (size: %d)", ErrOrchestratorPlan, mainFileMeta.TotalSize)
			l.Error(err.Error())
			finalErrorChan <- err
			return
		}
		l.Info("Initial plan has no assignments (file is likely empty). Download considered complete.")
		// Create an empty file.
		emptyFile, createErr := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePermission)
		if createErr != nil {
			finalErrorChan <- fmt.Errorf("%w: creating empty destination file %s: %v", ErrFileWriteFailed, destPath, createErr)
			return
		}
		emptyFile.Close()

		// Verify checksum for empty file if provided
		if mainFileMeta.TotalSize == 0 && mainFileMeta.ChecksumValue != "" {
			if !verifyChecksum([]byte{}, mainFileMeta.ChecksumAlgorithm, mainFileMeta.ChecksumValue) {
				l.Error("Checksum mismatch for empty file", "expected", mainFileMeta.ChecksumValue)
				finalErrorChan <- ErrChecksumMismatch
				return
			}
			l.Info("Checksum verified for empty file.")
		}
		finalErrorChan <- nil // Success for an empty file
		return
	}
	l.Info("Initial transfer plan received", "num_chunks", len(initialPlan.ChunkAssignments), "total_size_bytes", mainFileMeta.TotalSize)

	// --- File and State Initialization ---
	if mainFileMeta.TotalSize > 0 {
		if err := destWriter.Truncate(mainFileMeta.TotalSize); err != nil {
			l.Warn("Failed to pre-allocate file space, continuing without it", "error", err)
			// Non-fatal, proceed without pre-allocation.
		}
	}

	totalChunksInPlan := len(initialPlan.ChunkAssignments)
	var (
		wg                           sync.WaitGroup
		resultsChan                  = make(chan *ChunkDownloadResult, d.config.Concurrency*2)
		writeChan                    = make(chan chunkToWrite, d.config.Concurrency*2) // Buffered
		downloadedSize               atomic.Int64
		completedChunksCount         atomic.Int32
		permanentlyFailedChunksCount atomic.Int32
		chunkStates                  = make(map[uint64]*chunkTrack, totalChunksInPlan)
		muChunkStates                sync.RWMutex
		jobsChan                     = make(chan downloadWorkerJob, d.config.Concurrency*2) // Buffered
		updatedPlanExternalChan      = make(chan *qwrappb.UpdatedTransferPlan, 10)          // Buffered
		activeDownloads              atomic.Int32
		fileWriterWG                 sync.WaitGroup
	)

	workerCtx, workerCancel := context.WithCancel(ctx) // Contexte pour tous les workers et goroutines de support

	// --- Stratégie de contre-pression ---
	var diskWritePermits chan struct{}
	if d.config.BackpressureStrategy == TokenBackpressure {
		l.Info("Using Token backpressure strategy", "permits", d.config.TokenPermits)
		diskWritePermits = make(chan struct{}, d.config.TokenPermits)
		// Remplir le pool de jetons initial.
		for i := 0; i < d.config.TokenPermits; i++ {
			diskWritePermits <- struct{}{}
		}
	}

	// Démarrer les workers
	var wgWorkers sync.WaitGroup
	wgWorkers.Add(d.config.Concurrency)
	for i := 0; i < d.config.Concurrency; i++ {
		go d.downloadWorker(workerCtx, i+1, jobsChan, resultsChan, &wgWorkers, diskWritePermits)
	}

	fileWriterWG.Add(1)
	// Le fileWriter n'a besoin que de io.WriterAt, pas de l'interface complète de destinationWriter.
	// The fileWriter only needs io.WriterAt, not the full destinationWriter interface.
	go d.fileWriter(workerCtx, &fileWriterWG, destWriter, writeChan, mainFileMeta.GetFileId(), diskWritePermits)

	// --- Initial Job Dispatch ---
	muChunkStates.Lock()
	sortedInitialAssignments := make([]*qwrappb.ChunkAssignment, len(initialPlan.ChunkAssignments))
	copy(sortedInitialAssignments, initialPlan.ChunkAssignments)
	sort.Slice(sortedInitialAssignments, func(i, j int) bool {
		return sortedInitialAssignments[i].ChunkInfo.ChunkId < sortedInitialAssignments[j].ChunkInfo.ChunkId
	})
	for _, assignment := range sortedInitialAssignments {
		chunkID := assignment.ChunkInfo.ChunkId
		chunkStates[chunkID] = &chunkTrack{info: assignment.ChunkInfo, currentAssignment: assignment, currentAttemptOnAssign: 0} // attempt 0, sera incrémenté avant l'envoi
		// Envoyer le job initial
		l.Debug("Dispatching initial job for chunk", "chunk_id", chunkID, "agent_id", assignment.AgentId)
		jobsChan <- downloadWorkerJob{assignment: assignment, attempt: 1}
		activeDownloads.Add(1)
	}
	muChunkStates.Unlock()

	// --- Start Orchestrator Listener ---
	if d.config.OrchestratorComms != nil && initialPlan.PlanId != "" {
		l.Info("Starting listener for updated plans from orchestrator", "plan_id", initialPlan.PlanId)
		wg.Add(1) // S'assurer que cette goroutine est attendue
		go func() {
			defer wg.Done() // Assurer que WaitGroup est décrémenté quand la goroutine se termine
			errListener := d.config.OrchestratorComms.ListenForUpdatedPlans(workerCtx, initialPlan.PlanId, updatedPlanExternalChan)
			if errListener != nil && !errors.Is(errListener, context.Canceled) && workerCtx.Err() == nil {
				l.Error("Orchestrator plan update listener returned an error", "error", errListener)
				// Ce n'est pas nécessairement fatal pour le téléchargement en cours si on a déjà un plan.
			}
			l.Info("Orchestrator plan update listener has finished.")
		}()
	} else {
		// Si pas de comms orchestrateur ou pas de plan ID, on ferme le channel des mises à jour pour ne pas bloquer
		close(updatedPlanExternalChan)
	}

	var finalErrLoop error
	outstandingChunks := int32(totalChunksInPlan)
	reassignmentCheckTicker := time.NewTicker(chunkReassignmentTimeout / 2)
	defer reassignmentCheckTicker.Stop()
	var writeQueue []chunkToWrite
	var writeChanSaturated bool // Pour suivre l'état de saturation de writeChan

	// --- Main Loop ---
	l.Info("Starting main download processing loop", "outstanding_chunks", outstandingChunks, "active_downloads", activeDownloads.Load())
mainLoop:
	for outstandingChunks > 0 || activeDownloads.Load() > 0 || len(writeQueue) > 0 {
		// --- Select avec écriture conditionnelle (pattern 'nil channel') ---
		var (
			chunkToSend chunkToWrite
			sendChan    chan<- chunkToWrite
		)

		if len(writeQueue) > 0 {
			chunkToSend = writeQueue[0]
			sendChan = writeChan // Activer le cas d'écriture
		}

		select {
		case <-d.shutdownChan: // Shutdown requested
			finalErrLoop = errors.New("downloader gracefully shut down")
			l.Info("Graceful shutdown initiated.")
			break mainLoop

		case <-ctx.Done(): // Contexte parent annulé
			finalErrLoop = ErrDownloadCancelled
			l.Info("Download cancelled by parent context (in main select).")
			break mainLoop

		case <-workerCtx.Done(): // Contexte des workers annulé (peut être par ce propre manageDownloadLifecycle)
			if finalErrLoop == nil { // Si pas déjà une erreur fatale
				finalErrLoop = workerCtx.Err()
			}
			l.Info("Worker context cancelled, breaking main loop.", "reason", workerCtx.Err())
			break mainLoop

		case updatedPlan, ok := <-updatedPlanExternalChan:
			if !ok {
				updatedPlanExternalChan = nil // Cesser de sélectionner sur un canal fermé
				l.Info("Updated plan channel closed by listener.")
				continue // Redémarrer la boucle select pour ne pas tomber dans le default si d'autres canaux sont actifs
			}
			if updatedPlan == nil {
				continue
			}
			l.Info("Downloader received UpdatedTransferPlan", "num_new_assign", len(updatedPlan.NewOrUpdatedAssignments), "num_invalidated", len(updatedPlan.InvalidatedChunks))

			d.muCurrentPlan.Lock()
			d.currentPlanSnapshot = mergePlan(d.currentPlanSnapshot, updatedPlan)
			d.muCurrentPlan.Unlock()

			muChunkStates.Lock()
			for _, newAssign := range updatedPlan.NewOrUpdatedAssignments {
				if newAssign == nil || newAssign.ChunkInfo == nil {
					continue
				}
				chunkID := newAssign.ChunkInfo.ChunkId
				track, exists := chunkStates[chunkID]
				if !exists || track.isCompleted || track.isPermanentlyFailed {
					muChunkStates.Unlock() // Libérer le verrou avant de continuer ou de sortir de la boucle
					continue
				}
				l.Info("Processing new assignment from orchestrator", "chunk_id", chunkID, "new_agent", newAssign.AgentId)
				track.currentAssignment = newAssign
				track.currentAttemptOnAssign = 0 // Sera incrémenté avant envoi
				track.localRetryTotalCount = 0   // Réinitialiser pour une nouvelle assignation de l'orchestrateur
				track.awaitingReassignment = false

				// Essayer d'envoyer le job de manière non bloquante ou abandonner si workerCtx est fait
				select {
				case jobsChan <- downloadWorkerJob{assignment: newAssign, attempt: 1}:
					activeDownloads.Add(1)
				case <-workerCtx.Done():
					l.Warn("Could not re-queue job with new assignment (worker context done)", "chunk_id", chunkID)
				default: // Non-bloquant: si jobsChan est plein, on logue mais on ne bloque pas.
					l.Warn("Could not re-queue job with new assignment (jobsChan full or default hit)", "chunk_id", chunkID)
				}
			}
			for _, invalidatedCI := range updatedPlan.InvalidatedChunks {
				if invalidatedCI == nil {
					continue
				}
				chunkID := invalidatedCI.ChunkId
				track, exists := chunkStates[chunkID]
				if exists && !track.isCompleted && !track.isPermanentlyFailed {
					l.Info("Chunk invalidated by orchestrator, marking as perm. failed", "chunk_id", chunkID)
					if !track.isPermanentlyFailed { // Assurer que Add est appelé une seule fois
						track.isPermanentlyFailed = true
						permanentlyFailedChunksCount.Add(1)
						atomic.AddInt32(&outstandingChunks, -1) // Décrémenter les chunks en attente
						l.Warn("Chunk has failed all local retries and is now permanently failed", "chunk_id", chunkID, "agent_id", track.currentAssignment.AgentId)
					}
				}
			}
			muChunkStates.Unlock()

		case result, ok := <-resultsChan:
			if !ok {
				resultsChan = nil // Cesser de sélectionner sur un canal fermé
				l.Info("Results channel closed by workers.")
				// Si tous les workers sont finis et qu'il reste des chunks, c'est une erreur.
				if activeDownloads.Load() == 0 && outstandingChunks > 0 && workerCtx.Err() == nil {
					finalErrLoop = errors.New("results channel closed but outstanding chunks remain and workers not cancelled")
					l.Error(finalErrLoop.Error())
				}
				continue // Redémarrer la boucle select
			}
			activeDownloads.Add(-1) // Un worker a terminé un job
			l.Debug("Processed a result from resultsChan", "current_backlog", len(resultsChan))

			muChunkStates.Lock()
			track, exists := chunkStates[result.ChunkInfo.ChunkId]
			if !exists { // ChunkID non suivi, peut arriver si le plan a changé radicalement
				l.Warn("Received result for untracked chunkID", "chunk_id", result.ChunkInfo.ChunkId)
				muChunkStates.Unlock()
				continue
			}
			if track.isCompleted || track.isPermanentlyFailed { // Déjà traité
				l.Debug("Received result for already completed/failed chunk", "chunk_id", result.ChunkInfo.ChunkId, "status_completed", track.isCompleted, "status_failed", track.isPermanentlyFailed)
				muChunkStates.Unlock()
				continue
			}
			// Est-ce que ce résultat correspond à l'assignation actuelle que nous suivons ?
			isStaleResult := track.currentAssignment.AgentId != result.AgentID || track.currentAssignment.ChunkInfo.ChunkId != result.ChunkInfo.ChunkId
			muChunkStates.Unlock() // Libérer avant d'envoyer le rapport (potentiellement bloquant)

			statusReport := qwrappb.ChunkTransferStatus{ChunkInfo: result.ChunkInfo, AgentId: result.AgentID}
			if result.Err != nil {
				statusReport.Details = result.Err.Error()
			}

			if result.Err != nil { // Échec de la tentative de téléchargement
				l.Warn("Chunk download attempt failed",
					"chunk_id", result.ChunkInfo.ChunkId, "agent", result.AgentID,
					"attempt_on_assign", result.Attempt, "total_local_attempts_for_chunk", track.localRetryTotalCount+1, "error", result.Err.Error())

				statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR
				if errors.Is(result.Err, context.Canceled) || errors.Is(result.Err, context.DeadlineExceeded) || errors.Is(result.Err, net.ErrClosed) {
					statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_AGENT_UNREACHABLE
				}
				l.Debug("Sending chunk status report to orchestrator", "chunk_id", statusReport.ChunkInfo.ChunkId, "status", statusReport.Event)
				d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &statusReport)

				muChunkStates.Lock()
				if isStaleResult || track.isCompleted || track.isPermanentlyFailed { // Revérifier sous lock
					muChunkStates.Unlock()
					continue
				}
				track.localRetryTotalCount++
				if track.currentAttemptOnAssign < d.config.MaxLocalRetries { // Retry local sur la même assignation
					l.Info("Queueing chunk for local retry on same assignment", "chunk_id", track.info.ChunkId, "next_attempt_on_assign", track.currentAttemptOnAssign+1)
					track.currentAttemptOnAssign++
					select {
					case jobsChan <- downloadWorkerJob{assignment: track.currentAssignment, attempt: track.currentAttemptOnAssign}:
						activeDownloads.Add(1)
					case <-workerCtx.Done():
						l.Warn("Could not queue local retry (worker context done)", "chunk_id", track.info.ChunkId)
					default:
						l.Warn("Could not queue local retry (jobsChan full or default hit)", "chunk_id", track.info.ChunkId)
					}
				} else { // Max retries locaux sur cette assignation
					l.Warn("Max local retries on current assignment. Requesting reassignment.", "chunk_id", track.info.ChunkId, "agent", track.currentAssignment.AgentId)
					track.awaitingReassignment = true
					track.lastAwaitingReassignmentTime = time.Now()
					reassignReqReport := &qwrappb.ChunkTransferStatus{
						ChunkInfo: statusReport.ChunkInfo,
						AgentId:   statusReport.AgentId,
						Event:     qwrappb.TransferEvent_REASSIGNMENT_REQUESTED,
						Details:   "Max local retries on assignment " + track.currentAssignment.AgentId,
					}
					l.Debug("Requesting chunk reassignment from orchestrator", "chunk_id", reassignReqReport.ChunkInfo.ChunkId, "reason", reassignReqReport.Details)
					d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, reassignReqReport)
				}
				muChunkStates.Unlock()

			} else { // Succès du téléchargement du chunk
				l.Debug("Chunk download successful", "chunk_id", result.ChunkInfo.ChunkId, "size", len(result.Data))
				statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_SUCCESSFUL
				// statusReport.TransferDuration = ... // TODO

				isVerified := true
				d.muCurrentPlan.RLock()
				planOpts := d.currentPlanSnapshot.Options
				d.muCurrentPlan.RUnlock()
				if planOpts != nil && planOpts.VerifyChunkChecksums && result.ChunkInfo.ChecksumValue != "" {
					if !verifyChecksum(result.Data, result.ChunkInfo.ChecksumAlgorithm, result.ChunkInfo.ChecksumValue) {
						isVerified = false
						statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_CHECKSUM_MISMATCH
						statusReport.Details = "Checksum mismatch after download"
						l.Error(statusReport.Details, "chunk_id", result.ChunkInfo.ChunkId)
						muChunkStates.Lock()
						if !track.isCompleted && !track.isPermanentlyFailed { // Ne pas modifier si déjà traité
							track.localRetryTotalCount++
							track.awaitingReassignment = true
							track.lastAwaitingReassignmentTime = time.Now()
						}
						muChunkStates.Unlock()
					}
				}
				l.Debug("Sending chunk status report to orchestrator", "chunk_id", statusReport.ChunkInfo.ChunkId, "status", statusReport.Event)
				d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &statusReport)

				if isVerified {
					muChunkStates.Lock()
					if !track.isCompleted && !track.isPermanentlyFailed { // Éviter double traitement
						track.isCompleted = true
						track.awaitingReassignment = false
						completedChunksCount.Add(1)
						atomic.AddInt32(&outstandingChunks, -1) // Important: doit être atomique
						downloadedSize.Add(int64(len(result.Data)))
						muChunkStates.Unlock()

						// Ajouter à la file d'attente pour l'écriture disque
						writeQueue = append(writeQueue, chunkToWrite{
							chunkID: result.ChunkInfo.ChunkId,
							data:    result.Data, // Le worker a déjà fait une copie si nécessaire
							offset:  result.ChunkInfo.Range.Offset,
						})
					} else {
						muChunkStates.Unlock()
					}
				}
			}

		case <-reassignmentCheckTicker.C:
			muChunkStates.Lock()
			now := time.Now()
			for chunkID, tr := range chunkStates {
				if tr.awaitingReassignment && !tr.isCompleted && !tr.isPermanentlyFailed {
					if now.Sub(tr.lastAwaitingReassignmentTime) > chunkReassignmentTimeout {
						l.Error("Chunk awaiting reassignment timed out, marking as perm. failed.", "chunk_id", chunkID, "agent", tr.currentAssignment.AgentId)
						if !tr.isPermanentlyFailed { // Assurer que Add est appelé une seule fois
							tr.isPermanentlyFailed = true
							tr.awaitingReassignment = false
							permanentlyFailedChunksCount.Add(1)
							atomic.AddInt32(&outstandingChunks, -1) // Décrémenter les chunks en attente
							statusReport := qwrappb.ChunkTransferStatus{ChunkInfo: tr.info, Event: qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR, AgentId: tr.currentAssignment.AgentId, Details: "Reassignment request timed out"}
							l.Debug("Sending chunk status report to orchestrator", "chunk_id", statusReport.ChunkInfo.ChunkId, "status", statusReport.Event)
							d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &statusReport)
						}
					}
				}
			}
			muChunkStates.Unlock()

		case sendChan <- chunkToSend:
			// Le chunk a été envoyé avec succès au fileWriter
			writeQueue = writeQueue[1:]
			if writeChanSaturated {
				l.Info("writeChan is no longer saturated.")
				writeChanSaturated = false
			}
		}

		// Logique de progression après chaque itération significative du select
		currentProgress := ProgressInfo{
			TotalSizeBytes:      mainFileMeta.TotalSize,
			DownloadedSizeBytes: downloadedSize.Load(),
			TotalChunks:         totalChunksInPlan,
			CompletedChunks:     int(completedChunksCount.Load()),
			FailedChunks:        int(permanentlyFailedChunksCount.Load()),
			ActiveDownloads:     int(activeDownloads.Load()),
		}
		// Envoyer la progression de manière non bloquante
		select {
		case progressChan <- currentProgress:
		default: // Ne pas bloquer si le canal de progression est plein
		}
	} // Fin de la boucle principale `mainLoop`

	l.Info("Exited main download loop.")

	// Si la boucle s'est terminée à cause d'une erreur de contexte capturée avant, on la propage.
	if ctx.Err() != nil && finalErrLoop == nil {
		finalErrLoop = ctx.Err()
		l.Info("Main context cancellation detected after loop exit.", "error", finalErrLoop)
	}
	// Si le workerCtx s'est terminé (potentiellement initié par cette fonction via finalErrLoop),
	// s'assurer que finalErrLoop reflète cela si pas déjà une erreur plus spécifique.
	if workerCtx.Err() != nil && finalErrLoop == nil {
		finalErrLoop = workerCtx.Err()
		l.Info("Worker context cancellation detected after loop exit.", "error", finalErrLoop)
	}

	// Signaler aux goroutines de s'arrêter si ce n'est pas déjà fait par le ctx parent.
	l.Debug("Cancelling worker context to signal all related goroutines.")
	workerCancel()

	// Attendre que tous les workers et le fileWriter se terminent.
	l.Debug("Waiting for all downloader goroutines to finish...")
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	// Boucle de drainage finale pour resultsChan et updatedPlanExternalChan.
	// Cette boucle doit se terminer si waitDone se ferme OU si le contexte parent est annulé.
drainFinalLoop:
	for {
		select {
		case result, ok := <-resultsChan:
			if !ok {
				resultsChan = nil // Plus rien à drainer
			} else {
				activeDownloads.Add(-1) // Un worker peut avoir envoyé un dernier résultat
				l.Debug("Drained a result during final shutdown", "chunk_id", result.ChunkInfo.ChunkId, "err", result.Err)
			}
		case _, ok := <-updatedPlanExternalChan:
			if !ok {
				updatedPlanExternalChan = nil
			} else {
				l.Debug("Drained an updated plan during final shutdown")
			}
		case <-waitDone:
			l.Info("All background goroutines (workers, fileWriter, planListener) have finished.")
			break drainFinalLoop // Toutes les goroutines gérées par wg sont terminées
		case <-ctx.Done(): // Le contexte global est annulé
			l.Warn("Global context cancelled during final drain loop. Some goroutines might not have exited cleanly if wg.Wait() was still pending.")
			break drainFinalLoop
		case <-time.After(5 * time.Second): // Timeout de sécurité pour la boucle de drainage
			l.Error("Timeout in final drain loop. Some goroutines might be stuck.")
			if finalErrLoop == nil {
				finalErrLoop = errors.New("downloader shutdown timed out during final drain")
			}
			break drainFinalLoop
		}
		if resultsChan == nil && updatedPlanExternalChan == nil && (waitDone == nil || isChanClosed(waitDone)) {
			// Tous les canaux pertinents sont fermés et wg.Wait a fini (si waitDone est fermé)
			break drainFinalLoop
		}
	}

	// Il est maintenant sûr de fermer jobsChan (si ce n'est pas déjà fait par l'annulation du workerCtx)
	// car tous les workers l'écoutant devraient être terminés.
	close(jobsChan) // Les workers devraient déjà être sortis via workerCtx.Done().

	l.Info("Downloader goroutines finished. Proceeding to final checks.")

	// Si nous sommes sortis de la boucle principale sans erreur spécifique,
	// vérifier si tous les chunks ont été traités.
	muChunkStates.RLock()
	finalCompleted := completedChunksCount.Load()
	finalFailedPerm := permanentlyFailedChunksCount.Load()
	var unresolvedChunksCount int32 = 0
	for _, tr := range chunkStates {
		if !tr.isCompleted && !tr.isPermanentlyFailed {
			unresolvedChunksCount++
		}
	}
	muChunkStates.RUnlock()

	if finalErrLoop == nil { // Si aucune erreur fatale n'a déjà été enregistrée
		if int(finalCompleted) != totalChunksInPlan {
			if int(finalCompleted+finalFailedPerm) < totalChunksInPlan || unresolvedChunksCount > 0 {
				finalErrLoop = fmt.Errorf("%w: not all chunks resolved (completed: %d, failed_perm: %d, unresolved: %d, total: %d)",
					ErrChunkDownloadFailed, finalCompleted, finalFailedPerm, unresolvedChunksCount, totalChunksInPlan)
			} else if finalFailedPerm > 0 { // Tous résolus, mais certains ont échoué
				finalErrLoop = fmt.Errorf("%w: %d chunks failed permanently, %d completed", ErrChunkDownloadFailed, finalFailedPerm, finalCompleted)
			}
		} else { // Tous les chunks sont marqués comme complétés, vérifions l'écriture
			// On ne peut pas vérifier nextChunkToWriteID ici car il est local à fileWriter.
			// On suppose que si writeChan s'est vidé et fileWriter s'est terminé sans erreur (ce qu'on ne peut pas vérifier ici), c'est bon.
			// La vérification du checksum global devient plus importante.
			l.Info("All chunks reported as completed by download workers.")
		}
	}

	// Attendre que le fileWriter se termine.
	l.Info("Closing write channel and waiting for file writer to terminate...")
	close(writeChan)
	fileWriterWG.Wait() // Wait for the file writer to finish writing all chunks.
	l.Info("File writer has terminated.")

	// Vérification du checksum global si aucune erreur majeure n'est survenue
	if finalErrLoop == nil {
		l.Info("Download complete, verifying final checksum...")

		// Attempt to get the underlying *os.File for Sync and Seek operations
		fileForVerify, ok := destWriter.(*os.File)
		if !ok {
			l.Warn("Checksum verification skipped: destination writer is not an os.File, cannot sync/seek.")
		} else {
			if errSync := fileForVerify.Sync(); errSync != nil {
				l.Error("Failed to sync destination file before checksum", "error", errSync)
				if finalErrLoop == nil {
					finalErrLoop = fmt.Errorf("sync error before checksum: %w", errSync)
				}
			}

			if finalErrLoop == nil { // Continuer seulement si sync a réussi
				if _, errSeek := fileForVerify.Seek(0, io.SeekStart); errSeek != nil {
					l.Error("Failed to seek for checksum", "error", errSeek)
					if finalErrLoop == nil {
						finalErrLoop = fmt.Errorf("seek error before checksum: %w", errSeek)
					}
				} else {
					var hasher hash.Hash
					d.muCurrentPlan.RLock()
					algo := mainFileMeta.ChecksumAlgorithm // Utiliser le meta du plan initial comme référence
					d.muCurrentPlan.RUnlock()

					switch algo {
					case qwrappb.ChecksumAlgorithm_SHA256:
						hasher = sha256.New()
					default:
						l.Warn("Unsupported global checksum algorithm for verification", "algo", algo)
					}

					if hasher != nil {
						if _, errCopy := io.Copy(hasher, fileForVerify); errCopy != nil {
							l.Error("Failed to read destination file for checksum", "error", errCopy)
							if finalErrLoop == nil {
								finalErrLoop = fmt.Errorf("read error during checksum: %w", errCopy)
							}
						} else {
							actualChecksum := hex.EncodeToString(hasher.Sum(nil))
							if actualChecksum != mainFileMeta.ChecksumValue {
								l.Error("Global file checksum MISMATCH", "expected", mainFileMeta.ChecksumValue, "actual", actualChecksum)
								if finalErrLoop == nil {
									finalErrLoop = fmt.Errorf("%w: global checksum mismatch (expected: %s, actual: %s)", ErrChecksumMismatch, mainFileMeta.ChecksumValue, actualChecksum)
								}
							} else {
								l.Info("Global file checksum VERIFIED.", "checksum", actualChecksum)
							}
						}
					}
				}
			}
		}
	}

	// Final summary logging
	l.Info("Download process finished",
		"completed_chunks", completedChunksCount.Load(),
		"permanently_failed_chunks", permanentlyFailedChunksCount.Load(),
		"total_downloaded_bytes", downloadedSize.Load(),
		"final_error", finalErrLoop,
	)

	// Envoyer l'erreur finale (ou nil si succès)
	if finalErrLoop != nil {
		finalErrorChan <- finalErrLoop
	} else {
		finalErrorChan <- nil
	}
}

// isChanClosed est un helper pour vérifier si un channel est fermé.
func isChanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// sendOrchestratorReport est un helper pour envoyer des rapports de manière non bloquante.
func (d *downloaderImpl) sendOrchestratorReport(ctx context.Context, planID string, status *qwrappb.ChunkTransferStatus) {
	if d.config.OrchestratorComms == nil {
		return
	}
	// Copier le statut pour éviter les races si l'original est modifié
	reportStatus := *status
	if status.ChunkInfo != nil { // Copier ChunkInfo aussi
		ciCopy := *status.ChunkInfo
		reportStatus.ChunkInfo = &ciCopy
	}

	// Utiliser un nouveau contexte avec timeout pour l'envoi du rapport,
	// détaché du contexte du chunk spécifique qui a pu être annulé.
	reportCtx, reportCancel := context.WithTimeout(context.Background(), orchestratorCommsTimeout)

	// Exécuter dans une goroutine pour ne pas bloquer la boucle principale du downloader
	go func() {
		defer reportCancel()
		clientReport := &qwrappb.ClientReport{
			PlanId:          planID,
			ReportTimestamp: timestamppb.Now(),
			ChunkStatuses:   []*qwrappb.ChunkTransferStatus{&reportStatus},
		}
		err := d.config.OrchestratorComms.ReportTransferStatus(reportCtx, clientReport)
		if err != nil {
			d.config.Logger.Error("Failed to report chunk status to orchestrator",
				"plan_id", planID, "chunk_id", reportStatus.ChunkInfo.ChunkId,
				"event", reportStatus.Event, "error", err)
		} else {
			d.config.Logger.Debug("Successfully reported chunk status to orchestrator",
				"plan_id", planID, "chunk_id", reportStatus.ChunkInfo.ChunkId, "event", reportStatus.Event)
		}
	}()
}

// Helper pour fusionner un UpdatedTransferPlan dans un TransferPlan existant (simplifié)
func mergePlan(current *qwrappb.TransferPlan, update *qwrappb.UpdatedTransferPlan) *qwrappb.TransferPlan {
	if current == nil || update == nil || current.PlanId != update.PlanId {
		// Logique d'erreur ou retourner current tel quel
		return current
	}

	// Créer une copie pour ne pas modifier l'original directement si ce n'est pas souhaité
	// Pour cet exemple, on modifie current.
	// Une vraie implémentation devrait être plus prudente.

	newAssignmentsMap := make(map[uint64]*qwrappb.ChunkAssignment)
	for _, assign := range current.ChunkAssignments {
		if assign != nil && assign.ChunkInfo != nil {
			newAssignmentsMap[assign.ChunkInfo.ChunkId] = assign
		}
	}

	for _, newAssign := range update.NewOrUpdatedAssignments {
		if newAssign != nil && newAssign.ChunkInfo != nil {
			newAssignmentsMap[newAssign.ChunkInfo.ChunkId] = newAssign // Remplace ou ajoute
		}
	}

	// Gérer les invalidated_chunks (les retirer de la map)
	for _, invalidated := range update.InvalidatedChunks {
		if invalidated != nil {
			delete(newAssignmentsMap, invalidated.ChunkId)
		}
	}

	// Reconstruire la slice d'assignments
	finalAssignments := make([]*qwrappb.ChunkAssignment, 0, len(newAssignmentsMap))
	for _, assign := range newAssignmentsMap {
		finalAssignments = append(finalAssignments, assign)
	}
	// Optionnel: retrier finalAssignments par ChunkId
	sort.Slice(finalAssignments, func(i, j int) bool {
		return finalAssignments[i].ChunkInfo.ChunkId < finalAssignments[j].ChunkInfo.ChunkId
	})

	current.ChunkAssignments = finalAssignments
	// Mettre à jour d'autres champs du plan si nécessaire (ex: options, metadata si elles peuvent changer)
	return current
}

func (d *downloaderImpl) downloadWorker(
	ctx context.Context,
	workerID int,
	jobsChan <-chan downloadWorkerJob,
	resultsChan chan<- *ChunkDownloadResult,
	wg *sync.WaitGroup,
	diskWritePermits chan struct{}, // Le worker prend un jeton et le rend après usage.
) {
	defer wg.Done()
	l := d.config.Logger.With("op", "downloadWorker", "worker_id", workerID)
	l.Info("Worker started.")
	defer l.Info("Worker finished.")

	for job := range jobsChan {
		// Étape 1: Si la stratégie de backpressure est activée, acquérir un jeton.
		var permitAcquired bool
		if diskWritePermits != nil {
			l.Debug("Worker waiting for disk-write permit...", "chunk_id", job.assignment.ChunkInfo.ChunkId)
			select {
			case <-ctx.Done():
				return // Terminer si le contexte est annulé en attendant le jeton.
			case <-diskWritePermits:
				permitAcquired = true
				l.Debug("Worker acquired disk-write permit.", "chunk_id", job.assignment.ChunkInfo.ChunkId)
			}
		}

		// Étape 2: Télécharger le chunk.
		l.Debug("Starting download for chunk", "chunk_id", job.assignment.ChunkInfo.ChunkId, "attempt", job.attempt)
		data, err := d.fetchChunk(ctx, job.assignment, workerID)
		if err != nil {
			l.Warn("Fetch chunk failed", "chunk_id", job.assignment.ChunkInfo.ChunkId, "error", err, "attempt", job.attempt)
		}

		// Étape 3: Envoyer le résultat (succès ou échec) au gestionnaire principal.
		select {
		case resultsChan <- &ChunkDownloadResult{
			ChunkInfo:    job.assignment.ChunkInfo,
			Data:         data,
			Err:          err,
			AgentID:      job.assignment.AgentId,
			Attempt:      job.attempt,
		}:
			// L'envoi a réussi.
		case <-ctx.Done():
			l.Debug("Context cancelled while worker sending result", "worker_id", workerID, "chunk_id", job.assignment.ChunkInfo.ChunkId)
			// Si nous abandonnons, nous devons rendre le jeton pour ne pas le perdre.
			if permitAcquired {
				diskWritePermits <- struct{}{}
				l.Debug("Disk write permit released by worker due to context cancellation during send")
			}
			return
		}

		// Étape 4: Libérer le jeton APRÈS que le résultat a été envoyé avec succès.
		if permitAcquired {
			diskWritePermits <- struct{}{}
			l.Debug("Disk write permit released by worker after successful send", "chunk_id", job.assignment.ChunkInfo.ChunkId)
		}
	}
}

func (d *downloaderImpl) fetchChunk(
	parentCtx context.Context, // Contexte venant du worker, qui est lié au contexte global du download
	assignment *qwrappb.ChunkAssignment,
	workerID int,
) ([]byte, error) { // Retourne les données du chunk ou une erreur
	var err error
	var conn quic.Connection
	var stream quic.Stream

	// Contexte spécifique pour cette tentative de fetchChunk, avec un timeout global pour l'opération.
	fetchCtx, fetchCancel := context.WithTimeout(parentCtx, d.config.ChunkRequestTimeout)
	defer fetchCancel() // Très important pour libérer les ressources du contexte du timeout

	// Logger contextuel pour ce fetch spécifique
	fetchLogger := d.config.Logger.With(
		"op", "fetchChunk",
		"worker_id", workerID,
		"agent_addr", assignment.AgentAddress,
		"file_id", assignment.ChunkInfo.FileId,
		"chunk_id", assignment.ChunkInfo.ChunkId,
	)

	// Defer pour la fermeture du stream QUIC, quoi qu'il arrive.
	defer func() {
		if stream != nil {
			// Annuler les opérations de lecture/écriture en cours sur le stream avant de le fermer.
			// Utiliser des codes d'erreur applicatifs (0 pour normal, >0 pour erreur spécifique).
			stream.CancelRead(quic.StreamErrorCode(0))  // Code d'erreur "NO_ERROR"
			stream.CancelWrite(quic.StreamErrorCode(0)) // Code d'erreur "NO_ERROR"
			// stream.Close() est appelé implicitement par CancelRead/CancelWrite si le stream est encore ouvert,
			// ou peut être appelé explicitement. quic.Stream.Close() est idempotent.
			// On ne logue pas l'erreur de Close ici, car elle est souvent redondante
			// si le contexte a été annulé ou si une erreur d'I/O a déjà eu lieu.
		}
	}()

	// 1. Obtenir/Établir la connexion QUIC avec l'agent via ConnectionManager
	conn, err = d.config.ConnManager.GetOrConnect(fetchCtx, assignment.AgentAddress)
	if err != nil {
		fetchLogger.Warn("Failed to get/connect to agent", "error", err)
		// Invalider la connexion dans le manager pour forcer une nouvelle tentative de Dial la prochaine fois.
		if d.config.ConnManager != nil { // Vérifier au cas où il serait nil (ne devrait pas avec panic dans setDefaults)
			// Utiliser un contexte d'arrière-plan pour l'invalidation, car fetchCtx peut déjà être expiré.
			invalidateCtx, invalidateCancel := context.WithTimeout(context.Background(), 5*time.Second)
			d.config.ConnManager.Invalidate(invalidateCtx, assignment.AgentAddress)
			invalidateCancel()
		}
		return nil, fmt.Errorf("agent connection to %s failed for chunk %d: %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, err)
	}

	// 2. Ouvrir un nouveau flux QUIC sur la connexion
	// Utiliser fetchCtx pour OpenStreamSync pour que l'ouverture du flux respecte le timeout global de fetchChunk.
	stream, err = conn.OpenStreamSync(fetchCtx)
	if err != nil {
		fetchLogger.Warn("Failed to open QUIC stream to agent", "error", err)
		// Si l'ouverture du flux échoue, la connexion sous-jacente pourrait être mauvaise.
		// L'invalidation est déjà gérée par GetOrConnect si Dial échoue, mais ici
		// Dial a réussi et OpenStreamSync a échoué. Invalider explicitement peut être une bonne idée.
		if d.config.ConnManager != nil {
			invalidateCtx, invalidateCancel := context.WithTimeout(context.Background(), 5*time.Second)
			d.config.ConnManager.Invalidate(invalidateCtx, assignment.AgentAddress)
			invalidateCancel()
		}
		return nil, fmt.Errorf("open stream to %s for chunk %d failed: %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, err)
	}
	// Le defer func() ci-dessus s'occupera de fermer/annuler ce stream.
	fetchLogger = fetchLogger.With("stream_id", stream.StreamID()) // Ajouter l'ID du stream au logger

	// 3. Envoyer StreamPurposeRequest à l'Agent
	msgWriter := d.config.WriterFactory(stream, fetchLogger.With("role", "writer")) // Logger contextuel pour le writer

	purposeMsg := &qwrappb.StreamPurposeRequest{Purpose: qwrappb.StreamPurposeRequest_CHUNK_REQUEST}
	// Utiliser fetchCtx pour l'écriture du message de but.
	if err = msgWriter.WriteMsg(fetchCtx, purposeMsg); err != nil {
		if fetchCtx.Err() != nil { // Vérifier si le timeout/annulation du contexte est la cause
			return nil, fmt.Errorf("context cancelled/timed out sending CHUNK_REQUEST purpose to agent %s (chunk %d): %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, fetchCtx.Err())
		}
		fetchLogger.Warn("Failed to send CHUNK_REQUEST purpose to agent", "error", err)
		return nil, fmt.Errorf("send CHUNK_REQUEST purpose to %s for chunk %d failed: %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, err)
	}
	fetchLogger.Debug("Sent CHUNK_REQUEST purpose to agent")

	// 4. Envoyer le vrai ChunkRequest (contenant ChunkInfoRequested)
	req := &qwrappb.ChunkRequest{
		ChunkInfoRequested: assignment.ChunkInfo, // Le ChunkInfo de l'assignation contient le FileId, ChunkId, et ByteRange
	}
	if err = msgWriter.WriteMsg(fetchCtx, req); err != nil {
		if fetchCtx.Err() != nil {
			return nil, fmt.Errorf("context cancelled/timed out sending ChunkRequest payload to agent %s (chunk %d): %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, fetchCtx.Err())
		}
		fetchLogger.Warn("Failed to send ChunkRequest payload to agent", "error", err)
		return nil, fmt.Errorf("send ChunkRequest payload to %s for chunk %d failed: %w", assignment.AgentAddress, assignment.ChunkInfo.ChunkId, err)
	}
	fetchLogger.Debug("Sent ChunkRequest payload to agent")

	// Le client a fini d'écrire sur ce flux. Il attend maintenant les données du chunk.
	// L'agent lira les deux messages, puis enverra les données du chunk et fermera sa partie du flux.
	// Le client lira jusqu'à la longueur attendue ou EOF.

	// 5. Lire les données brutes du chunk
	if assignment.ChunkInfo.Range == nil || assignment.ChunkInfo.Range.Length <= 0 {
		fetchLogger.Error("Invalid assignment: ChunkInfo.Range is nil or length is zero/negative")
		return nil, fmt.Errorf("invalid range in chunk assignment for chunk %d (file %s)", assignment.ChunkInfo.ChunkId, assignment.ChunkInfo.FileId)
	}
	expectedSize := assignment.ChunkInfo.Range.Length

	// Utiliser un buffer du pool pour la lecture afin de réduire les allocations.
	// Assurez-vous que GetBuffer et PutBuffer sont correctement définis dans votre package messaging (ou framing).
	payloadBufPtr := framing.GetBuffer()
	defer framing.PutBuffer(payloadBufPtr)

	payloadBuf := *payloadBufPtr
	if cap(payloadBuf) < int(expectedSize) {
		payloadBuf = make([]byte, expectedSize) // Allouer si le buffer du pool est trop petit
	} else {
		payloadBuf = payloadBuf[:expectedSize] // Utiliser la capacité existante
	}

	// 5. Lire les données du chunk avec une boucle de lecture robuste.
	fetchLogger.Debug("Starting robust chunk read loop")

	// Allouer un buffer de la taille exacte attendue pour stocker les données du chunk.
	// Cela évite de devoir créer une copie plus tard.
	chunkData := make([]byte, expectedSize)
	totalBytesRead := 0

	// Utiliser un buffer de lecture temporaire pour éviter de multiples petites lectures.
	// 32KB est une taille courante et efficace.
	readBuf := make([]byte, 32*1024)

	for int64(totalBytesRead) < expectedSize {
		// Vérifier si le contexte a été annulé avant de tenter une nouvelle lecture.
		if fetchCtx.Err() != nil {
			fetchLogger.Warn("Context cancelled during chunk data read loop", "error", fetchCtx.Err(), "bytes_read_so_far", totalBytesRead)
			return nil, fmt.Errorf("context error reading chunk %d data from %s: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, fetchCtx.Err())
		}

		// Lire depuis le stream dans le buffer temporaire.
		// Le stream QUIC respecte le contexte `fetchCtx`, donc `Read` se débloquera si le contexte est annulé.
		n, readErr := stream.Read(readBuf)

		if n > 0 {
			// Copier les octets lus du buffer temporaire vers le buffer final du chunk.
			// S'assurer de ne pas écrire au-delà des limites du buffer `chunkData`.
			copy(chunkData[totalBytesRead:], readBuf[:n])
			totalBytesRead += n
		}

		if readErr != nil {
			if readErr == io.EOF {
				// EOF signifie que l'agent a fermé le stream de son côté.
				// C'est une condition de fin normale. On sort de la boucle et on vérifiera la taille totale lue.
				fetchLogger.Debug("EOF reached while reading chunk stream", "total_bytes_read", totalBytesRead, "expected_size", expectedSize)
				break
			}
			// Pour toute autre erreur (ex: réinitialisation de connexion, etc.), c'est une défaillance.
			fetchLogger.Warn("Failed to read chunk data from agent (non-EOF error)", "error", readErr, "bytes_read_so_far", totalBytesRead)
			return nil, fmt.Errorf("read chunk %d data from %s failed: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, readErr)
		}
	}

	// Après la boucle, valider que la quantité totale de données lues correspond à la taille attendue.
	if int64(totalBytesRead) < expectedSize {
		fetchLogger.Warn("Incomplete chunk received from agent after read loop", "expected", expectedSize, "received", totalBytesRead)
		// io.ErrUnexpectedEOF est une erreur appropriée pour indiquer une fin de flux prématurée.
		return nil, fmt.Errorf("incomplete chunk %d from %s: got %d, want %d: %w",
			assignment.ChunkInfo.ChunkId, assignment.AgentAddress, totalBytesRead, expectedSize, io.ErrUnexpectedEOF)
	}

	// Il est peu probable de lire plus que prévu avec la condition de la boucle, mais c'est une vérification de sécurité.
	if int64(totalBytesRead) > expectedSize {
		fetchLogger.Error("Read more data than expected for chunk", "expected", expectedSize, "received", totalBytesRead)
		// Trancher le buffer pour retourner uniquement la taille attendue, mais logger l'anomalie.
		chunkData = chunkData[:expectedSize]
	}

	fetchLogger.Info("Successfully fetched chunk data with robust loop", "size", len(chunkData))
	return chunkData, nil
}

// verifyChecksum vérifie le checksum des données.
func verifyChecksum(data []byte, algo qwrappb.ChecksumAlgorithm, expectedChecksumHex string) bool {
	if expectedChecksumHex == "" {
		return true // Pas de checksum à vérifier
	}
	var hasher io.Writer
	switch algo {
	case qwrappb.ChecksumAlgorithm_SHA256:
		h := sha256.New()
		defer h.Reset()
		hasher = h
	// Ajoutez d'autres algos ici si nécessaire (SHA512, XXHASH64)
	default:
		slog.Warn("Unsupported checksum algorithm for chunk verification", "algorithm", algo)
		return false // Ou true si on veut être permissif pour les algos inconnus
	}

	hashInstance, ok := hasher.(interface{ Sum(b []byte) []byte })
	if !ok {
		slog.Error("Hasher does not implement Sum(b []byte) method")
		return false
	}

	if _, err := io.Copy(hasher, bytes.NewReader(data)); err != nil {
		slog.Error("Failed to compute checksum for chunk verification", "error", err)
		return false
	}
	actualChecksumHex := hex.EncodeToString(hashInstance.Sum(nil))
	return actualChecksumHex == expectedChecksumHex
}

// fileWriter is a goroutine that reads completed chunks from a channel and writes them to the destination file.
func (d *downloaderImpl) fileWriter(
	ctx context.Context,
	wg *sync.WaitGroup,
	f io.WriterAt,
	writeChan <-chan chunkToWrite,
	fileID string, // For logging
	diskWritePermits chan<- struct{}, // Peut être nil
) {
	defer wg.Done()
	l := d.config.Logger.With("op", "fileWriter", "file_id", fileID)
	l.Info("File writer started.")
	defer l.Info("File writer finished.")

	for {
		select {
		case <-ctx.Done():
			l.Info("File writer shutting down due to context cancellation.")
			// Drain the channel of any remaining chunks to avoid deadlocking the main loop
			for chunk := range writeChan {
				l.Warn("Draining chunk after cancellation, data will be lost", "chunk_id", chunk.chunkID)
			}
			return
		case chunk, ok := <-writeChan:
			if !ok {
				l.Info("Write channel closed, writer is terminating.")
				return
			}

			l.Debug("Received chunk to write", "chunk_id", chunk.chunkID, "offset", chunk.offset, "size", len(chunk.data))
			_, err := f.WriteAt(chunk.data, chunk.offset)
			if err != nil {
				// This is a critical error. We can't easily recover.
				// We'll log it and the main loop will eventually time out.
				l.Error("CRITICAL: Failed to write chunk to file, download cannot continue.", "error", err)
				// In a more robust implementation, we might send this error back to the main loop
				// via a dedicated channel to trigger a faster failure.
				return
			}


		}
	}
}
