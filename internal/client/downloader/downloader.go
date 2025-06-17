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
	Concurrency          int
	MaxLocalRetries      int // Renommé de MaxRetriesPerChunk
	RetryBaseDelay       time.Duration
	ChunkRequestTimeout  time.Duration
	Logger               *slog.Logger
	ConnManager          ConnectionManager        // Interface définie précédemment
	OrchestratorComms    orchestratorclient.Comms // Interface pour la communication avec l'orchestrateur
	MessageWriterFactory func(w io.Writer, l *slog.Logger) framing.Writer
	MessageReaderFactory func(r io.Reader, l *slog.Logger) framing.Reader
}

func (c *downloaderConfig) setDefaults(logger *slog.Logger) { // logger est passé en argument
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

	// Configurer le logger
	if logger == nil { // Si aucun logger n'est passé à NewDownloader
		c.Logger = slog.Default().With("component", "downloader")
	} else {
		c.Logger = logger.With("component", "downloader") // Utiliser le logger passé et ajouter le contexte
	}

	if c.ConnManager == nil {
		// Normalement, ConnManager devrait être une dépendance obligatoire passée à NewDownloader.
		// Lever une panique ici est approprié si c'est une condition non récupérable pour le fonctionnement.
		panic("ConnectionManager is required for downloader")
	}

	if c.OrchestratorComms == nil {
		c.Logger.Warn("OrchestratorComms is nil, downloader will operate in a degraded mode (no dynamic replanning or detailed reporting). Using NoOp implementation.")
		// <<<< CORRECTION ICI >>>>
		// Utiliser le constructeur NewNoOpComms qui retourne orchestratorclient.Comms
		c.OrchestratorComms = orchestratorclient.NewNoOpComms(c.Logger)
	}

	// Factories pour le messaging
	if c.MessageWriterFactory == nil {
		c.MessageWriterFactory = func(w io.Writer, l *slog.Logger) framing.Writer {
			// Utiliser c.Logger qui est déjà configuré avec le composant "downloader"
			logToUse := c.Logger
			if l != nil {
				logToUse = l
			}
			return framing.NewMessageWriter(w, logToUse.With("subcomponent", "dl_msg_writer"))
		}
	}
	if c.MessageReaderFactory == nil {
		c.MessageReaderFactory = func(r io.Reader, l *slog.Logger) framing.Reader {
			logToUse := c.Logger
			if l != nil {
				logToUse = l
			}
			return framing.NewMessageReader(r, logToUse.With("subcomponent", "dl_msg_reader"))
		}
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
		ConnManager:          connMgr,
		OrchestratorComms:    orchComms,
		Concurrency:          concurrency,
		MessageWriterFactory: writerFactory,
		MessageReaderFactory: readerFactory,
	}
	cfg.setDefaults(logger)
	return &downloaderImpl{config: cfg}
}

// Download (interface)
func (d *downloaderImpl) Download(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	destPath string,
) (<-chan ProgressInfo, <-chan error) {
	progressChan := make(chan ProgressInfo, d.config.Concurrency*2)
	finalErrorChan := make(chan error, 1)
	go d.manageDownloadLifecycle(ctx, initialTransferReq, destPath, progressChan, finalErrorChan)
	return progressChan, finalErrorChan
}

func (d *downloaderImpl) manageDownloadLifecycle(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	destPath string,
	progressChan chan<- ProgressInfo,
	finalErrorChan chan<- error,
) {
	defer close(progressChan)
	defer close(finalErrorChan)

	l := d.config.Logger.With("request_id", initialTransferReq.RequestId, "op", "manageDownloadLifecycle")
	l.Info("Downloader lifecycle started")

	planCtx, planCancel := context.WithTimeout(ctx, orchestratorCommsTimeout*2)
	initialPlan, err := d.config.OrchestratorComms.RequestInitialPlan(planCtx, initialTransferReq)
	planCancel()

	if err != nil { // Gère les erreurs de communication OU les erreurs applicatives (ErrorMessage)
		// L'erreur de QuicComms.RequestInitialPlan inclut déjà le message d'erreur de l'orchestrateur
		wrappedErr := fmt.Errorf("%w: %v", ErrOrchestratorPlan, err)
		l.Error("Failed to get initial transfer plan", "error", wrappedErr)
		finalErrorChan <- wrappedErr
		return
	}

	if initialPlan == nil { // Sécurité supplémentaire
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
		err = fmt.Errorf("%w: initial plan missing source file metadata after successful retrieval", ErrOrchestratorPlan)
		l.Error(err.Error(), "plan_id", initialPlan.PlanId)
		finalErrorChan <- err
		return
	}
	mainFileMeta := initialPlan.SourceFileMetadata[0]
	if mainFileMeta.TotalSize <= 0 && len(initialPlan.ChunkAssignments) > 0 {
		// Si la taille est toujours 0 mais qu'il y a des chunks, c'est un problème de logique orchestrateur
		err = fmt.Errorf("%w: orchestrator provided plan with chunks but TotalSize is %d", ErrOrchestratorPlan, mainFileMeta.TotalSize)
		l.Error(err.Error(), "plan_id", initialPlan.PlanId)
		finalErrorChan <- err
		return
	}
	d.muCurrentPlan.Lock()
	d.currentPlanSnapshot = initialPlan
	d.muCurrentPlan.Unlock()
	l = l.With("plan_id", initialPlan.PlanId, "file_id", mainFileMeta.FileId) // Mettre à jour le logger avec le planID

	if len(initialPlan.ChunkAssignments) == 0 {
		if mainFileMeta.TotalSize > 0 {
			err = fmt.Errorf("%w: initial plan has no chunk assignments for a non-empty file (size: %d)", ErrOrchestratorPlan, mainFileMeta.TotalSize)
			l.Error(err.Error())
			finalErrorChan <- err
			return
		}
		l.Info("Initial plan has no assignments (file is likely empty). Download considered complete.")
		// Vérifier checksum si fichier vide et checksum fourni
		if mainFileMeta.TotalSize == 0 && mainFileMeta.ChecksumValue != "" {
			if !verifyChecksum([]byte{}, mainFileMeta.ChecksumAlgorithm, mainFileMeta.ChecksumValue) {
				finalErrorChan <- ErrChecksumMismatch
				return
			}
		}
		finalErrorChan <- nil // Succès pour un fichier vide
		return
	}
	l.Info("Initial transfer plan received", "num_chunks", len(initialPlan.ChunkAssignments))

	destFile, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePermission)
	if err != nil {
		finalErrorChan <- fmt.Errorf("%w: creating destination file %s: %v", ErrFileWriteFailed, destPath, err)
		return
	}
	defer destFile.Close()
	if mainFileMeta.TotalSize > 0 {
		if err := destFile.Truncate(mainFileMeta.TotalSize); err != nil {
			l.Warn("Failed to pre-allocate file space", "path", destPath, "error", err)
		}
	}

	totalChunksInPlan := len(initialPlan.ChunkAssignments)
	var (
		wg                           sync.WaitGroup
		resultsChan                  = make(chan ChunkDownloadResult, d.config.Concurrency) // Assez grand pour ne pas bloquer les workers
		downloadedSize               atomic.Int64
		completedChunksCount         atomic.Int32
		permanentlyFailedChunksCount atomic.Int32
		chunkStates                  = make(map[uint64]*chunkTrack, totalChunksInPlan)
		muChunkStates                sync.RWMutex
		jobsChan                     = make(chan downloadWorkerJob, d.config.Concurrency*2) // Pour initiaux et retries
		updatedPlanExternalChan      = make(chan *qwrappb.UpdatedTransferPlan, 10)
		activeDownloads              atomic.Int32
		nextChunkToWriteID           atomic.Uint64
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	muChunkStates.Lock()
	sortedInitialAssignments := make([]*qwrappb.ChunkAssignment, len(initialPlan.ChunkAssignments))
	copy(sortedInitialAssignments, initialPlan.ChunkAssignments)
	sort.Slice(sortedInitialAssignments, func(i, j int) bool {
		return sortedInitialAssignments[i].ChunkInfo.ChunkId < sortedInitialAssignments[j].ChunkInfo.ChunkId
	})
	for _, assignment := range sortedInitialAssignments {
		if assignment.ChunkInfo == nil {
			l.Error("Corrupted initial plan: assignment with nil ChunkInfo") /* Gérer erreur fatale */
			return
		}
		chunkID := assignment.ChunkInfo.ChunkId
		chunkStates[chunkID] = &chunkTrack{info: assignment.ChunkInfo, currentAssignment: assignment, currentAttemptOnAssign: 1}
		select {
		case jobsChan <- downloadWorkerJob{assignment: assignment, attempt: 1}:
			activeDownloads.Add(1)
		default:
			l.Error("Failed to send initial job (jobsChan full)")
			finalErrorChan <- errors.New("job channel init failed")
			muChunkStates.Unlock()
			return
		}
	}
	muChunkStates.Unlock()

	for i := 0; i < d.config.Concurrency; i++ {
		wg.Add(1)
		go d.downloadWorker(workerCtx, i, jobsChan, resultsChan, &wg)
	}

	if d.config.OrchestratorComms != nil {
		l.Info("Starting listener for updated plans from orchestrator", "plan_id", initialPlan.PlanId)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// updatedPlanExternalChan sera fermé par la goroutine interne de ListenForUpdatedPlans
			errListener := d.config.OrchestratorComms.ListenForUpdatedPlans(workerCtx, initialPlan.PlanId, updatedPlanExternalChan)
			// Logguer l'erreur si ListenForUpdatedPlans retourne une erreur (ex: échec d'ouverture du stream initial)
			if errListener != nil && !errors.Is(errListener, context.Canceled) && workerCtx.Err() == nil {
				l.Error("OrchestratorComms.ListenForUpdatedPlans returned an error", "plan_id", initialPlan.PlanId, "error", errListener)
				// Si cette communication est critique, on pourrait annuler tout le téléchargement.
				// workerCancel()
			}
			l.Info("Goroutine for ListenForUpdatedPlans has finished.", "plan_id", initialPlan.PlanId)
			// NE PAS fermer updatedPlanExternalChan ici.
		}()
	} else {
		close(updatedPlanExternalChan) // Pas de comms, donc on ferme pour que le select ne bloque pas.
	}

	var finalErrLoop error
	outstandingChunks := int32(totalChunksInPlan)
	reassignmentCheckTicker := time.NewTicker(chunkReassignmentTimeout / 2) // Vérifier périodiquement les chunks en attente
	defer reassignmentCheckTicker.Stop()

	// --- Boucle Principale de Gestion ---
	for outstandingChunks > 0 {
		// Envoyer la progression
		// ... (logique existante)

		select {
		case <-ctx.Done():
			finalErrLoop = ErrDownloadCancelled
			l.Info("Download cancelled by parent context.")
			goto endLoopDownload

		case updatedPlan, ok := <-updatedPlanExternalChan:
			if !ok {
				updatedPlanExternalChan = nil
				continue
			}
			if updatedPlan == nil {
				continue
			}
			l.Info("Downloader received UpdatedTransferPlan", "num_new_assign", len(updatedPlan.NewOrUpdatedAssignments), "num_invalidated", len(updatedPlan.InvalidatedChunks))

			d.muCurrentPlan.Lock()
			d.currentPlanSnapshot = mergePlan(d.currentPlanSnapshot, updatedPlan) // Mettre à jour la vue locale du plan
			d.muCurrentPlan.Unlock()

			muChunkStates.Lock()
			for _, newAssign := range updatedPlan.NewOrUpdatedAssignments {
				if newAssign == nil || newAssign.ChunkInfo == nil {
					continue
				}
				chunkID := newAssign.ChunkInfo.ChunkId
				track, exists := chunkStates[chunkID]
				if !exists || track.isCompleted || track.isPermanentlyFailed {
					continue
				}
				l.Info("Processing new assignment from orchestrator", "chunk_id", chunkID, "new_agent", newAssign.AgentId)
				track.currentAssignment = newAssign
				track.currentAttemptOnAssign = 1
				track.localRetryTotalCount = 0
				track.awaitingReassignment = false
				select {
				case jobsChan <- downloadWorkerJob{assignment: newAssign, attempt: 1}:
					activeDownloads.Add(1)
				default:
					l.Warn("Could not re-queue job with new assignment (jobsChan full)", "chunk_id", chunkID)
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
					if !track.isPermanentlyFailed {
						track.isPermanentlyFailed = true
						permanentlyFailedChunksCount.Add(1)
						atomic.AddInt32(&outstandingChunks, -1)
					}
				}
			}
			muChunkStates.Unlock()

		case result, ok := <-resultsChan:
			if !ok {
				if activeDownloads.Load() == 0 && outstandingChunks > 0 && workerCtx.Err() == nil {
					finalErrLoop = errors.New("results channel closed but outstanding chunks remain and not cancelled")
					l.Error(finalErrLoop.Error())
				}
				goto endLoopDownload
			}
			activeDownloads.Add(-1)

			muChunkStates.Lock() // Verrouiller pour accéder/modifier chunkStates
			track, exists := chunkStates[result.ChunkInfo.ChunkId]
			if !exists || track.isCompleted || track.isPermanentlyFailed {
				muChunkStates.Unlock()
				continue
			}
			// Le track.currentAssignment pourrait avoir changé si un UpdatedTransferPlan est arrivé PENDANT que ce chunk était en vol.
			// On doit comparer result.AgentID avec track.currentAssignment.AgentId pour savoir si c'est un résultat pour l'assignation actuelle.
			isStaleResult := track.currentAssignment.AgentId != result.AgentID || track.currentAssignment.ChunkInfo.ChunkId != result.ChunkInfo.ChunkId
			muChunkStates.Unlock() // Libérer avant d'envoyer le rapport

			statusReport := qwrappb.ChunkTransferStatus{ChunkInfo: &result.ChunkInfo, AgentId: result.AgentID}
			if result.Err != nil {
				statusReport.Details = result.Err.Error()
			}

			if result.Err != nil { // Échec de la tentative de téléchargement
				l.Warn("Chunk download attempt failed", "chunk_id", result.ChunkInfo.ChunkId, "agent", result.AgentID, "attempt_on_assign", result.Attempt, "total_local_attempts_for_chunk", track.localRetryTotalCount+1, "error", result.Err.Error())
				statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR
				if errors.Is(result.Err, context.Canceled) || errors.Is(result.Err, context.DeadlineExceeded) || errors.Is(result.Err, net.ErrClosed) {
					statusReport.Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_AGENT_UNREACHABLE
				}
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
					default:
						l.Warn("Could not queue local retry (jobsChan full)", "chunk_id", track.info.ChunkId)
					}
				} else { // Max retries locaux sur cette assignation
					l.Warn("Max local retries on current assignment. Requesting reassignment.", "chunk_id", track.info.ChunkId, "agent", track.currentAssignment.AgentId)
					track.awaitingReassignment = true
					track.lastAwaitingReassignmentTime = time.Now()
					reassignReqReport := statusReport
					reassignReqReport.Event = qwrappb.TransferEvent_REASSIGNMENT_REQUESTED
					reassignReqReport.Details = "Max local retries on assignment " + track.currentAssignment.AgentId
					d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &reassignReqReport)
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
				d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &statusReport)

				if isVerified {
					muChunkStates.Lock()
					if !track.isCompleted && !track.isPermanentlyFailed { // Éviter double traitement
						track.data = result.Data
						track.isCompleted = true
						track.awaitingReassignment = false
						completedChunksCount.Add(1)
						atomic.AddInt32(&outstandingChunks, -1)
						downloadedSize.Add(int64(len(result.Data)))

						// Écrire les données du chunk dans le fichier immédiatement
						if track.info.Range != nil {
							_, writeErr := destFile.WriteAt(track.data, track.info.Range.Offset)
							if writeErr != nil {
								l.Error("Failed to write chunk to file", "chunk_id", track.info.ChunkId, "error", writeErr)
								track.isPermanentlyFailed = true
								track.isCompleted = false
								permanentlyFailedChunksCount.Add(1)
								completedChunksCount.Add(-1)
								atomic.AddInt32(&outstandingChunks, 1)
								muChunkStates.Unlock()
								if finalErrLoop == nil {
									finalErrLoop = fmt.Errorf("failed to write chunk %d to file: %w", track.info.ChunkId, writeErr)
								}
								continue
							}
							l.Debug("Chunk written to file", "chunk_id", track.info.ChunkId, "offset", track.info.Range.Offset, "size", len(track.data))
						}

						// Libérer la mémoire après écriture
						track.data = nil

						// Essayer d'écrire les chunks suivants qui pourraient être prêts
						muChunkStates.Unlock()
						d.tryWritePendingChunks(destFile, chunkStates, &muChunkStates, &nextChunkToWriteID, mainFileMeta.FileId)
					} else {
						muChunkStates.Unlock()
					}
				}
			}

		case <-reassignmentCheckTicker.C: // Vérifier les chunks en attente de réassignation
			muChunkStates.Lock()
			now := time.Now()
			for chunkID, tr := range chunkStates {
				if tr.awaitingReassignment && !tr.isCompleted && !tr.isPermanentlyFailed {
					if now.Sub(tr.lastAwaitingReassignmentTime) > chunkReassignmentTimeout {
						l.Error("Chunk awaiting reassignment timed out, marking as perm. failed.", "chunk_id", chunkID, "agent", tr.currentAssignment.AgentId)
						if !tr.isPermanentlyFailed {
							tr.isPermanentlyFailed = true
							tr.awaitingReassignment = false
							permanentlyFailedChunksCount.Add(1)
							atomic.AddInt32(&outstandingChunks, -1)
							statusReport := qwrappb.ChunkTransferStatus{ChunkInfo: tr.info, Event: qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR, AgentId: tr.currentAssignment.AgentId, Details: "Reassignment request timed out"}
							d.sendOrchestratorReport(workerCtx, initialPlan.PlanId, &statusReport)
						}
					}
				}
			}
			muChunkStates.Unlock()
		} // Fin du select
	} // Fin de la boucle for outstandingChunks > 0

endLoopDownload:
	if finalErrLoop != nil && workerCtx.Err() == nil {
		workerCancel()
	} else if workerCtx.Err() != nil && finalErrLoop == nil {
		finalErrLoop = workerCtx.Err()
	}

	l.Debug("Main download loop ended. Waiting for all goroutines.")
	wg.Wait()
	if resultsChan != nil {
		close(resultsChan)
	}
	l.Info("All downloader goroutines have finished.")

	// Tentative finale d'écriture et vérifications finales
	_, writeErr := d.tryWritePendingChunks(destFile, chunkStates, &muChunkStates, &nextChunkToWriteID, mainFileMeta.FileId)
	if writeErr != nil && finalErrLoop == nil {
		finalErrLoop = writeErr
	}

	muChunkStates.RLock()
	finalCompleted := completedChunksCount.Load()
	finalFailedPerm := permanentlyFailedChunksCount.Load()
	// Compter les chunks non résolus (ni complétés, ni échoués définitivement)
	var unresolvedAwaitingCount int32 = 0
	for _, tr := range chunkStates {
		if !tr.isCompleted && !tr.isPermanentlyFailed {
			unresolvedAwaitingCount++
		}
	}
	muChunkStates.RUnlock()

	if finalErrLoop == nil {
		// Si tous les chunks sont soit complétés, soit marqués comme échec permanent.
		if (finalCompleted+finalFailedPerm) < int32(totalChunksInPlan) || unresolvedAwaitingCount > 0 {
			finalErrLoop = fmt.Errorf("%w: not all chunks resolved (completed: %d, failed_perm: %d, unresolved_still_pending: %d, total: %d)",
				ErrChunkDownloadFailed, finalCompleted, finalFailedPerm, unresolvedAwaitingCount, totalChunksInPlan)
		} else if finalFailedPerm > 0 { // Tous résolus, mais certains ont échoué
			finalErrLoop = fmt.Errorf("%w: %d chunks failed permanently", ErrChunkDownloadFailed, finalFailedPerm)
		} else if int(nextChunkToWriteID.Load()) != totalChunksInPlan { // Tous complétés et aucun échec, mais pas tout écrit
			finalErrLoop = fmt.Errorf("%w: file assembly incomplete, wrote %d of %d chunks (all chunks reported downloaded)",
				ErrFileWriteFailed, nextChunkToWriteID.Load(), totalChunksInPlan)
		}
	}

	// Vérification du checksum global
	if finalErrLoop == nil && mainFileMeta.ChecksumValue != "" && mainFileMeta.ChecksumAlgorithm != qwrappb.ChecksumAlgorithm_CHECKSUM_ALGORITHM_UNKNOWN {
		if errSync := destFile.Sync(); errSync != nil {
			d.config.Logger.Error("Failed to sync destination file before checksum", "error", errSync)
		}
		if _, errSeek := destFile.Seek(0, io.SeekStart); errSeek != nil {
			if finalErrLoop == nil {
				finalErrLoop = fmt.Errorf("failed to seek for checksum: %w", errSeek)
			}
		} else {
			// Utiliser l'algo du FileMeta
			var hasher hash.Hash
			d.muCurrentPlan.RLock()
			algo := d.currentPlanSnapshot.SourceFileMetadata[0].ChecksumAlgorithm // Utiliser le plan actuel
			d.muCurrentPlan.RUnlock()
			switch algo {
			case qwrappb.ChecksumAlgorithm_SHA256:
				hasher = sha256.New()
			// TODO: Ajouter d'autres algos supportés
			default:
				d.config.Logger.Warn("Unsupported global checksum algorithm for verification", "algo", mainFileMeta.ChecksumAlgorithm)
			}
			if hasher != nil {
				if _, errCopy := io.Copy(hasher, destFile); errCopy != nil {
					finalErrLoop = fmt.Errorf("failed to read dest file for checksum: %w", errCopy)
				} else {
					actualChecksum := hex.EncodeToString(hasher.Sum(nil))
					if actualChecksum != mainFileMeta.ChecksumValue {
						finalErrLoop = fmt.Errorf("%w: global checksum mismatch (expected: %s, actual: %s)", ErrChecksumMismatch, mainFileMeta.ChecksumValue, actualChecksum)
					} else {
						d.config.Logger.Info("Global file checksum VERIFIED.", "checksum", actualChecksum)
					}
				}
			}
		}
	}

	if finalErrLoop != nil {
		finalErrorChan <- finalErrLoop
		d.config.Logger.Error("Download process finished with error", "plan_id", initialPlan.PlanId, "error", finalErrLoop)
	} else {
		d.config.Logger.Info("Download process completed successfully", "plan_id", initialPlan.PlanId, "file_id", mainFileMeta.FileId)
		finalErrorChan <- nil
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

func (d *downloaderImpl) tryWritePendingChunks(
	destFile *os.File,
	chunkStates map[uint64]*chunkTrack, // Lecture seule ici après lock externe
	muChunkStates *sync.RWMutex,
	nextChunkToWriteID *atomic.Uint64,
	fileID string,
) (processedAChunk bool, criticalWriteError error) {
	muChunkStates.Lock()
	defer muChunkStates.Unlock()

	processedAChunk = false

	for {
		currentWriteID := nextChunkToWriteID.Load()
		track, exists := chunkStates[currentWriteID]

		if !exists || !track.isCompleted {
			break
		}

		// Si le chunk est marqué comme complété mais n'a pas de données en mémoire,
		// c'est qu'il a déjà été écrit sur le disque
		if track.data == nil {
			nextChunkToWriteID.Add(1)
			processedAChunk = true
			continue
		}

		processedAChunk = true // On a trouvé un chunk à traiter

		chunkRange := track.info.Range

		if int64(len(track.data)) != chunkRange.Length {
			d.config.Logger.Error("Data length mismatch for writing chunk, marking as permanently failed",
				"chunk_id", currentWriteID, "expected", chunkRange.Length, "actual", len(track.data))

			track.isPermanentlyFailed = true // Marquer comme corrompu DANS le track
			track.isCompleted = false
			track.data = nil // Libérer la mémoire
			criticalWriteError = fmt.Errorf("chunk %d data length mismatch, permanently failed", currentWriteID)
			nextChunkToWriteID.Add(1) // On passe au suivant pour ne pas rester bloqué
			continue                  // Essayer le prochain chunk si possible, ou la boucle se terminera
		}

		_, err := destFile.WriteAt(track.data, chunkRange.Offset)
		if err != nil {
			d.config.Logger.Error("Failed to write chunk to file, critical error",
				"chunk_id", currentWriteID, "offset", chunkRange.Offset, "error", err)
			track.isPermanentlyFailed = true
			track.isCompleted = false
			track.data = nil
			criticalWriteError = fmt.Errorf("critical file write error for chunk %d: %w", currentWriteID, err)
			nextChunkToWriteID.Add(1) // Passer au chunk suivant malgré l'erreur
			continue
		}

		d.config.Logger.Debug("Successfully wrote chunk to disk",
			"chunk_id", currentWriteID, "offset", chunkRange.Offset, "size", len(track.data))

		track.data = nil // Libérer la mémoire après écriture
		nextChunkToWriteID.Add(1)
	}
	return
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
	resultsChan chan<- ChunkDownloadResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	d.config.Logger.Debug("Download worker started", "id", workerID)

	for {
		var job downloadWorkerJob
		var ok bool

		select {
		case <-ctx.Done():
			d.config.Logger.Debug("Download worker shutting down due to context cancellation", "id", workerID)
			return
		case job, ok = <-jobsChan:
			if !ok {
				d.config.Logger.Debug("Download worker shutting down as jobs channel closed", "id", workerID)
				return
			}
		}

		d.config.Logger.Debug("Worker picked up job",
			"worker_id", workerID,
			"file_id", job.assignment.ChunkInfo.FileId, "chunk_id", job.assignment.ChunkInfo.ChunkId,
			"agent_addr", job.assignment.AgentAddress, "attempt_for_assignment", job.attempt,
		)

		// Vérifier si le contexte global n'est pas déjà annulé avant de commencer le travail
		if ctx.Err() != nil {
			d.config.Logger.Debug("Global context cancelled before starting job fetch", "worker_id", workerID, "chunk_id", job.assignment.ChunkInfo.ChunkId)
			// Envoyer un résultat d'erreur pour ce job non tenté
			select {
			case resultsChan <- ChunkDownloadResult{ChunkInfo: *job.assignment.ChunkInfo, Err: ctx.Err(), AgentID: job.assignment.AgentId, Attempt: job.attempt}:
			case <-time.After(1 * time.Second): // Éviter de bloquer indéfiniment si resultsChan est plein
				d.config.Logger.Warn("Timeout sending cancellation result to resultsChan", "worker_id", workerID)
			}
			continue // Prendre le prochain job ou sortir
		}

		chunkData, err := d.fetchChunk(ctx, job.assignment, workerID)

		// Envoyer le résultat, même en cas d'annulation du contexte pendant fetchChunk
		select {
		case resultsChan <- ChunkDownloadResult{
			ChunkInfo: *job.assignment.ChunkInfo, // Envoyer une copie
			Data:      chunkData,
			Err:       err, // Peut être context.Canceled ou context.DeadlineExceeded
			AgentID:   job.assignment.AgentId,
			Attempt:   job.attempt,
		}:
		case <-ctx.Done(): // Si le contexte global est annulé pendant l'envoi du résultat
			d.config.Logger.Debug("Context cancelled while worker sending result", "worker_id", workerID, "chunk_id", job.assignment.ChunkInfo.ChunkId)
			return
		case <-time.After(5 * time.Second): // Timeout pour l'envoi du résultat, pour éviter un deadlock si la boucle principale est bloquée
			d.config.Logger.Error("Timeout sending job result to resultsChan. Potential deadlock or slow main loop.",
				"worker_id", workerID, "chunk_id", job.assignment.ChunkInfo.ChunkId)
			// Que faire ici ? Si on retourne, on perd le résultat. Si on boucle, on peut bloquer.
			// Normalement, resultsChan devrait être consommé.
			// On pourrait essayer de renvoyer avec un select non bloquant une fois de plus.
			return // Sortir pour éviter de bloquer indéfiniment ce worker.
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
	msgWriter := d.config.MessageWriterFactory(stream, fetchLogger.With("role", "writer")) // Logger contextuel pour le writer

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

	// Lire exactement expectedSize octets. io.ReadFull est approprié ici.
	// Le stream QUIC respectera le contexte `fetchCtx` pour l'annulation globale de l'opération de lecture.
	// Si le serveur ferme le flux ou si la connexion est perdue, ReadFull retournera une erreur (probablement io.EOF ou io.ErrUnexpectedEOF).
	bytesRead, readErr := io.ReadFull(stream, payloadBuf)

	if readErr != nil {
		// Si le contexte a été annulé/a expiré PENDANT la lecture, c'est l'erreur prioritaire.
		if fetchCtx.Err() != nil {
			fetchLogger.Warn("Context cancelled/timed out during chunk data read from agent", "error", fetchCtx.Err(), "bytes_read_before_cancel", bytesRead)
			return nil, fmt.Errorf("context error reading chunk %d data from %s: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, fetchCtx.Err())
		}
		// Sinon, c'est une erreur d'I/O.
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			// Si nous obtenons EOF mais que nous n'avons pas lu expectedSize, c'est une lecture incomplète.
			if int64(bytesRead) < expectedSize {
				fetchLogger.Warn("Incomplete chunk received from agent", "expected", expectedSize, "received", bytesRead, "underlying_error", readErr)
				return nil, fmt.Errorf("incomplete chunk %d from %s: got %d, want %d: %w",
					assignment.ChunkInfo.ChunkId, assignment.AgentAddress, bytesRead, expectedSize, readErr)
			}
			// Si bytesRead == expectedSize et readErr == io.EOF, c'est normal pour ReadFull.
			fetchLogger.Debug("ReadFull successful with expected EOF at end of chunk.")
		} else {
			fetchLogger.Warn("Failed to read chunk data from agent (non-EOF error)", "error", readErr, "bytes_read", bytesRead)
			return nil, fmt.Errorf("read chunk %d data from %s failed: %w", assignment.ChunkInfo.ChunkId, assignment.AgentAddress, readErr)
		}
	}
	// Si readErr est nil, cela signifie que bytesRead == expectedSize.

	// Renvoyer une copie des données lues pour éviter les problèmes si payloadBuf (du pool) est modifié/réutilisé.
	// C'est crucial si les données du chunk doivent persister au-delà de cet appel de fonction.
	dataCopy := make([]byte, bytesRead) // bytesRead devrait être égal à expectedSize ici si pas d'erreur
	copy(dataCopy, payloadBuf[:bytesRead])

	fetchLogger.Debug("Successfully fetched chunk data", "size", len(dataCopy))
	return dataCopy, nil
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
