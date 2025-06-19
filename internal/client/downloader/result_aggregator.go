package downloader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"qwrap/pkg/qwrappb"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type resultAggregator struct {
	initialPlan          *qwrappb.TransferPlan
	resultsInChan        <-chan *chunkResult
	orderedChunksOutChan chan<- fileWriterJob
	reportsOutChan       chan<- *qwrappb.ClientReport
	progressOutChan      chan<- ProgressInfo
	retryJobsOutChan     chan<- downloadJob
	logger               *slog.Logger

	// Utiliser RWMutex si les lectures de chunkStates sont beaucoup plus fréquentes
	// que les écritures. Pour l'instant, un Mutex simple suffit car les écritures
	// (mises à jour d'état) sont fréquentes à chaque résultat.
	mu                     sync.Mutex
	chunkStates            map[uint64]*chunkState
	totalChunksInPlan      int
	completedChunksCount   atomic.Int32
	permanentlyFailedCount atomic.Int32
	downloadedBytes        atomic.Int64

	maxLocalRetriesPerAssign int
	maxTotalRetriesPerChunk  int
	reassignmentWaitTimeout  time.Duration

	doneChan   chan struct{}
	finalError error
}

func newResultAggregator(
	initialPlan *qwrappb.TransferPlan,
	resultsIn <-chan *chunkResult,
	orderedChunksOut chan<- fileWriterJob,
	reportsOut chan<- *qwrappb.ClientReport,
	progressOut chan<- ProgressInfo,
	logger *slog.Logger,
	maxLocalRetriesPerAssign int,
	maxTotalRetriesPerChunk int,
	reassignmentWaitTimeout time.Duration,
	retryJobsOut chan<- downloadJob,
) *resultAggregator {

	numChunks := 0
	states := make(map[uint64]*chunkState)
	if initialPlan != nil && len(initialPlan.ChunkAssignments) > 0 {
		numChunks = len(initialPlan.ChunkAssignments)
		states = make(map[uint64]*chunkState, numChunks)
		for _, assignment := range initialPlan.ChunkAssignments {
			if assignment != nil && assignment.ChunkInfo != nil {
				states[assignment.ChunkInfo.ChunkId] = &chunkState{
					info:              assignment.ChunkInfo,
					currentAssignment: assignment,
					status:            qwrappb.TransferEvent_TRANSFER_EVENT_UNKNOWN,
				}
			} else {
				logger.Warn("Nil assignment or chunk info in initial plan, skipping state creation for one entry")
				numChunks--
			}
		}
	}

	return &resultAggregator{
		initialPlan:              initialPlan,
		resultsInChan:            resultsIn,
		orderedChunksOutChan:     orderedChunksOut,
		reportsOutChan:           reportsOut,
		progressOutChan:          progressOut,
		retryJobsOutChan:         retryJobsOut,
		logger:                   logger,
		chunkStates:              states,
		totalChunksInPlan:        numChunks,
		maxLocalRetriesPerAssign: maxLocalRetriesPerAssign,
		maxTotalRetriesPerChunk:  maxTotalRetriesPerChunk,
		reassignmentWaitTimeout:  reassignmentWaitTimeout,
		doneChan:                 make(chan struct{}),
	}
}

func (ra *resultAggregator) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ra.logger.Info("ResultAggregator starting...")
	defer func() {
		close(ra.doneChan)
		ra.logger.Info("ResultAggregator stopped.", "final_error_if_any", ra.finalError)
	}()

	progressTicker := time.NewTicker(ProgressReportInterval) // Utilise la constante exportée
	defer progressTicker.Stop()

	reassignmentCheckTicker := time.NewTicker(ra.reassignmentWaitTimeout / 2)
	defer reassignmentCheckTicker.Stop()

	for {
		if ra.totalChunksInPlan > 0 && int(ra.completedChunksCount.Load()+ra.permanentlyFailedCount.Load()) >= ra.totalChunksInPlan {
			if ra.permanentlyFailedCount.Load() > 0 {
				if ra.finalError == nil { // Ne pas écraser une erreur plus prioritaire
					ra.finalError = fmt.Errorf("%d chunks permanently failed during download", ra.permanentlyFailedCount.Load())
				}
			}
			ra.logger.Info("All chunks processed by ResultAggregator.")
			ra.sendProgress()
			return
		}
		if ra.totalChunksInPlan == 0 && ra.initialPlan != nil && len(ra.initialPlan.ChunkAssignments) == 0 {
			ra.logger.Info("No chunks in plan, download considered complete by aggregator.")
			ra.sendProgress()
			return
		}

		select {
		case <-ctx.Done():
			ra.logger.Info("ResultAggregator context cancelled.")
			if ra.finalError == nil {
				ra.finalError = ctx.Err()
			}
			return

		case result, ok := <-ra.resultsInChan:
			if !ok {
				ra.logger.Info("Results channel closed. Aggregator assuming workers are done.")
				if int(ra.completedChunksCount.Load()+ra.permanentlyFailedCount.Load()) < ra.totalChunksInPlan {
					if ra.finalError == nil {
						ra.finalError = errors.New("workers finished but not all chunks processed by aggregator")
						ra.logger.Error(ra.finalError.Error())
					}
				}
				return
			}
			ra.processChunkResult(ctx, result)

		case <-progressTicker.C:
			ra.sendProgress()

		case <-reassignmentCheckTicker.C:
			ra.checkReassignmentTimeouts(ctx)
		}
	}
}

func (ra *resultAggregator) processChunkResult(ctx context.Context, result *chunkResult) {
	ra.mu.Lock() // Utilisation de Lock simple

	chunkID := result.chunkInfo.ChunkId
	state, exists := ra.chunkStates[chunkID]

	if !exists {
		ra.logger.Warn("Received result for untracked chunk", "chunk_id", chunkID)
		ra.mu.Unlock()
		return
	}

	if state.isCompleted || state.isPermanentlyFailed {
		ra.logger.Debug("Received result for already processed chunk", "chunk_id", chunkID)
		ra.mu.Unlock()
		return
	}

	if state.currentAssignment.AgentId != result.job.assignment.AgentId || state.currentAssignment.ChunkInfo.ChunkId != result.job.assignment.ChunkInfo.ChunkId {
		ra.logger.Info("Received stale result for chunk (assignment changed), ignoring",
			"chunk_id", chunkID,
			"result_agent", result.agentID, "result_assignment_chunk_id", result.job.assignment.ChunkInfo.ChunkId,
			"expected_agent", state.currentAssignment.AgentId, "expected_assignment_chunk_id", state.currentAssignment.ChunkInfo.ChunkId)
		ra.mu.Unlock()
		return
	}

	// Mettre à jour les compteurs d'état du chunk avec les valeurs du job qui a produit ce résultat
	state.currentAssignmentAttempts = result.job.attemptForThisAssignment
	state.totalOverallAttempts = result.job.totalAttemptsForChunk

	ra.mu.Unlock() // Libérer avant d'envoyer le rapport ou de potentiellement bloquer sur retryJobsOutChan

	report := &qwrappb.ClientReport{
		PlanId:          ra.initialPlan.PlanId,
		ReportTimestamp: timestamppb.Now(),
		ChunkStatuses: []*qwrappb.ChunkTransferStatus{{
			ChunkInfo:        result.chunkInfo,
			AgentId:          result.agentID,
			Details:          "",
			TransferDuration: durationpb.New(result.downloadTime),
		}},
	}

	if result.err != nil {
		state.lastFailureTime = time.Now() // Mettre à jour avant de potentiellement relâcher le lock
		report.ChunkStatuses[0].Details = result.err.Error()

		ra.logger.Warn("Chunk download attempt failed", "chunk_id", chunkID, "agent", result.agentID,
			"attempt_on_assign", state.currentAssignmentAttempts, "total_overall_attempts", state.totalOverallAttempts, "error", result.err)

		// if errors.Is(result.err, ErrChecksumMismatch) { // Checksum ignoré
		//     report.ChunkStatuses[0].Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_CHECKSUM_MISMATCH
		// } else
		if errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, net.ErrClosed) {
			report.ChunkStatuses[0].Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_AGENT_UNREACHABLE
		} else {
			report.ChunkStatuses[0].Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR
		}

		ra.mu.Lock() // Reprendre le lock pour modifier state
		state.status = report.ChunkStatuses[0].Event

		if state.totalOverallAttempts >= ra.maxTotalRetriesPerChunk {
			ra.logger.Error("Max total retries reached for chunk, marking as permanently failed", "chunk_id", chunkID, "total_attempts", state.totalOverallAttempts)
			if !state.isPermanentlyFailed { // Éviter double comptage
				state.isPermanentlyFailed = true
				state.awaitingReassignment = false
				ra.permanentlyFailedCount.Add(1)
			}
			report.ChunkStatuses[0].Event = qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR
			report.ChunkStatuses[0].Details = fmt.Sprintf("Permanently failed after %d total attempts. Last error: %s", state.totalOverallAttempts, result.err.Error())
		} else if state.currentAssignmentAttempts >= ra.maxLocalRetriesPerAssign {
			ra.logger.Warn("Max local retries reached for chunk on current assignment, requesting reassignment", "chunk_id", chunkID, "agent", result.agentID, "attempts_on_assign", state.currentAssignmentAttempts)
			state.awaitingReassignment = true
			state.lastReassignmentRequestTime = time.Now()
			report.ChunkStatuses[0].Event = qwrappb.TransferEvent_REASSIGNMENT_REQUESTED
			report.ChunkStatuses[0].Details = fmt.Sprintf("Requesting reassignment after %d attempts on agent %s. Last error: %s", state.currentAssignmentAttempts, result.agentID, result.err.Error())
			state.status = qwrappb.TransferEvent_REASSIGNMENT_REQUESTED
		} else {
			ra.logger.Info("Queueing chunk for local retry on same assignment", "chunk_id", chunkID, "next_attempt_on_assign", state.currentAssignmentAttempts+1, "agent", result.agentID)

			jobForRetry := downloadJob{
				assignment:               state.currentAssignment,
				attemptForThisAssignment: state.currentAssignmentAttempts, // Le dispatcher incrémentera pour la *prochaine* tentative sur *cette* assignation
				totalAttemptsForChunk:    state.totalOverallAttempts,      // Le dispatcher incrémentera pour la *prochaine* tentative globale
			}
			ra.mu.Unlock()
			select {
			case ra.retryJobsOutChan <- jobForRetry:
			case <-ctx.Done():
				ra.logger.Info("Context cancelled while queueing local retry", "chunk_id", chunkID)
				// Pas de `default` ici, on veut que la retentative soit envoyée ou que le contexte annule.
				// Le channel retryJobsOutChan doit être suffisamment bufferisé.
			}
			ra.mu.Lock() // Reprendre le lock
		}
		ra.mu.Unlock()

	} else { // Succès
		ra.logger.Debug("Chunk download reported as successful by worker", "chunk_id", chunkID, "agent", result.agentID, "size", len(result.data))
		report.ChunkStatuses[0].Event = qwrappb.TransferEvent_DOWNLOAD_SUCCESSFUL

		ra.mu.Lock()
		if state.isCompleted || state.isPermanentlyFailed {
			ra.logger.Debug("Chunk already processed while handling success, ignoring duplicate", "chunk_id", chunkID)
			ra.mu.Unlock()
			return
		}
		state.status = qwrappb.TransferEvent_DOWNLOAD_SUCCESSFUL
		state.isCompleted = true
		state.awaitingReassignment = false
		state.data = result.data
		ra.mu.Unlock()

		ra.completedChunksCount.Add(1)
		ra.downloadedBytes.Add(int64(len(result.data)))

		fwJob := fileWriterJob{
			chunkID: chunkID,
			offset:  result.chunkInfo.Range.Offset,
			data:    result.data,
		}
		select {
		case ra.orderedChunksOutChan <- fwJob:
		case <-ctx.Done():
			ra.logger.Warn("Context cancelled while sending chunk to file writer", "chunk_id", chunkID)
		}
	}

	select {
	case ra.reportsOutChan <- report:
	case <-ctx.Done():
	default:
		ra.logger.Warn("Reports to orchestrator channel full, report might be delayed or lost for chunk", "chunk_id", chunkID)
	}
}

func (ra *resultAggregator) sendProgress() {
	ra.mu.Lock() // Utiliser Lock simple
	activeApprox := 0
	if ra.chunkStates != nil {
		for _, st := range ra.chunkStates {
			if !st.isCompleted && !st.isPermanentlyFailed {
				activeApprox++
			}
		}
	}
	ra.mu.Unlock()

	var totalSizeBytes int64
	if ra.initialPlan != nil && len(ra.initialPlan.SourceFileMetadata) > 0 && ra.initialPlan.SourceFileMetadata[0] != nil {
		totalSizeBytes = ra.initialPlan.SourceFileMetadata[0].TotalSize
	}

	p := ProgressInfo{
		TotalSizeBytes:      totalSizeBytes,
		DownloadedSizeBytes: ra.downloadedBytes.Load(),
		TotalChunks:         ra.totalChunksInPlan,
		CompletedChunks:     int(ra.completedChunksCount.Load()),
		FailedChunks:        int(ra.permanentlyFailedCount.Load()),
		ActiveDownloads:     activeApprox,
	}
	select {
	case ra.progressOutChan <- p:
	default:
	}
}

func (ra *resultAggregator) checkReassignmentTimeouts(ctx context.Context) {
	ra.mu.Lock()
	defer ra.mu.Unlock()

	now := time.Now()
	for chunkID, state := range ra.chunkStates {
		if state.awaitingReassignment && !state.isCompleted && !state.isPermanentlyFailed {
			if now.Sub(state.lastReassignmentRequestTime) > ra.reassignmentWaitTimeout {
				ra.logger.Error("Chunk awaiting reassignment timed out, marking as permanently failed.", "chunk_id", chunkID, "agent_originally_failed", state.currentAssignment.AgentId)
				if !state.isPermanentlyFailed { // Éviter double comptage
					state.isPermanentlyFailed = true
					state.awaitingReassignment = false
					ra.permanentlyFailedCount.Add(1)
				}

				report := &qwrappb.ClientReport{
					PlanId:          ra.initialPlan.PlanId,
					ReportTimestamp: timestamppb.Now(),
					ChunkStatuses: []*qwrappb.ChunkTransferStatus{{
						ChunkInfo: state.info,
						AgentId:   state.currentAssignment.AgentId,
						Event:     qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR,
						Details:   fmt.Sprintf("Reassignment request timed out after %v", ra.reassignmentWaitTimeout),
					}},
				}
				select {
				case ra.reportsOutChan <- report:
				case <-ctx.Done():
				default:
					ra.logger.Warn("Reports to orchestrator channel full while reporting reassignment timeout", "chunk_id", chunkID)
				}
			}
		}
	}
}
