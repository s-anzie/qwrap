package downloader

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"qwrap/pkg/qwrappb"
)

type jobDispatcher struct {
	jobsOutChan       chan<- downloadJob
	planUpdatesInChan <-chan *qwrappb.UpdatedTransferPlan
	retryJobChan      chan downloadJob
	logger            *slog.Logger

	mu          sync.Mutex
	pendingJobs map[uint64]downloadJob
	currentPlan *qwrappb.TransferPlan

	lastDispatchAttempt time.Time
	dispatchCoolDown    time.Duration
}

func newJobDispatcher(
	jobsOut chan<- downloadJob,
	planUpdatesIn <-chan *qwrappb.UpdatedTransferPlan,
	logger *slog.Logger,
) *jobDispatcher {
	return &jobDispatcher{
		jobsOutChan:       jobsOut,
		planUpdatesInChan: planUpdatesIn,
		retryJobChan:      make(chan downloadJob, ResultBufferSize),
		logger:            logger,
		pendingJobs:       make(map[uint64]downloadJob),
		dispatchCoolDown:  100 * time.Millisecond,
	}
}

func (jd *jobDispatcher) run(ctx context.Context, wg *sync.WaitGroup, initialPlan *qwrappb.TransferPlan) {
	defer wg.Done()
	jd.logger.Info("JobDispatcher starting...")
	defer jd.logger.Info("JobDispatcher stopped.")

	jd.mu.Lock()
	jd.currentPlan = initialPlan
	if initialPlan != nil {
		for _, assignment := range initialPlan.ChunkAssignments {
			if assignment != nil && assignment.ChunkInfo != nil {
				job := downloadJob{
					assignment:               assignment,
					attemptForThisAssignment: 0, // Sera 1 lors du premier dispatch
					totalAttemptsForChunk:    0, // Sera 1 lors du premier dispatch
				}
				jd.pendingJobs[assignment.ChunkInfo.ChunkId] = job
			}
		}
	}
	jd.mu.Unlock()

	initialDispatchDone := make(chan struct{})
	go func() {
		defer close(initialDispatchDone)
		if len(jd.pendingJobs) > 0 {
			jd.dispatchPendingJobs(ctx, true)
		}
	}()

	ticker := time.NewTicker(jd.dispatchCoolDown * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			jd.logger.Info("JobDispatcher context cancelled.")
			return

		case <-initialDispatchDone:
			initialDispatchDone = nil

		case jobToRetry, ok := <-jd.retryJobChan:
			if !ok {
				jd.retryJobChan = nil
				jd.logger.Info("Retry job channel closed.")
				// Condition de sortie si tous les canaux d'entrée sont fermés et plus de jobs.
				if initialDispatchDone == nil && jd.planUpdatesInChan == nil && len(jd.pendingJobs) == 0 {
					return
				}
				continue
			}
			// Les compteurs dans jobToRetry sont ceux *avant* cette nouvelle tentative.
			// Le dispatcher va incrémenter attemptForThisAssignment avant d'envoyer.
			// totalAttemptsForChunk sera aussi incrémenté avant l'envoi.
			jd.logger.Debug("Received job to retry",
				"chunk_id", jobToRetry.assignment.ChunkInfo.ChunkId,
				"prev_attempt_on_assign", jobToRetry.attemptForThisAssignment, // C'est l'état du job qui a échoué
				"prev_total_attempts", jobToRetry.totalAttemptsForChunk) // C'est l'état du job qui a échoué

			jd.mu.Lock()
			currentAssignForChunk, planHasChunk := jd.findAssignmentInCurrentPlan(jobToRetry.assignment.ChunkInfo.ChunkId)
			if !planHasChunk {
				jd.logger.Warn("Chunk to retry not found in current plan, discarding retry", "chunk_id", jobToRetry.assignment.ChunkInfo.ChunkId)
				jd.mu.Unlock()
				continue
			}

			// Préparer le nouveau job de retry
			newRetryJob := downloadJob{
				assignment:               currentAssignForChunk,            // Utiliser l'assignation à jour du plan
				attemptForThisAssignment: 0,                                // Sera incrémenté par dispatchPendingJobs
				totalAttemptsForChunk:    jobToRetry.totalAttemptsForChunk, // Le dispatcher incrémentera le total
			}
			// Si l'agent est le même, on continue les tentatives pour *cette* assignation
			if currentAssignForChunk.AgentId == jobToRetry.assignment.AgentId {
				newRetryJob.attemptForThisAssignment = jobToRetry.attemptForThisAssignment // Le dispatcher incrémentera
			}
			// sinon (agent différent), attemptForThisAssignment reste à 0 (sera 1)

			jd.pendingJobs[jobToRetry.assignment.ChunkInfo.ChunkId] = newRetryJob
			jd.mu.Unlock()
			jd.dispatchPendingJobs(ctx, false)

		case updatedPlan, ok := <-jd.planUpdatesInChan:
			if !ok {
				jd.planUpdatesInChan = nil
				jd.logger.Info("Plan update channel closed.")
				if initialDispatchDone == nil && jd.retryJobChan == nil && len(jd.pendingJobs) == 0 {
					return
				}
				continue
			}
			jd.logger.Info("JobDispatcher received updated plan", "plan_id", updatedPlan.PlanId)
			jd.mu.Lock()
			jd.currentPlan = mergePlan(jd.currentPlan, updatedPlan)

			for _, invalidatedChunk := range updatedPlan.InvalidatedChunks {
				delete(jd.pendingJobs, invalidatedChunk.ChunkId)
				jd.logger.Debug("Removed invalidated chunk from pending jobs", "chunk_id", invalidatedChunk.ChunkId)
			}
			for _, newAssign := range updatedPlan.NewOrUpdatedAssignments {
				totalAttemptsSoFar := 0
				// Si le chunk existait déjà (réassignation), on garde son compteur global de tentatives.
				// Si c'est un tout nouveau chunk (ne devrait pas arriver via UpdatedTransferPlan), on part de 0.
				if existingJob, found := jd.pendingJobs[newAssign.ChunkInfo.ChunkId]; found {
					totalAttemptsSoFar = existingJob.totalAttemptsForChunk
				} else if oldAssignState, foundOldState := jd.findOriginalStateForUpdatedChunk(newAssign.ChunkInfo.ChunkId); foundOldState {
					// Si le job n'est plus dans pendingJobs (déjà dispatché mais on a une update)
					// on récupère son total de tentatives depuis l'état avant update.
					totalAttemptsSoFar = oldAssignState.totalAttemptsForChunk
				}

				job := downloadJob{
					assignment:               newAssign,
					attemptForThisAssignment: 0,                  // Nouvelle assignation (ou mise à jour), réinitialiser la tentative pour *cette* assignation.
					totalAttemptsForChunk:    totalAttemptsSoFar, // Conserver le total pour le chunk.
				}
				jd.pendingJobs[newAssign.ChunkInfo.ChunkId] = job
				jd.logger.Debug("Added/Updated job due to plan update", "chunk_id", newAssign.ChunkInfo.ChunkId, "new_agent", newAssign.AgentId)
			}
			jd.mu.Unlock()
			jd.dispatchPendingJobs(ctx, false)

		case <-ticker.C:
			if len(jd.pendingJobs) > 0 {
				jd.dispatchPendingJobs(ctx, false)
			}
		}
	}
}

// findOriginalStateForUpdatedChunk est un helper pour trouver l'état d'un job qui pourrait avoir été
// déjà dispatché mais pour lequel une mise à jour d'assignation arrive.
// Doit être appelé avec jd.mu détenu.
func (jd *jobDispatcher) findOriginalStateForUpdatedChunk(chunkID uint64) (downloadJob, bool) {
	// Cette fonction est un peu spéculative, car le job pourrait ne plus être dans pendingJobs.
	// Le `ResultAggregator` a une vision plus complète de l'état des chunks.
	// Pour le `JobDispatcher`, le plus simple est de se baser sur `pendingJobs`.
	// Si une mise à jour arrive pour un chunk non en attente, on assume que ses tentatives globales sont à jour
	// via les `retryJobChan` ou qu'il s'agit d'une nouvelle entrée dans le plan.
	// Cette fonction n'est peut-être pas nécessaire si ResultAggregator gère bien les compteurs.
	// On va la simplifier : si le job n'est pas dans pendingJobs, on assume totalAttemptsForChunk = 0
	// car le ResultAggregator sera la source de vérité pour le nombre de tentatives via les retry.
	job, exists := jd.pendingJobs[chunkID]
	return job, exists
}

func (jd *jobDispatcher) dispatchPendingJobs(ctx context.Context, isInitialDispatch bool) {
	jd.mu.Lock()
	defer jd.mu.Unlock()

	if !isInitialDispatch && time.Since(jd.lastDispatchAttempt) < jd.dispatchCoolDown {
		return
	}
	jd.lastDispatchAttempt = time.Now()

	var jobKeys []uint64
	for k := range jd.pendingJobs {
		jobKeys = append(jobKeys, k)
	}
	sort.Slice(jobKeys, func(i, j int) bool { return jobKeys[i] < jobKeys[j] })

	for _, chunkID := range jobKeys {
		job, exists := jd.pendingJobs[chunkID]
		if !exists {
			continue
		}

		jobToSend := job
		jobToSend.attemptForThisAssignment++ // Incrémenter pour *cette* tentative sur l'assignation actuelle
		jobToSend.totalAttemptsForChunk++    // Incrémenter le compteur global de tentatives pour ce chunk

		select {
		case jd.jobsOutChan <- jobToSend:
			jd.logger.Debug("Dispatched job for chunk",
				"chunk_id", jobToSend.assignment.ChunkInfo.ChunkId,
				"agent", jobToSend.assignment.AgentId,
				"attempt_on_this_assign", jobToSend.attemptForThisAssignment,
				"total_attempts_for_chunk", jobToSend.totalAttemptsForChunk)
			delete(jd.pendingJobs, chunkID)
		case <-ctx.Done():
			jd.logger.Info("Context cancelled during job dispatch.")
			return
		default:
			jd.logger.Debug("Jobs output channel full, will retry dispatch later.", "pending_count", len(jd.pendingJobs))
			// Ne pas supprimer le job de pendingJobs, il sera retenté plus tard.
			// Les compteurs ne sont pas incrémentés pour ce job non envoyé.
			return
		}
	}
}

func (jd *jobDispatcher) findAssignmentInCurrentPlan(chunkID uint64) (*qwrappb.ChunkAssignment, bool) {
	// Cette fonction doit être appelée avec jd.mu DÉJÀ détenu si elle modifie currentPlan
	// ou si currentPlan est modifié par une autre goroutine.
	// Ici, on ne fait que lire, donc RLock serait bien si mu était RWMutex.
	// Avec un Mutex simple, le Lock externe suffit.
	if jd.currentPlan == nil {
		return nil, false
	}
	for _, assign := range jd.currentPlan.ChunkAssignments {
		if assign != nil && assign.ChunkInfo != nil && assign.ChunkInfo.ChunkId == chunkID {
			return assign, true
		}
	}
	return nil, false
}
