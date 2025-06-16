package statemanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré

	"github.com/google/uuid" // Pour générer des IDs uniques
)

type stateManagerImpl struct {
	mu        sync.RWMutex
	transfers map[string]*TransferState // Clé: PlanID
	config    StateManagerConfig
	// (Futur) db *badger.DB ou *bolt.DB
	// (Futur) wal *os.File
}

// NewStateManager crée une nouvelle instance de StateManager (en mémoire pour l'instant).
func NewStateManager(config StateManagerConfig) StateManager {
	config.setDefaults()
	return &stateManagerImpl{
		transfers: make(map[string]*TransferState),
		config:    config,
	}
}

// --- Gestion des Transferts ---

func (sm *stateManagerImpl) CreateTransfer(ctx context.Context, clientReq *qwrappb.TransferRequest) (string, *TransferState, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	planID := "plan-" + uuid.NewString() // Générer un ID unique pour le plan

	if _, exists := sm.transfers[planID]; exists {
		// Très improbable avec UUID, mais bonne pratique
		return "", nil, fmt.Errorf("generated planID %s already exists", planID)
	}
	if clientReq == nil || len(clientReq.FilesToTransfer) == 0 {
		return "", nil, errors.New("cannot create transfer from nil or empty request")
	}

	now := time.Now()
	ts := &TransferState{
		PlanID:          planID,
		ClientRequestID: clientReq.RequestId,
		Status:          StatusPending, // Initialement en attente de planification
		Files:           make(map[string]*FileState),
		CreationTime:    now,
		LastUpdateTime:  now,
	}

	for _, fm := range clientReq.FilesToTransfer {
		if fm == nil || fm.FileId == "" {
			sm.config.Logger.Warn("Skipping file with nil metadata or empty FileId in CreateTransfer", "request_id", clientReq.RequestId)
			continue
		}
		ts.Files[fm.FileId] = &FileState{
			Metadata: fm,
			Chunks:   make(map[uint64]*ChunkState), // Sera peuplé par LinkSchedulerPlan
			Status:   StatusPending,
		}
	}
	if len(ts.Files) == 0 {
		return "", nil, errors.New("no valid files to process in transfer request")
	}

	sm.transfers[planID] = ts
	sm.config.Logger.Info("New transfer state created", "plan_id", planID, "client_request_id", clientReq.RequestId)

	// Retourner une copie pour éviter les modifications externes
	// Pour l'instant, on retourne le pointeur, mais pour la persistance, on travaillerait avec des copies.
	return planID, ts, nil
}

func (sm *stateManagerImpl) GetTransfer(ctx context.Context, planID string) (*TransferState, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return nil, ErrTransferNotFound
	}
	// Retourner une copie pour l'isolation (surtout si on avait de la persistance)
	// Pour une version en mémoire simple, retourner le pointeur est acceptable, mais moins sûr.
	// copy := *ts // Copie superficielle, les maps et slices internes sont toujours partagés.
	// Pour une vraie copie profonde, il faudrait une fonction dédiée.
	return ts, nil
}

func (sm *stateManagerImpl) UpdateTransferStatus(ctx context.Context, planID string, status TransferStatus, errorMessage ...string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return ErrTransferNotFound
	}

	oldStatus := ts.Status
	ts.Status = status
	ts.LastUpdateTime = time.Now()

	// if status == StatusFailed && len(errorMessage) > 0 {
	// 	ts.ErrorMessage = errorMessage[0]
	// }
	// if status == StatusCompleted {
	//  ts.CompletionTime = ts.LastUpdateTime
	// }

	sm.config.Logger.Info("Transfer status updated",
		"plan_id", planID,
		"old_status", oldStatus,
		"new_status", status,
	)
	// (Futur) Écrire dans le WAL ici
	return nil
}

func (sm *stateManagerImpl) LinkSchedulerPlan(ctx context.Context, planID string, schedulerPlan *qwrappb.TransferPlan) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return ErrTransferNotFound
	}

	if ts.Status != StatusPending && ts.Status != StatusPlanning {
		return fmt.Errorf("%w: cannot link plan when transfer status is %s", ErrInvalidStateOperation, ts.Status)
	}
	if schedulerPlan == nil {
		return errors.New("schedulerPlan cannot be nil")
	}

	ts.AssignedSchedulerPlan = schedulerPlan // Stocker le plan complet
	ts.Status = StatusInProgress             // Le plan est là, on passe en InProgress

	// Peupler les ChunkStates
	for _, assignment := range schedulerPlan.ChunkAssignments {
		if assignment == nil || assignment.ChunkInfo == nil || assignment.ChunkInfo.FileId == "" {
			sm.config.Logger.Warn("Skipping invalid chunk assignment in LinkSchedulerPlan", "plan_id", planID)
			continue
		}
		fileID := assignment.ChunkInfo.FileId
		chunkID := assignment.ChunkInfo.ChunkId

		fileState, fsExists := ts.Files[fileID]
		if !fsExists {
			// Cela ne devrait pas arriver si CreateTransfer a bien initialisé les FileStates
			// basé sur le clientReq, et que le schedulerPlan est cohérent.
			sm.config.Logger.Error("File ID from scheduler plan not found in transfer state",
				"plan_id", planID, "file_id", fileID)
			// On pourrait créer le FileState ici ou retourner une erreur.
			// Pour être robuste, on le crée s'il manque, en supposant que le schedulerPlan est la source de vérité pour les fichiers.
			sourceMeta := findFileMeta(schedulerPlan.SourceFileMetadata, fileID)
			if sourceMeta == nil {
				sm.config.Logger.Error("File metadata not found in scheduler plan for file ID", "plan_id", planID, "file_id", fileID)
				return fmt.Errorf("metadata for file %s not found in scheduler plan", fileID)
			}
			fileState = &FileState{
				Metadata: sourceMeta,
				Chunks:   make(map[uint64]*ChunkState),
				Status:   StatusInProgress, // Le fichier est maintenant en cours
			}
			ts.Files[fileID] = fileState
		}
		fileState.Status = StatusInProgress // Marquer le fichier comme en cours aussi

		// Créer le ChunkState
		chunkState := &ChunkState{
			ChunkInfo:      assignment.ChunkInfo,         // Stocker une référence
			AssignedAgents: []string{assignment.AgentId}, // Initialement assigné à cet agent
			PrimaryAgent:   assignment.AgentId,
			Status:         qwrappb.TransferEvent_TRANSFER_EVENT_UNKNOWN, // Aucun rapport client encore
			Attempts:       0,
			LastReportTime: time.Now(),
		}
		fileState.Chunks[chunkID] = chunkState
	}
	ts.LastUpdateTime = time.Now()
	sm.config.Logger.Info("Scheduler plan linked to transfer", "plan_id", planID, "num_assignments", len(schedulerPlan.ChunkAssignments))
	// (Futur) Écrire dans le WAL ici
	return nil
}

// Helper pour trouver FileMetadata dans une slice
func findFileMeta(metas []*qwrappb.FileMetadata, fileID string) *qwrappb.FileMetadata {
	for _, meta := range metas {
		if meta != nil && meta.FileId == fileID {
			return meta
		}
	}
	return nil
}

// --- Gestion des Chunks ---

func (sm *stateManagerImpl) UpdateChunkStatus(ctx context.Context, planID string, fileID string, chunkID uint64, agentID string, event qwrappb.TransferEvent, details string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return ErrTransferNotFound
	}
	fileState, fsExists := ts.Files[fileID]
	if !fsExists {
		return ErrFileNotFound
	}
	chunkState, csExists := fileState.Chunks[chunkID]
	if !csExists {
		return ErrChunkNotFound
	}

	chunkState.Status = event
	chunkState.LastReportTime = time.Now()
	if event == qwrappb.TransferEvent_DOWNLOAD_STARTED {
		chunkState.Attempts++
	}
	// (Futur) Mettre à jour des métriques plus fines ici si nécessaire

	sm.config.Logger.Debug("Chunk status updated",
		"plan_id", planID,
		"file_id", fileID,
		"chunk_id", chunkID,
		"agent_id", agentID,
		"new_event", event,
	)
	ts.LastUpdateTime = time.Now() // Mettre à jour le transfert global aussi

	// Logique pour déterminer si le fichier ou le transfert est complété
	// (Simplifié : vérifier si tous les chunks sont DOWNLOAD_SUCCESSFUL)
	if event == qwrappb.TransferEvent_DOWNLOAD_SUCCESSFUL {
		allFileChunksDone := true
		for _, cs := range fileState.Chunks {
			if cs.Status != qwrappb.TransferEvent_DOWNLOAD_SUCCESSFUL {
				allFileChunksDone = false
				break
			}
		}
		if allFileChunksDone {
			fileState.Status = StatusCompleted
			sm.config.Logger.Info("File download completed", "plan_id", planID, "file_id", fileID)

			allTransferFilesDone := true
			for _, fs := range ts.Files {
				if fs.Status != StatusCompleted {
					allTransferFilesDone = false
					break
				}
			}
			if allTransferFilesDone {
				ts.Status = StatusCompleted
				// ts.CompletionTime = time.Now()
				sm.config.Logger.Info("Entire transfer completed", "plan_id", planID)
			}
		}
	} else if event == qwrappb.TransferEvent_DOWNLOAD_FAILED_AGENT_UNREACHABLE ||
		event == qwrappb.TransferEvent_DOWNLOAD_FAILED_CHECKSUM_MISMATCH ||
		event == qwrappb.TransferEvent_DOWNLOAD_FAILED_TRANSFER_ERROR {
		// (Futur) Logique pour marquer le transfert comme FAILED si trop d'échecs de chunks
		// ou si un chunk critique échoue toutes ses tentatives de réassignation.
	}

	// (Futur) Écrire dans le WAL ici
	return nil
}

func (sm *stateManagerImpl) ReassignChunk(ctx context.Context, planID string, fileID string, chunkID uint64, newAssignments []*qwrappb.ChunkAssignment) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return ErrTransferNotFound
	}
	fileState, fsExists := ts.Files[fileID]
	if !fsExists {
		return ErrFileNotFound
	}
	chunkState, csExists := fileState.Chunks[chunkID]
	if !csExists {
		return ErrChunkNotFound
	}
	if len(newAssignments) == 0 {
		return errors.New("newAssignments cannot be empty for ReassignChunk")
	}

	chunkState.AssignedAgents = []string{}
	for _, assign := range newAssignments {
		chunkState.AssignedAgents = append(chunkState.AssignedAgents, assign.AgentId)
	}
	chunkState.PrimaryAgent = newAssignments[0].AgentId              // Le premier est le nouveau primaire
	chunkState.Status = qwrappb.TransferEvent_TRANSFER_EVENT_UNKNOWN // Réinitialiser le statut client
	chunkState.Attempts = 0                                          // Réinitialiser les tentatives pour la nouvelle assignation
	if newAssignments[0].ChunkInfo != nil {
		// Attention: ne pas écraser le pointeur si ChunkInfo contient des données en mémoire
		// qu'on veut préserver. Ici, ChunkInfo est principalement des métadonnées.
		// Faire une copie si nécessaire ou s'assurer que le scheduler ne modifie que l'agent.
		// Pour une réassignation simple, on assume que le ChunkInfo est le même.
		// Si le scheduler peut changer le ChunkInfo (ex: un autre range pour un chunk "corrigé"),
		// alors chunkState.ChunkInfo = newAssignments[0].ChunkInfo serait nécessaire.
		// Pour l'instant, on suppose que le ChunkInfo d'origine est conservé.
	}

	sm.config.Logger.Info("Chunk reassigned",
		"plan_id", planID,
		"file_id", fileID,
		"chunk_id", chunkID,
		"new_primary_agent", chunkState.PrimaryAgent,
		"num_replicas", len(chunkState.AssignedAgents),
	)
	ts.LastUpdateTime = time.Now()
	// (Futur) Écrire dans le WAL ici
	return nil
}

func (sm *stateManagerImpl) GetChunkState(ctx context.Context, planID string, fileID string, chunkID uint64) (*ChunkState, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ts, exists := sm.transfers[planID]
	if !exists {
		return nil, ErrTransferNotFound
	}
	fileState, fsExists := ts.Files[fileID]
	if !fsExists {
		return nil, ErrFileNotFound
	}
	chunkState, csExists := fileState.Chunks[chunkID]
	if !csExists {
		return nil, ErrChunkNotFound
	}
	// copy := *chunkState // Copie superficielle
	return chunkState, nil
}

// --- Opérations de Persistance et de Cycle de Vie (Placeholders) ---

// func (sm *stateManagerImpl) LoadState(ctx context.Context) error {
// 	sm.mu.Lock()
// 	defer sm.mu.Unlock()
// 	sm.config.Logger.Info("Loading state from persistence...")
// 	// TODO: Implémenter la lecture depuis BadgerDB/BoltDB, rejouer le WAL.
// 	return errors.New("LoadState not implemented")
// }

// func (sm *stateManagerImpl) SnapshotState(ctx context.Context) error {
// 	sm.mu.RLock() // RLock car on lit l'état pour le snapshot
// 	defer sm.mu.RUnlock()
// 	sm.config.Logger.Info("Creating state snapshot...")
// 	// TODO: Implémenter la sérialisation de sm.transfers et l'écriture sur disque.
// 	// S'assurer que le WAL est purgé jusqu'à ce point.
// 	return errors.New("SnapshotState not implemented")
// }

// func (sm *stateManagerImpl) ApplyWALEntry(ctx context.Context, entry []byte) error {
// 	sm.mu.Lock()
// 	defer sm.mu.Unlock()
// 	// TODO: Désérialiser l'entrée et appliquer la modification à sm.transfers.
// 	// Cette méthode serait appelée pendant la recovery.
// 	return errors.New("ApplyWALEntry not implemented")
// }

// func (sm *stateManagerImpl) GarbageCollect(ctx context.Context, olderThan time.Time) (int, error) {
// 	sm.mu.Lock()
// 	defer sm.mu.Unlock()
// 	sm.config.Logger.Info("Running garbage collection for old transfers...", "older_than", olderThan)
// 	count := 0
// 	for planID, ts := range sm.transfers {
// 		if (ts.Status == StatusCompleted || ts.Status == StatusFailed || ts.Status == StatusCancelled) &&
// 			ts.LastUpdateTime.Before(olderThan) { // Ou ts.CompletionTime
// 			delete(sm.transfers, planID)
// 			count++
// 		}
// 	}
// 	sm.config.Logger.Info("Garbage collection finished", "deleted_count", count)
// 	return count, nil
// }
