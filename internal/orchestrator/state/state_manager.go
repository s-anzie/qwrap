package statemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré

	"github.com/google/uuid" // Pour générer des IDs uniques
	"go.etcd.io/bbolt"
)

type stateManagerImpl struct {
	mu        sync.RWMutex
	transfers map[string]*TransferState // Clé: PlanID
	config    StateManagerConfig
	db        *bbolt.DB // Handle de la base de données BoltDB
	// (Futur) wal *os.File
}

// NewStateManager crée une nouvelle instance de StateManager.
func NewStateManager(config StateManagerConfig) (StateManager, error) {
	config.setDefaults()

	if config.DBPath == "" {
		return nil, errors.New("DBPath must be specified in StateManagerConfig")
	}

	db, err := bbolt.Open(config.DBPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt database at %s: %w", config.DBPath, err)
	}

	sm := &stateManagerImpl{
		transfers: make(map[string]*TransferState),
		config:    config,
		db:        db,
	}

	// Ensure the 'transfers' bucket exists
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("transfers"))
		return err
	})
	if err != nil {
		db.Close() // Close the db if we can't create the bucket
		return nil, fmt.Errorf("failed to create 'transfers' bucket: %w", err)
	}

	// Load existing state from the database
	if err := sm.loadStateFromDB(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load existing state from db: %w", err)
	}

	config.Logger.Info("StateManager initialized with BoltDB persistence", "db_path", config.DBPath)

	return sm, nil
}

func (sm *stateManagerImpl) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.db != nil {
		sm.config.Logger.Info("Closing BoltDB database.")
		return sm.db.Close()
	}
	return nil
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

	// Persist the new transfer state to the database before adding to memory
	jsonData, err := json.Marshal(ts)
	if err != nil {
		return "", nil, fmt.Errorf("failed to serialize transfer state for plan %s: %w", planID, err)
	}

	err = sm.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("transfers"))
		// This check is for safety, bucket should exist from NewStateManager
		if b == nil {
			return fmt.Errorf("transfers bucket not found")
		}
		return b.Put([]byte(planID), jsonData)
	})

	if err != nil {
		return "", nil, fmt.Errorf("failed to persist transfer state for plan %s: %w", planID, err)
	}

	sm.transfers[planID] = ts
	sm.config.Logger.Info("New transfer state created and persisted", "plan_id", planID, "client_request_id", clientReq.RequestId)

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

	// Persist the updated state
	if err := sm.saveTransferState(ts); err != nil {
		// If persistence fails, should we roll back the in-memory change?
		// For now, we log the error and the in-memory state might be ahead of persisted state.
		ts.Status = oldStatus // Rollback in-memory change
		return fmt.Errorf("failed to persist status update for plan %s: %w", planID, err)
	}

	sm.config.Logger.Info("Transfer status updated and persisted",
		"plan_id", planID,
		"old_status", oldStatus,
		"new_status", status,
	)
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

	// Persist the updated state
	if err := sm.saveTransferState(ts); err != nil {
		// This is a critical failure. The in-memory state is now significantly different from the persisted state.
		// A real-world system might need a more robust recovery or rollback mechanism.
		// For now, we will log the error and return it, leaving the system in a potentially inconsistent state.
		return fmt.Errorf("CRITICAL: failed to persist linked scheduler plan for plan %s: %w", planID, err)
	}

	sm.config.Logger.Info("Scheduler plan linked and persisted to transfer", "plan_id", planID, "num_assignments", len(schedulerPlan.ChunkAssignments))
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

	// Persist the changes
	if err := sm.saveTransferState(ts); err != nil {
		// Log the error, but the in-memory state has already changed. This could lead to inconsistency.
		return fmt.Errorf("failed to persist chunk status update for plan %s: %w", planID, err)
	}

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

	// Persist the changes
	if err := sm.saveTransferState(ts); err != nil {
		return fmt.Errorf("failed to persist chunk reassignment for plan %s: %w", planID, err)
	}

	return nil
}

// saveTransferState serializes and persists a single TransferState to the database.
// This method assumes the caller has already acquired the necessary locks.
func (sm *stateManagerImpl) saveTransferState(ts *TransferState) error {
	if ts == nil || ts.PlanID == "" {
		return errors.New("cannot save nil or empty planID transfer state")
	}

	jsonData, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("failed to serialize transfer state for plan %s: %w", ts.PlanID, err)
	}

	return sm.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("transfers"))
		return b.Put([]byte(ts.PlanID), jsonData)
	})
}

// loadStateFromDB iterates over all transfers in the DB and loads them into memory.
func (sm *stateManagerImpl) loadStateFromDB() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("transfers"))
		if b == nil {
			// Bucket not existing is not an error, just means no transfers yet.
			sm.config.Logger.Info("'transfers' bucket not found, assuming new database.")
			return nil
		}

		count := 0
		err := b.ForEach(func(k, v []byte) error {
			var ts TransferState
			if err := json.Unmarshal(v, &ts); err != nil {
				// Log the corrupted entry but continue loading others
				sm.config.Logger.Error("Failed to deserialize transfer state from DB", "plan_id", string(k), "error", err)
				return nil // Continue to next item
			}
			sm.transfers[ts.PlanID] = &ts
			count++
			return nil
		})

		if err != nil {
			return fmt.Errorf("error while iterating over transfers bucket: %w", err)
		}

		sm.config.Logger.Info("Successfully loaded state for transfers from DB", "count", count)
		return nil
	})
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
