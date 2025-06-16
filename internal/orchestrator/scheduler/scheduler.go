package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"qwrap/pkg/qwrappb" // Votre package protobuf généré
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	// "github.com/google/uuid" // Pour générer des IDs de plan
)

type schedulerImpl struct {
	mu     sync.RWMutex
	agents map[string]*AgentInfo // Clé: AgentID
	config SchedulerConfig
	// (Futur) Structures pour algorithmes spécifiques, ex: anneau pour consistent hashing
	// (Futur) Prometheus Gauges/Counters
}

// NewScheduler crée une nouvelle instance du Scheduler.
func NewScheduler(config SchedulerConfig) Scheduler {
	config.setDefaults()
	s := &schedulerImpl{
		agents: make(map[string]*AgentInfo),
		config: config,
	}
	// (Futur) Initialiser les métriques Prometheus ici
	return s
}

func (s *schedulerImpl) RegisterAgent(ctx context.Context, req *qwrappb.AgentRegistrationRequest) (*qwrappb.AgentRegistrationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	agentID := req.AgentId
	if agentID == "" {
		// (Optionnel) Générer un ID si l'agent n'en fournit pas.
		// agentID = uuid.NewString()
		s.config.Logger.Warn("Agent registered without an ID, ID generation might be needed or enforce client-side ID", "address", req.AgentAddress)
		return &qwrappb.AgentRegistrationResponse{Success: false, Message: "AgentID is required"}, errors.New("AgentID is required for registration")
	}

	s.config.Logger.Info("Registering or updating agent", "agent_id", agentID, "address", req.AgentAddress)

	agent, exists := s.agents[agentID]
	if !exists {
		agent = &AgentInfo{ID: agentID}
		s.agents[agentID] = agent
		s.config.Logger.Info("New agent registered", "agent_id", agentID)
	} else {
		s.config.Logger.Info("Updating existing agent registration", "agent_id", agentID)
	}

	agent.Address = req.AgentAddress
	agent.Capabilities = req.Capabilities
	agent.MaxConcurrentStreams = req.MaxConcurrentStreams
	if agent.MaxConcurrentStreams <= 0 {
		agent.MaxConcurrentStreams = 10 // Une valeur par défaut raisonnable
		s.config.Logger.Warn("Agent registered with non-positive MaxConcurrentStreams, defaulting", "agent_id", agentID, "default_streams", agent.MaxConcurrentStreams)
	}
	agent.Status = qwrappb.AgentStatus_AGENT_STATUS_IDLE // Devient IDLE après enregistrement/màj
	agent.LastHeartbeatTime = time.Now()
	agent.ConsecutiveFailures = 0
	agent.BlacklistedUntil = time.Time{} // Clear blacklist on re-registration

	return &qwrappb.AgentRegistrationResponse{
		Success:           true,
		Message:           fmt.Sprintf("Agent %s registered/updated successfully", agentID),
		HeartbeatInterval: durationpb.New(s.config.AgentHeartbeatTimeout / 3), // Suggérer un intervalle
	}, nil
}

func (s *schedulerImpl) HandleHeartbeat(ctx context.Context, hb *qwrappb.AgentHeartbeat) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	agentID := hb.AgentId
	agent, exists := s.agents[agentID]
	if !exists {
		s.config.Logger.Warn("Received heartbeat from unknown or deregistered agent", "agent_id", agentID)
		// On pourrait choisir de ré-enregistrer l'agent ici s'il fournit assez d'infos,
		// ou simplement ignorer. Pour l'instant, on ignore.
		return fmt.Errorf("%w: %s, received heartbeat", ErrAgentNotFound, agentID)
	}
	if hb.Timestamp == nil {
		s.config.Logger.Warn("Received heartbeat with nil timestamp", "agent_id", agentID)
		return fmt.Errorf("%w: nil timestamp for agent %s", ErrInvalidAgentHeartbeat, agentID)
	}

	s.config.Logger.Debug("Handling heartbeat", "agent_id", agentID, "status", hb.Status)

	agent.Status = hb.Status
	agent.LastHeartbeatTime = time.Now() // Utiliser l'heure de réception du serveur, hb.Timestamp pour info
	agent.CPULoadPercent = hb.CpuLoadPercent
	agent.MemoryUsageBytes = hb.MemoryUsageBytes
	agent.AvailableBandwidthBPS = hb.AvailableBandwidthBps
	agent.ActiveClientConnections = hb.ActiveClientConnections
	agent.ActiveChunkTransfers = hb.ActiveChunkTransfers

	// Recalculer la charge
	if agent.MaxConcurrentStreams > 0 {
		agent.CurrentLoadPercentage = (float64(agent.ActiveChunkTransfers) / float64(agent.MaxConcurrentStreams)) * 100.0
	} else {
		agent.CurrentLoadPercentage = 100.0 // Considérer comme plein s'il n'a pas de capacité définie
	}

	// Réinitialiser les échecs consécutifs si le heartbeat est OK et qu'il n'est pas blacklisté
	if agent.Status != qwrappb.AgentStatus_AGENT_STATUS_ERROR &&
		agent.Status != qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE &&
		agent.BlacklistedUntil.IsZero() { // Pas activement blacklisté
		agent.ConsecutiveFailures = 0
	}

	// (Futur) Mettre à jour les métriques Prometheus pour cet agent
	return nil
}

func (s *schedulerImpl) CreateTransferPlan(ctx context.Context, planID string, transferReq *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error) {
	s.mu.RLock() // Lecture pour obtenir la liste des agents

	if len(transferReq.FilesToTransfer) == 0 || transferReq.FilesToTransfer[0] == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: no files specified in transfer request", ErrTransferPlanCreationFailed)
	}
	// Pour l'instant, on se concentre sur le premier fichier.
	// Une vraie gestion multi-fichiers nécessiterait une boucle et une agrégation de plans.
	fileMeta := transferReq.FilesToTransfer[0]
	if fileMeta.FileId == "" || fileMeta.TotalSize <= 0 {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: invalid file metadata (ID: %s, Size: %d)", ErrTransferPlanCreationFailed, fileMeta.FileId, fileMeta.TotalSize)
	}

	// Filtrer les agents disponibles et sains
	availableAgents := make([]*AgentInfo, 0, len(s.agents))
	for _, agent := range s.agents {
		if agent.Status == qwrappb.AgentStatus_AGENT_STATUS_IDLE || agent.Status == qwrappb.AgentStatus_AGENT_STATUS_ACTIVE {
			if agent.BlacklistedUntil.IsZero() || time.Now().After(agent.BlacklistedUntil) {
				if agent.CurrentLoadPercentage < maxAgentLoadPercentage { // Ne pas surcharger
					availableAgents = append(availableAgents, agent)
				}
			}
		}
	}
	s.mu.RUnlock() // Libérer le RLock avant les opérations potentiellement longues

	if len(availableAgents) == 0 {
		s.config.Logger.Error("Failed to create transfer plan: no suitable agents available")
		return nil, ErrNoAgentsAvailable
	}

	// Déterminer la taille des chunks (ex: 1MB par défaut, ou configurable)
	// Doit être cohérent avec ce que les agents peuvent gérer.
	// Pour cette version, utilisons une taille fixe.
	// Une meilleure approche serait de la rendre configurable ou basée sur la taille du fichier.
	defaultChunkSize := int64(1 * 1024 * 1024)                           // 1 MiB
	if fileMeta.TotalSize < defaultChunkSize && fileMeta.TotalSize > 0 { // Si fichier plus petit que chunk size
		defaultChunkSize = fileMeta.TotalSize
	}

	numChunks := int(math.Ceil(float64(fileMeta.TotalSize) / float64(defaultChunkSize)))

	s.config.Logger.Info("Creating transfer plan",
		"plan_id", planID,
		"file_id", fileMeta.FileId,
		"total_size", fileMeta.TotalSize,
		"num_chunks", numChunks,
		"num_available_agents", len(availableAgents),
		"algorithm", s.config.DefaultAlgorithm,
	)
	assignments := s.planWithLeastLoaded(fileMeta, defaultChunkSize, numChunks, availableAgents)

	if len(assignments) != numChunks {
		s.config.Logger.Error("Failed to assign all chunks", "assigned_count", len(assignments), "expected_count", numChunks)
		return nil, fmt.Errorf("%w: could not assign all chunks for file %s", ErrTransferPlanCreationFailed, fileMeta.FileId)
	}

	// Créer le checksum global si non fourni (pourrait être coûteux pour l'orchestrateur)
	// Ici, on assume qu'il est fourni dans FileMetadata ou qu'on le laisse vide.
	// Un vrai orchestrateur pourrait avoir accès au fichier ou demander à un agent de le calculer.

	plan := &qwrappb.TransferPlan{
		PlanId:             planID,
		ClientRequestId:    transferReq.RequestId,
		SourceFileMetadata: []*qwrappb.FileMetadata{fileMeta}, // Renvoyer les métadonnées confirmées
		DefaultChunkSize:   defaultChunkSize,
		ChunkAssignments:   assignments,
		Options:            transferReq.Options, // Transmettre les options du client
	}

	return plan, nil
}

// planWithLeastLoaded est un exemple d'algorithme.
// Il essaie d'assigner les chunks aux agents les moins chargés.
// Il ne gère pas encore la réplication des chunks critiques de manière sophistiquée ici.
func (s *schedulerImpl) planWithLeastLoaded(
	fileMeta *qwrappb.FileMetadata,
	chunkSize int64,
	numChunks int,
	agents []*AgentInfo,
) []*qwrappb.ChunkAssignment {
	assignments := make([]*qwrappb.ChunkAssignment, numChunks)

	// Trier les agents par leur charge actuelle (du moins au plus chargé)
	// et secondairement par la bande passante disponible (du plus au moins).
	sort.SliceStable(agents, func(i, j int) bool {
		if agents[i].CurrentLoadPercentage == agents[j].CurrentLoadPercentage {
			return agents[i].AvailableBandwidthBPS > agents[j].AvailableBandwidthBPS // Plus de bande passante en premier
		}
		return agents[i].CurrentLoadPercentage < agents[j].CurrentLoadPercentage
	})

	agentIndex := 0
	for i := 0; i < numChunks; i++ {
		currentOffset := int64(i) * chunkSize
		actualChunkSize := chunkSize
		if currentOffset+chunkSize > fileMeta.TotalSize {
			actualChunkSize = fileMeta.TotalSize - currentOffset
		}

		selectedAgent := agents[agentIndex%len(agents)] // Round-robin simple sur la liste triée

		// (Simplification) Calculer un checksum de chunk factice ou le laisser vide.
		// Un vrai système aurait besoin que l'agent source (ou l'orchestrateur) calcule cela.
		// Pour l'instant, on ne met pas de checksum par chunk pour ne pas surcharger le proto.
		// Si `transferReq.Options.VerifyChunkChecksums` est vrai, l'orchestrateur
		// DOIT s'assurer que les checksums de chunks sont présents dans ChunkInfo.

		chunkInfo := &qwrappb.ChunkInfo{
			FileId:  fileMeta.FileId,
			ChunkId: uint64(i),
			Range: &qwrappb.ByteRange{
				Offset: currentOffset,
				Length: actualChunkSize,
			},
			// ChecksumAlgorithm: qwrappb.ChecksumAlgorithm_XXHASH64, // Exemple
			// ChecksumValue:     calculateXxhash64ForRange(...), // Nécessiterait accès aux données
		}

		assignments[i] = &qwrappb.ChunkAssignment{
			ChunkInfo:    chunkInfo,
			AgentId:      selectedAgent.ID,
			AgentAddress: selectedAgent.Address,
		}

		// Simuler une augmentation de la charge pour l'équilibrage dans cette boucle
		// (dans un vrai système, la charge de l'agent serait mise à jour par les heartbeats,
		// ou le scheduler pourrait maintenir une charge "prévue")
		selectedAgent.ActiveChunkTransfers++ // Attention: ceci modifie l'état partagé.
		// Pour la planification, il vaut mieux travailler sur des copies ou des charges prévues.
		// Pour cette version simplifiée, on l'omet pour éviter la complexité de la gestion
		// de la charge "prévue" qui doit être décrémentée une fois le transfert réel fini.
		// On se fie au tri initial et au round-robin.

		agentIndex++
	}
	return assignments
}

func (s *schedulerImpl) ReportAgentFailure(ctx context.Context, agentID string, chunkInfo *qwrappb.ChunkInfo, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	agent, exists := s.agents[agentID]
	if !exists {
		s.config.Logger.Warn("Reported failure for unknown agent", "agent_id", agentID)
		return ErrAgentNotFound
	}

	s.config.Logger.Warn("Agent failure reported",
		"agent_id", agentID,
		"file_id", chunkInfo.FileId,
		"chunk_id", chunkInfo.ChunkId,
		"reason", reason,
	)

	agent.ConsecutiveFailures++

	// Logique de Circuit Breaker / Blacklist simple
	// (Exemple : après 3 échecs consécutifs, blacklist pour 1 minute)
	if agent.ConsecutiveFailures >= 3 {
		blacklistDuration := 1 * time.Minute
		agent.BlacklistedUntil = time.Now().Add(blacklistDuration)
		agent.Status = qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE // Le marquer indisponible
		s.config.Logger.Warn("Agent blacklisted due to repeated failures",
			"agent_id", agentID,
			"failures", agent.ConsecutiveFailures,
			"blacklist_duration", blacklistDuration,
		)
		// (Futur) Déclencher une replanification pour les chunks affectés à cet agent.
	}
	return nil
}

func (s *schedulerImpl) GetAgentInfo(agentID string) (*AgentInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agent, exists := s.agents[agentID]
	if !exists {
		return nil, ErrAgentNotFound
	}
	// Retourner une copie pour éviter les modifications externes non contrôlées
	agentCopy := *agent
	return &agentCopy, nil
}

func (s *schedulerImpl) ListAvailableAgents() []*AgentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	available := make([]*AgentInfo, 0, len(s.agents))
	now := time.Now()
	for _, agent := range s.agents {
		if (agent.Status == qwrappb.AgentStatus_AGENT_STATUS_IDLE || agent.Status == qwrappb.AgentStatus_AGENT_STATUS_ACTIVE) &&
			(agent.BlacklistedUntil.IsZero() || now.After(agent.BlacklistedUntil)) &&
			now.Sub(agent.LastHeartbeatTime) <= s.config.AgentHeartbeatTimeout { // Vérifier aussi le timeout du heartbeat
			agentCopy := *agent // Retourner des copies
			available = append(available, &agentCopy)
		}
	}
	return available
}

func (s *schedulerImpl) StartBackgroundTasks(ctx context.Context) {
	s.config.Logger.Info("Starting scheduler background tasks...")
	go s.agentCleanupLoop(ctx)
	// (Futur) go s.metricsUpdateLoop(ctx)
}

func (s *schedulerImpl) agentCleanupLoop(ctx context.Context) {
	// Vérifier périodiquement les agents inactifs/morts.
	ticker := time.NewTicker(s.config.AgentHeartbeatTimeout) // Vérifier à la fréquence du timeout
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.config.Logger.Info("Agent cleanup loop stopping due to context cancellation.")
			return
		case <-ticker.C:
			s.cleanupDeadAgents()
		}
	}
}

func (s *schedulerImpl) cleanupDeadAgents() {
	s.mu.Lock() // Lock complet car on peut supprimer des éléments
	defer s.mu.Unlock()

	now := time.Now()
	agentsToEvict := []string{}

	for agentID, agent := range s.agents {
		// Si un agent n'a pas envoyé de heartbeat depuis un certain temps (ex: 2 * timeout)
		// OU si son statut est en erreur et qu'il n'a pas récupéré.
		if now.Sub(agent.LastHeartbeatTime) > s.config.AgentHeartbeatTimeout*2 {
			if agent.Status != qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE && agent.Status != qwrappb.AgentStatus_AGENT_STATUS_ERROR {
				s.config.Logger.Warn("Agent missed heartbeats, marking as unavailable", "agent_id", agentID, "last_heartbeat", agent.LastHeartbeatTime)
				agent.Status = qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE // Marquer comme indisponible d'abord
				agent.ConsecutiveFailures = 3                               // Forcer le circuit breaker si pas déjà actif
				if agent.BlacklistedUntil.IsZero() || now.After(agent.BlacklistedUntil) {
					agent.BlacklistedUntil = now.Add(1 * time.Minute) // Blacklist courte
				}
			}
			// Si l'agent est silencieux depuis très longtemps, le supprimer complètement.
			if now.Sub(agent.LastHeartbeatTime) > s.config.AgentEvictionGracePeriod {
				s.config.Logger.Warn("Agent silent for too long, queueing for eviction", "agent_id", agentID, "last_heartbeat", agent.LastHeartbeatTime)
				agentsToEvict = append(agentsToEvict, agentID)
			}

		} else if (agent.Status == qwrappb.AgentStatus_AGENT_STATUS_ERROR || agent.Status == qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE) &&
			(agent.BlacklistedUntil.IsZero() || now.After(agent.BlacklistedUntil)) && // N'est plus explicitement blacklisté
			agent.ConsecutiveFailures > 0 { // Mais a toujours des échecs

			// Si un agent est en erreur/indisponible mais que son timeout de blacklist est passé,
			// on pourrait lui donner une chance de se ré-enregistrer ou de se rétablir.
			// S'il continue à ne pas envoyer de heartbeats OK, il sera attrapé par la logique ci-dessus.
			// Pour l'instant, on ne fait rien de plus ici que ce que fait déjà HandleHeartbeat.
		}
	}

	if len(agentsToEvict) > 0 {
		s.config.Logger.Info("Evicting dead/silent agents", "count", len(agentsToEvict), "agent_ids", agentsToEvict)
		for _, agentID := range agentsToEvict {
			delete(s.agents, agentID)
		}
	}
}

// MaybeReplanChunk tente de replanifier un chunk après un échec rapporté.
func (s *schedulerImpl) MaybeReplanChunk(ctx context.Context, planID string, failedChunkInfo *qwrappb.ChunkInfo, failedAgentID string) (bool, []*qwrappb.ChunkAssignment, error) {
	s.mu.RLock()         // Utiliser RLock d'abord pour lire l'état des agents
	defer s.mu.RUnlock() // S'assurer qu'il est libéré même si on retourne tôt

	if failedChunkInfo == nil {
		return false, nil, errors.New("failedChunkInfo cannot be nil for replanning")
	}

	agent, agentExists := s.agents[failedAgentID]
	// Même si l'agent n'existe plus (a été évincé), on pourrait vouloir replanifier le chunk.
	// La décision de replanifier peut se baser sur le fait qu'un échec a eu lieu.

	s.config.Logger.Info("Considering replan for chunk",
		"plan_id", planID, "chunk_id", failedChunkInfo.ChunkId, "file_id", failedChunkInfo.FileId,
		"failed_agent_id", failedAgentID, "agent_exists_in_scheduler", agentExists)

	// Décider si une replanification est nécessaire.
	// Par exemple, si l'agent est maintenant blacklisté, en erreur, ou si on veut simplement essayer un autre agent.
	// Pour cette version, on replanifie si l'agent est défaillant ou si on le demande explicitement.
	// Une logique plus fine pourrait considérer le type d'erreur du chunk.
	needsReplanDecision := false
	if agentExists {
		if agent.Status == qwrappb.AgentStatus_AGENT_STATUS_UNAVAILABLE ||
			agent.Status == qwrappb.AgentStatus_AGENT_STATUS_ERROR ||
			(!agent.BlacklistedUntil.IsZero() && time.Now().Before(agent.BlacklistedUntil)) {
			needsReplanDecision = true
			s.config.Logger.Info("Replan needed due to failed agent status or blacklist", "agent_id", failedAgentID, "status", agent.Status, "blacklisted_until", agent.BlacklistedUntil)
		}
	} else {
		// Si l'agent n'existe plus dans le scheduler, il faut absolument replanifier si le chunk n'est pas complet.
		needsReplanDecision = true
		s.config.Logger.Info("Replan needed because failed agent no longer known to scheduler", "agent_id", failedAgentID)
	}

	// On pourrait avoir une politique pour toujours essayer de replanifier après N échecs sur un agent, même s'il semble OK.
	// Pour l'instant, on se base sur l'état de l'agent. Si aucune raison ci-dessus, on pourrait décider de ne pas replanifier.
	// Toutefois, si cette fonction est appelée, c'est généralement parce qu'un échec a eu lieu.
	// On va forcer la replanification si le chunk a échoué, pour au moins essayer un autre agent.
	needsReplanDecision = true

	if !needsReplanDecision {
		s.config.Logger.Info("Replan not deemed necessary for chunk based on current agent states", "chunk_id", failedChunkInfo.ChunkId)
		return false, nil, nil
	}

	// Filtrer les agents disponibles et sains, en excluant l'agent défaillant.
	availableGoodAgents := make([]*AgentInfo, 0, len(s.agents))
	now := time.Now()
	for _, ag := range s.agents {
		if ag.ID == failedAgentID { // Exclure l'agent qui vient d'échouer pour cette affectation
			continue
		}
		if (ag.Status == qwrappb.AgentStatus_AGENT_STATUS_IDLE || ag.Status == qwrappb.AgentStatus_AGENT_STATUS_ACTIVE) &&
			(ag.BlacklistedUntil.IsZero() || now.After(ag.BlacklistedUntil)) &&
			now.Sub(ag.LastHeartbeatTime) <= s.config.AgentHeartbeatTimeout && // Encore en vie
			ag.CurrentLoadPercentage < maxAgentLoadPercentage { // Pas surchargé
			availableGoodAgents = append(availableGoodAgents, ag)
		}
	}

	if len(availableGoodAgents) == 0 {
		s.config.Logger.Warn("No alternative suitable agents available for replanning chunk", "chunk_id", failedChunkInfo.ChunkId, "failed_agent_id", failedAgentID)
		return true, nil, ErrNoAgentsAvailable // Replan était nécessaire, mais impossible
	}

	// Appliquer la stratégie de sélection (ex: le moins chargé parmi les alternatifs)
	sort.SliceStable(availableGoodAgents, func(i, j int) bool {
		if availableGoodAgents[i].CurrentLoadPercentage == availableGoodAgents[j].CurrentLoadPercentage {
			return availableGoodAgents[i].AvailableBandwidthBPS > availableGoodAgents[j].AvailableBandwidthBPS
		}
		return availableGoodAgents[i].CurrentLoadPercentage < availableGoodAgents[j].CurrentLoadPercentage
	})

	newPrimaryAgent := availableGoodAgents[0]

	// (Futur) Gérer la réplication : si le chunk est critique ou si la politique l'exige,
	// sélectionner plusieurs agents ici. Pour l'instant, une seule nouvelle affectation.
	// replicationFactor := 1
	// if isChunkCritical(failedChunkInfo) { replicationFactor = criticalChunkReplicationFactor }
	// newAssignments := make([]*qwrappb.ChunkAssignment, 0, replicationFactor)
	// for i := 0; i < replicationFactor && i < len(availableGoodAgents); i++ {
	//    selectedAg := availableGoodAgents[i]
	//    newAssignments = append(newAssignments, &qwrappb.ChunkAssignment{
	//        ChunkInfo:    failedChunkInfo, // Utiliser les mêmes ChunkInfo (ID, Range, FileId)
	//        AgentId:      selectedAg.ID,
	//        AgentAddress: selectedAg.Address,
	//    })
	// }

	newAssignments := []*qwrappb.ChunkAssignment{{
		ChunkInfo:    failedChunkInfo, // Important: le ChunkInfo (ID, Range, etc.) reste le même
		AgentId:      newPrimaryAgent.ID,
		AgentAddress: newPrimaryAgent.Address,
	}}

	s.config.Logger.Info("Chunk replanned successfully",
		"plan_id", planID, "chunk_id", failedChunkInfo.ChunkId,
		"old_agent_id", failedAgentID, "new_primary_agent_id", newPrimaryAgent.ID)

	// Note: Ce `Scheduler` ne met PAS à jour la charge prévisionnelle du nouvel agent.
	// Cette charge sera mise à jour par les heartbeats ou quand le StateManager
	// confirme que le chunk est activement en cours de transfert par ce nouvel agent.

	return true, newAssignments, nil
}
