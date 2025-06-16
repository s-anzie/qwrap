package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré
	// Pour les métriques Prometheus (optionnel pour la première passe, mais bon à garder en tête)
	// "github.com/prometheus/client_golang/prometheus"
	// "github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultAgentHeartbeatTimeout    = 30 * time.Second
	defaultAgentEvictionGracePeriod = 5 * time.Minute // Temps avant de supprimer un agent totalement silencieux
	maxAgentLoadPercentage          = 90.0            // Pourcentage de charge max avant de considérer un agent comme surchargé
	minAgentCapacityForCritical     = 2               // Nombre min d'agents pour répliquer un chunk critique
	criticalChunkReplicationFactor  = 2               // Nombre de répliques pour les chunks critiques
)

var (
	ErrNoAgentsAvailable          = errors.New("no suitable agents available")
	ErrAgentNotFound              = errors.New("agent not found")
	ErrInvalidAgentHeartbeat      = errors.New("invalid agent heartbeat data")
	ErrTransferPlanCreationFailed = errors.New("transfer plan creation failed")
)

// AgentInfo contient l'état et les métriques d'un agent connu par le scheduler.
type AgentInfo struct {
	ID                   string
	Address              string // IP:Port pour les clients QUIC
	Capabilities         []string
	MaxConcurrentStreams int64 // Capacité déclarée par l'agent
	Status               qwrappb.AgentStatus
	LastHeartbeatTime    time.Time

	// Métriques de performance (mises à jour par les heartbeats)
	CPULoadPercent          float32
	MemoryUsageBytes        int64
	AvailableBandwidthBPS   int64 // Bande passante sortante disponible estimée
	ActiveClientConnections int32
	ActiveChunkTransfers    int32 // Nombre de chunks que l'agent sert actuellement

	// Métriques calculées par le scheduler
	CurrentLoadPercentage float64   // (ActiveChunkTransfers / MaxConcurrentStreams) * 100
	Score                 float64   // Score composite pour la sélection
	ConsecutiveFailures   int       // Pour le circuit breaker
	BlacklistedUntil      time.Time // Pour la blacklist temporaire

	// (Futur) LatencyToClients map[string]time.Duration // Latence moyenne vers des groupes de clients
	// (Futur) GeographicLocation string
}

// PlanificationAlgorithmType définit le type d'algorithme de planification à utiliser.
type PlanificationAlgorithmType string

const (
	AlgoLeastLoaded        PlanificationAlgorithmType = "least_loaded"
	AlgoRoundRobinWeighted PlanificationAlgorithmType = "round_robin_weighted"
	AlgoGeoConsistentHash  PlanificationAlgorithmType = "geo_consistent_hash"
	// ... autres algorithmes
)

// SchedulerConfig contient la configuration du Scheduler.
type SchedulerConfig struct {
	DefaultAlgorithm         PlanificationAlgorithmType
	AgentHeartbeatTimeout    time.Duration
	AgentEvictionGracePeriod time.Duration
	Logger                   *slog.Logger
	// (Futur) Prometheus Registry
}

func (c *SchedulerConfig) setDefaults() {
	if c.DefaultAlgorithm == "" {
		c.DefaultAlgorithm = AlgoLeastLoaded
	}
	if c.AgentHeartbeatTimeout <= 0 {
		c.AgentHeartbeatTimeout = defaultAgentHeartbeatTimeout
	}
	if c.AgentEvictionGracePeriod <= 0 {
		c.AgentEvictionGracePeriod = defaultAgentEvictionGracePeriod
	}
	if c.Logger == nil {
		c.Logger = slog.Default().With("component", "scheduler")
	}
}

// Scheduler est responsable de l'enregistrement des agents, du suivi de leur état,
// et de l'attribution des tâches de transfert de chunks.
type Scheduler interface {
	// RegisterAgent enregistre un nouvel agent ou met à jour les informations d'un agent existant.
	RegisterAgent(ctx context.Context, req *qwrappb.AgentRegistrationRequest) (*qwrappb.AgentRegistrationResponse, error)
	// HandleHeartbeat traite un heartbeat reçu d'un agent.
	HandleHeartbeat(ctx context.Context, hb *qwrappb.AgentHeartbeat) error
	// CreateTransferPlan génère un plan de transfert pour une requête donnée.
	// Il sélectionne les agents appropriés pour chaque chunk.
	CreateTransferPlan(ctx context.Context, planID string, transferReq *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error)
	// ReportAgentFailure est appelé par d'autres composants (ex: le client via l'orchestrateur principal)
	// pour signaler qu'un agent a échoué à servir un chunk.
	ReportAgentFailure(ctx context.Context, agentID string, chunkInfo *qwrappb.ChunkInfo, reason string) error
	// MaybeReplanChunk tente de replanifier un chunk après un échec rapporté sur un agent spécifique.
	MaybeReplanChunk(ctx context.Context, planID string, failedChunkInfo *qwrappb.ChunkInfo, failedAgentID string) (needsReplan bool, newAssignments []*qwrappb.ChunkAssignment, err error)
	// GetAgentInfo retourne les informations d'un agent spécifique.
	GetAgentInfo(agentID string) (*AgentInfo, error)
	// ListAvailableAgents retourne une liste des agents considérés comme disponibles et sains.
	ListAvailableAgents() []*AgentInfo
	// StartBackgroundTasks démarre les tâches de fond du scheduler (ex: nettoyage des agents morts).
	StartBackgroundTasks(ctx context.Context)
}
