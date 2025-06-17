package statemanager

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré
	// "github.com/google/uuid" // Pour générer des IDs uniques
)

var (
	ErrTransferNotFound      = errors.New("transfer not found")
	ErrFileNotFound          = errors.New("file not found in transfer")
	ErrChunkNotFound         = errors.New("chunk not found in file")
	ErrAssignmentNotFound    = errors.New("chunk assignment not found")
	ErrInvalidStateOperation = errors.New("invalid operation for current state")
)

// TransferStatus définit l'état global d'un transfert.
type TransferStatus string

const (
	StatusPending    TransferStatus = "PENDING"     // En attente de planification
	StatusPlanning   TransferStatus = "PLANNING"    // Le scheduler travaille dessus
	StatusInProgress TransferStatus = "IN_PROGRESS" // Chunks en cours de téléchargement
	StatusPaused     TransferStatus = "PAUSED"      // (Futur)
	StatusCompleted  TransferStatus = "COMPLETED"   // Tous les chunks téléchargés et vérifiés
	StatusFailed     TransferStatus = "FAILED"      // Échec irrécupérable du transfert
	StatusCancelled  TransferStatus = "CANCELLED"   // Annulé par l'utilisateur ou le système
	StatusArchived   TransferStatus = "ARCHIVED"    // (Futur) Gardé pour l'historique
)

// ChunkState contient l'état détaillé d'un chunk spécifique.
type ChunkState struct {
	ChunkInfo      *qwrappb.ChunkInfo
	AssignedAgents []string              // IDs des agents auxquels ce chunk est actuellement assigné (pour réplication)
	PrimaryAgent   string                // Agent principal désigné pour ce chunk
	Status         qwrappb.TransferEvent // Statut du point de vue du client (le plus récent)
	Attempts       int
	LastReportTime time.Time
	// (Futur) DownloadSpeedBPS float64
	// (Futur) DownloadedByClient string // ID du client qui a téléchargé (si multi-client)
}

// FileState contient l'état d'un fichier au sein d'un transfert.
type FileState struct {
	Metadata *qwrappb.FileMetadata
	Chunks   map[uint64]*ChunkState // Clé: ChunkID
	Status   TransferStatus         // Peut être différent du statut global du transfert
	// (Futur) ProgressPercent float64
}

// TransferState contient l'état complet d'une opération de transfert.
type TransferState struct {
	PlanID                string // Corresponds au PlanID du qwrappb.TransferPlan
	ClientRequestID       string
	Status                TransferStatus
	Files                 map[string]*FileState // Clé: FileID
	CreationTime          time.Time
	LastUpdateTime        time.Time
	AssignedSchedulerPlan *qwrappb.TransferPlan // Le plan généré par le scheduler
	// (Futur) ClientID string
	// (Futur) ErrorMessage string // Si Status == FAILED
	// (Futur) CompletionTime time.Time
	// (Futur) TotalBytesTransferred int64
}

// StateManagerConfig contient la configuration du StateManager.
type StateManagerConfig struct {
	Logger   *slog.Logger
	DBPath   string // Chemin vers le fichier de la base de données BoltDB
	// (Futur) SnapshotInterval time.Duration
	// (Futur) GCInterval time.Duration (pour nettoyer les vieux transferts)
}

func (c *StateManagerConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default().With("component", "statemanager")
	}
}

// StateManager est responsable de la gestion de l'état de tous les transferts
// et des entités associées (fichiers, chunks, agents).
type StateManager interface {
	// --- Gestion des Transferts ---

	// CreateTransfer initialise un nouvel état de transfert basé sur une requête client.
	// Retourne l'ID du plan généré pour ce transfert.
	CreateTransfer(ctx context.Context, clientReq *qwrappb.TransferRequest) (string, *TransferState, error)

	// GetTransfer récupère l'état d'un transfert par son PlanID.
	GetTransfer(ctx context.Context, planID string) (*TransferState, error)

	// UpdateTransferStatus met à jour le statut global d'un transfert.
	UpdateTransferStatus(ctx context.Context, planID string, status TransferStatus, errorMessage ...string) error

	// LinkSchedulerPlan associe un plan généré par le scheduler à un état de transfert.
	// Cette opération peuple les FileStates et ChunkStates.
	LinkSchedulerPlan(ctx context.Context, planID string, schedulerPlan *qwrappb.TransferPlan) error

	// --- Gestion des Chunks ---

	// UpdateChunkStatus met à jour l'état d'un chunk spécifique au sein d'un transfert.
	// Cela serait généralement appelé suite à un ClientReport.
	UpdateChunkStatus(ctx context.Context, planID string, fileID string, chunkID uint64, agentID string, event qwrappb.TransferEvent, details string) error

	// ReassignChunk met à jour l'affectation d'un chunk à un (ou plusieurs) nouvel agent(s).
	// Utilisé par le scheduler en cas de rééquilibrage ou de panne.
	ReassignChunk(ctx context.Context, planID string, fileID string, chunkID uint64, newAssignments []*qwrappb.ChunkAssignment) error

	// GetChunkState récupère l'état d'un chunk.
	GetChunkState(ctx context.Context, planID string, fileID string, chunkID uint64) (*ChunkState, error)

	// --- Opérations de Persistance et de Cycle de Vie (Interfaces pour le futur) ---

	// LoadState (Futur) Charge l'état depuis la persistance au démarrage.
	// LoadState(ctx context.Context) error

	// SnapshotState (Futur) Crée un snapshot de l'état actuel.
	// SnapshotState(ctx context.Context) error

	// ApplyWALEntry (Futur) Applique une entrée du Write-Ahead Log.
	// ApplyWALEntry(ctx context.Context, entry []byte) error

	// GarbageCollect (Futur) Nettoie les anciens transferts terminés.
	// GarbageCollect(ctx context.Context, olderThan time.Time) (int, error)

	// ListTransfers (Futur) Liste les transferts (avec filtres optionnels)
	// ListTransfers(ctx context.Context, statusFilter TransferStatus, limit, offset int) ([]*TransferState, error)

	// Close ferme les ressources du StateManager, comme la connexion à la base de données.
	Close() error
}
