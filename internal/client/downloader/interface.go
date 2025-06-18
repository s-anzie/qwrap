package downloader

import (
	"context"
	"io"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré

	"github.com/quic-go/quic-go"
	// connectionmanager "qwrap/internal/client/connection_manager" // Si l'interface est ici
)

// ProgressInfo contient des informations sur l'état d'avancement du téléchargement.
type ProgressInfo struct {
	TotalSizeBytes      int64
	DownloadedSizeBytes int64
	TotalChunks         int
	CompletedChunks     int
	FailedChunks        int
	ActiveDownloads     int
	// (Futur) EstimatedTimeRemaining time.Duration
	// (Futur) CurrentSpeedBps float64
}

// BackpressureStrategy définit la méthode pour gérer la pression entre les téléchargements rapides et l'écriture disque lente.
type BackpressureStrategy string

const (
	// TokenBackpressure utilise un pool de jetons pour limiter le nombre de chunks en vol (téléchargés mais pas encore écrits).
	TokenBackpressure BackpressureStrategy = "token"
	// DiskQueueBackpressure utilise des fichiers temporaires sur disque pour mettre en file d'attente les chunks terminés, minimisant l'utilisation de la mémoire.
	DiskQueueBackpressure BackpressureStrategy = "disk"
)

// ChunkDownloadResult contient le résultat du téléchargement d'un chunk spécifique.
type ChunkDownloadResult struct {
	ChunkInfo *qwrappb.ChunkInfo // Utiliser un pointeur pour éviter la copie du mutex interne
	Data      []byte // Données du chunk si réussi
	Err       error  // Erreur si le téléchargement a échoué
	AgentID   string // Agent qui a fourni le chunk (ou tenté de le faire)
	Attempt   int    // Numéro de la tentative
}

// DestinationWriter définit les méthodes requises pour la destination du téléchargement.
// Il permet d'écrire à des offsets spécifiques et de tronquer, ce qui est nécessaire
// pour la pré-allocation et l'écriture de chunks en parallèle.
type DestinationWriter interface {
	io.WriterAt
	Truncate(size int64) error
}

// Downloader gère le processus de téléchargement distribué d'un fichier.
type Downloader interface {
	// Download démarre le processus de téléchargement pour le TransferPlan donné.
	// Il écrit le fichier reconstitué dans dest.
	// Le contexte peut être utilisé pour annuler le téléchargement.
	// Renvoie un channel pour suivre la progression et un channel d'erreurs finales.
	Download(ctx context.Context, initialPlanReq *qwrappb.TransferRequest, dest DestinationWriter) (<-chan ProgressInfo, <-chan error)

	// Shutdown tente d'arrêter proprement le downloader dans le délai imparti.
	Shutdown(timeout time.Duration) error
}

// Interface pour le ConnectionManager, pour faciliter le mocking.
// Si vous définissez l'interface ConnectionManager dans son propre package, importez-la.
// Sinon, définissez-la ici ou dans un fichier partagé `interfaces.go`.
type ConnectionManager interface {
	GetOrConnect(ctx context.Context, addr string) (quic.Connection, error)
	Invalidate(ctx context.Context, addr string)
	// CloseAll n'est pas directement utilisé par le downloader mais par le client principal
}

// Interface pour communiquer avec l'Orchestrateur (pour rapporter les échecs et demander des réassignations)
// (Simplifié pour l'instant, pourrait devenir plus complexe)
type OrchestratorReporter interface {
	ReportChunkFailure(ctx context.Context, planID string, chunkInfo *qwrappb.ChunkInfo, agentID string, reason string) error
	// (Futur) RequestChunkReassignment(ctx context.Context, planID string, chunkInfo *qwrappb.ChunkInfo) (*qwrappb.ChunkAssignment, error)
}
