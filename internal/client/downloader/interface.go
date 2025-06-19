package downloader

import (
	"context"
	"io"
	"time"

	"qwrap/pkg/qwrappb" // Votre package protobuf généré

	"github.com/quic-go/quic-go"
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

// DestinationWriter définit les méthodes requises pour la destination du téléchargement.
type DestinationWriter interface {
	io.WriterAt
	Truncate(size int64) error
	Sync() error // Ajouté pour la vérification du checksum
	io.Closer    // Pour fermer le fichier proprement
}

// Downloader gère le processus de téléchargement distribué d'un fichier.
type Downloader interface {
	// Download démarre le processus de téléchargement pour la TransferRequest donnée.
	// Il écrit le fichier reconstitué dans dest.
	// Le contexte peut être utilisé pour annuler le téléchargement.
	// Renvoie un channel pour suivre la progression et un channel d'erreurs finales.
	Download(ctx context.Context, initialPlanReq *qwrappb.TransferRequest, dest DestinationWriter) (<-chan ProgressInfo, <-chan error)

	// Shutdown tente d'arrêter proprement le downloader dans le délai imparti.
	Shutdown(timeout time.Duration) error
}

// ConnectionManager (interface pour le mock/injection)
type ConnectionManager interface {
	GetOrConnect(ctx context.Context, addr string) (quic.Connection, error)
	Invalidate(ctx context.Context, addr string)
	CloseAll() error
}

// OrchestratorComms (interface pour le mock/injection)
// (Utilisera l'interface existante de orchestratorclient.Comms)
