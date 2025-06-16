package orchestratorclient

import (
	"context"
	"qwrap/pkg/qwrappb" // Adaptez le chemin si nécessaire
)

// Comms est l'interface pour la communication du client qwrap avec l'Orchestrateur.
type Comms interface {
	// Connect établit la connexion sous-jacente (ex: QUIC) avec l'orchestrateur.
	// Doit être appelé avant les autres méthodes.
	Connect(ctx context.Context, orchestratorAddress string) error

	// ReportChunkTransferStatus envoie le statut d'un ou plusieurs chunks.
	// Bloquant jusqu'à l'envoi (avec timeout géré par ctx).
	ReportTransferStatus(ctx context.Context, report *qwrappb.ClientReport) error

	// RegisterForPlanUpdates ouvre un flux dédié pour recevoir les UpdatedTransferPlan.
	// Cette méthode doit lancer une goroutine pour écouter sur ce flux et pousser les
	// plans reçus dans le `updatedPlanChan`.
	// Elle doit retourner immédiatement ou après l'établissement du flux.
	// La goroutine interne doit se terminer lorsque le `ctx` est annulé.
	ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error

	// RequestInitialPlan demande le plan de transfert initial.
	RequestInitialPlan(ctx context.Context, req *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error)

	// Close ferme la connexion avec l'orchestrateur.
	Close() error
}
