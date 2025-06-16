package orchestratorclient

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"qwrap/pkg/qwrappb"
)

type noopComms struct {
	logger    *slog.Logger
	connected bool
}

// NewNoOpComms crée une implémentation factice de Comms.
func NewNoOpComms(logger *slog.Logger) Comms {
	if logger == nil {
		logger = slog.Default()
	}
	return &noopComms{logger: logger.With("component", "noop_orchestrator_comms")}
}

func (n *noopComms) Connect(ctx context.Context, orchestratorAddress string) error {
	n.logger.Info("NO-OP: Connect called", "address", orchestratorAddress)
	n.connected = true
	return nil
}

func (n *noopComms) ReportTransferStatus(ctx context.Context, report *qwrappb.ClientReport) error {
	if !n.connected {
		return errors.New("noopComms: not connected")
	}
	n.logger.Info("NO-OP: ReportTransferStatus called", "plan_id", report.PlanId, "num_statuses", len(report.ChunkStatuses))
	// Simuler un petit délai
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (n *noopComms) ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error {
	if !n.connected {
		return errors.New("noopComms: not connected")
	}
	n.logger.Info("NO-OP: RegisterForPlanUpdates called, will do nothing.", "plan_id", planID)
	// Pour éviter que le select dans le downloader ne bloque indéfiniment si ce channel n'est jamais fermé,
	// on lance une goroutine qui se termine avec le contexte.
	go func() {
		<-ctx.Done()
		n.logger.Debug("NO-OP: Plan update listener goroutine exiting due to context", "plan_id", planID)
		// Ne pas fermer updatedPlanChan ici, c'est la responsabilité de l'appelant de ListenForUpdatedPlans
		// ou du select qui le consomme de gérer sa fermeture.
		// Cependant, pour un mock, on pourrait le fermer pour signaler la fin.
		// Pour être plus réaliste, on ne le ferme pas ici.
	}()
	return nil
}

func (n *noopComms) RequestInitialPlan(ctx context.Context, req *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error) {
	if !n.connected {
		return nil, errors.New("noopComms: not connected")
	}
	n.logger.Info("NO-OP: RequestInitialPlan called", "request_id", req.RequestId)
	// Retourner un plan factice pour permettre au downloader de démarrer
	// Dans une vraie implémentation, ce serait un appel QUIC.
	time.Sleep(100 * time.Millisecond) // Simuler la latence réseau
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Créer un plan factice minimal
	if len(req.FilesToTransfer) == 0 {
		return nil, errors.New("no files in transfer request")
	}
	fm := req.FilesToTransfer[0]
	return &qwrappb.TransferPlan{
		PlanId:             "mock-plan-" + fm.FileId,
		ClientRequestId:    req.RequestId,
		SourceFileMetadata: []*qwrappb.FileMetadata{fm},
		DefaultChunkSize:   1024 * 1024, // 1MB
		ChunkAssignments:   []*qwrappb.ChunkAssignment{
			// Ajouter une ou deux assignations factices si fm.TotalSize le permet
			// Pour cela, il faudrait un agent factice connu.
			// Laissons vide pour l'instant, le downloader devrait gérer un plan vide.
			// Ou mieux, retourner une erreur si on ne peut pas faire un plan utile.
		},
		Options: req.Options,
	}, nil
}

func (n *noopComms) Close() error {
	n.logger.Info("NO-OP: Close called")
	n.connected = false
	return nil
}

// S'assurer que noopComms implémente bien Comms
var _ Comms = (*noopComms)(nil)
