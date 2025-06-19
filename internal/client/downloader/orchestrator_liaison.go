package downloader

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"qwrap/internal/client/orchestratorclient"
	"qwrap/pkg/qwrappb"
)

type orchestratorLiaison struct {
	comms              orchestratorclient.Comms
	reportsInChan      <-chan *qwrappb.ClientReport
	planUpdatesOutChan chan<- *qwrappb.UpdatedTransferPlan
	logger             *slog.Logger

	initialPlanReq  *qwrappb.TransferRequest
	initialPlanChan chan *qwrappb.TransferPlan // Pour envoyer le plan initial au manager de lifecycle

	mu     sync.Mutex
	planID string // Une fois connu
}

func newOrchestratorLiaison(
	comms orchestratorclient.Comms,
	reportsIn <-chan *qwrappb.ClientReport,
	planUpdatesOut chan<- *qwrappb.UpdatedTransferPlan,
	logger *slog.Logger,
) *orchestratorLiaison {
	return &orchestratorLiaison{
		comms:              comms,
		reportsInChan:      reportsIn,
		planUpdatesOutChan: planUpdatesOut,
		logger:             logger,
		initialPlanChan:    make(chan *qwrappb.TransferPlan, 1), // Buffer 1 pour ne pas bloquer
	}
}

func (ol *orchestratorLiaison) setPlanID(planID string) {
	ol.mu.Lock()
	defer ol.mu.Unlock()
	ol.planID = planID
}

func (ol *orchestratorLiaison) run(ctx context.Context, wg *sync.WaitGroup, initialReq *qwrappb.TransferRequest) {
	defer wg.Done()
	ol.logger.Info("OrchestratorLiaison starting...")
	defer ol.logger.Info("OrchestratorLiaison stopped.")
	defer close(ol.initialPlanChan) // Fermer quand run se termine

	ol.initialPlanReq = initialReq

	// Demander le plan initial
	planCtx, planCancel := context.WithTimeout(ctx, OrchestratorCommsTimeout)
	plan, err := ol.comms.RequestInitialPlan(planCtx, ol.initialPlanReq)
	planCancel()

	if err != nil {
		ol.logger.Error("Failed to request initial plan", "error", err)
		// La fermeture de initialPlanChan sans envoi signalera l'erreur.
		return
	}
	ol.initialPlanChan <- plan
	// Le planID sera défini par manageDownloadLifecycle via setPlanID

	// Boucle pour envoyer les rapports
	for {
		select {
		case <-ctx.Done():
			ol.logger.Info("Context cancelled, stopping report sender.")
			return
		case report, ok := <-ol.reportsInChan:
			if !ok {
				ol.logger.Info("Reports channel closed, stopping report sender.")
				return
			}
			reportCtx, reportCancel := context.WithTimeout(ctx, OrchestratorCommsTimeout)
			err := ol.comms.ReportTransferStatus(reportCtx, report)
			reportCancel()
			if err != nil {
				ol.logger.Error("Failed to send client report to orchestrator", "plan_id", report.PlanId, "error", err)
				// Que faire ici ? Retenter ? Pour l'instant, on logue.
			} else {
				ol.logger.Debug("Client report sent successfully", "plan_id", report.PlanId)
			}
		}
	}
}

func (ol *orchestratorLiaison) listenForPlanUpdates(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ol.logger.Info("Starting to listen for plan updates.")
	defer ol.logger.Info("Stopped listening for plan updates.")

	// Attendre que planID soit défini
	var currentPlanID string
	for {
		ol.mu.Lock()
		currentPlanID = ol.planID
		ol.mu.Unlock()
		if currentPlanID != "" {
			break
		}
		select {
		case <-ctx.Done():
			ol.logger.Info("Context cancelled before PlanID was set for listening.")
			return
		case <-time.After(100 * time.Millisecond):
			// Attendre
		}
	}
	ol.logger = ol.logger.With("plan_id_listener", currentPlanID)

	// Lancer la goroutine d'écoute de l'interface Comms
	// Cette goroutine interne à Comms devrait gérer sa propre fin sur ctx.Done()
	// et fermer planUpdatesOutChan quand elle se termine.
	// Le ListenForUpdatedPlans de l'interface Comms est bloquant et gère sa propre goroutine.
	err := ol.comms.ListenForUpdatedPlans(ctx, currentPlanID, ol.planUpdatesOutChan)
	if err != nil && !errors.Is(err, context.Canceled) {
		ol.logger.Error("ListenForUpdatedPlans returned an error", "error", err)
	}
	// Quand ListenForUpdatedPlans retourne (soit par ctx.Done, soit par erreur, soit par fin normale),
	// la goroutine du liaison a fini son travail d'écoute.
}
