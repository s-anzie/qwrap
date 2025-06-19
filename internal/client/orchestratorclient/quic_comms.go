package orchestratorclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog" // Pour net.ErrClosed
	"strings"  // Pour strings.Contains
	"sync"
	"time"

	"qwrap/internal/framing" // Assurez-vous que le chemin est correct
	"qwrap/pkg/qwrappb"      // Assurez-vous que le chemin est correct

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

// QuicCommsALPN est la constante pour le protocole applicatif. Exporter pour cmd/client/main.go
func QuicCommsALPN() string {
	return "qwrap-orchestrator" // ALPN pour Client <-> Orchestrateur
}

type quicCommsImpl struct {
	orchestratorAddress string
	tlsConfig           *tls.Config
	quicConfig          *quic.Config // Peut être nil pour utiliser les valeurs par défaut de quic-go
	logger              *slog.Logger

	connMu sync.Mutex // Protège qc.conn
	conn   quic.Connection

	writerFactory func(io.Writer, *slog.Logger) framing.Writer
	readerFactory func(io.Reader, *slog.Logger) framing.Reader
}

// NewQuicComms crée une implémentation de Comms basée sur QUIC.
func NewQuicComms(
	orchestratorAddress string,
	tlsConf *tls.Config,
	logger *slog.Logger,
	writerFactory func(io.Writer, *slog.Logger) framing.Writer,
	readerFactory func(io.Reader, *slog.Logger) framing.Reader,
) (Comms, error) {
	if tlsConf == nil {
		return nil, errors.New("TLS config is required for QUIC orchestrator client")
	}
	// S'assurer que l'ALPN est correctement configuré pour cette communication spécifique
	commsSpecificTLSConf := tlsConf.Clone() // Cloner pour ne pas affecter la config partagée
	commsSpecificTLSConf.NextProtos = []string{QuicCommsALPN()}

	if logger == nil {
		logger = slog.Default()
	}
	if writerFactory == nil { // Fournir des factories par défaut si nil
		writerFactory = func(w io.Writer, l *slog.Logger) framing.Writer {
			return framing.NewMessageWriter(w, l)
		}
	}
	if readerFactory == nil {
		readerFactory = func(r io.Reader, l *slog.Logger) framing.Reader {
			return framing.NewMessageReader(r, l)
		}
	}

	return &quicCommsImpl{
		orchestratorAddress: orchestratorAddress,
		tlsConfig:           commsSpecificTLSConf,
		quicConfig: &quic.Config{
			MaxIdleTimeout:       30 * time.Second,
			HandshakeIdleTimeout: 10 * time.Second,
			KeepAlivePeriod:      15 * time.Second, // Utile pour maintenir la connexion
		},
		logger:        logger.With("component", "quic_orchestrator_comms", "orch_addr", orchestratorAddress),
		writerFactory: writerFactory,
		readerFactory: readerFactory,
	}, nil
}

// Connect établit la connexion QUIC. Appelée par getConnection si nécessaire.
func (qc *quicCommsImpl) Connect(ctx context.Context, addr string) error {
	qc.connMu.Lock()
	// Si une connexion existe et est active, la réutiliser
	if qc.conn != nil && qc.conn.Context().Err() == nil {
		qc.connMu.Unlock()
		qc.logger.Debug("Already connected to orchestrator.")
		return nil
	}
	// Si une connexion existait mais est morte, la fermer avant de retenter
	if qc.conn != nil {
		_ = qc.conn.CloseWithError(0, "stale connection, reconnecting") // Ignorer l'erreur de fermeture
		qc.conn = nil
	}
	qc.connMu.Unlock() // Libérer le verrou avant l'appel bloquant DialAddr

	qc.logger.Info("Attempting to connect to orchestrator...")
	// Utiliser le contexte fourni pour le Dial, avec un timeout potentiellement plus court que le ctx global du downloader
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second) // Timeout pour l'opération de Dial
	defer dialCancel()

	conn, err := quic.DialAddr(dialCtx, qc.orchestratorAddress, qc.tlsConfig, qc.quicConfig)
	if err != nil {
		qc.logger.Error("Failed to dial orchestrator", "error", err)
		return fmt.Errorf("dial orchestrator at %s failed: %w", qc.orchestratorAddress, err)
	}

	qc.connMu.Lock()
	qc.conn = conn
	qc.connMu.Unlock()
	qc.logger.Info("Successfully connected to orchestrator.")
	return nil
}

// getConnection obtient une connexion active, en essayant de se connecter si nécessaire.
func (qc *quicCommsImpl) getConnection(ctx context.Context) (quic.Connection, error) {
	qc.connMu.Lock()
	if qc.conn != nil && qc.conn.Context().Err() == nil {
		conn := qc.conn
		qc.connMu.Unlock()
		return conn, nil
	}
	qc.connMu.Unlock()

	// Tenter de (re)connecter, Connect gère son propre lock interne pour qc.conn
	if err := qc.Connect(ctx, ""); err != nil {
		return nil, err
	}

	qc.connMu.Lock() // Reprendre le lock pour retourner qc.conn de manière sûre
	conn := qc.conn
	qc.connMu.Unlock()
	if conn == nil { // Ne devrait pas arriver si Connect a réussi
		return nil, errors.New("connection is nil after connect attempt")
	}
	return conn, nil
}

// doRequestResponse gère un échange requête/réponse simple sur un nouveau flux.
func (qc *quicCommsImpl) doRequestResponse(
	ctx context.Context, // Contexte pour l'ensemble de l'opération req/resp
	purposeValue qwrappb.StreamPurposeRequest_Purpose,
	planIDContext string, // PlanID pour le StreamPurposeRequest si applicable
	requestMsg proto.Message, // Le message de requête applicatif (peut être nil)
	responseMsg proto.Message, // Le message de réponse applicatif à peupler (peut être nil)
) error {
	conn, err := qc.getConnection(ctx) // Assure une connexion active
	if err != nil {
		return fmt.Errorf("failed to get connection for %v: %w", purposeValue, err)
	}

	// Utiliser un sous-contexte avec timeout pour l'ouverture du flux et les I/O
	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second) // Timeout pour cette transaction
	defer streamCancel()

	stream, err := conn.OpenStreamSync(streamCtx)
	if err != nil {
		// Si OpenStreamSync échoue, la connexion est peut-être mauvaise. Invalider.
		qc.invalidateConnection("open stream failed")
		return fmt.Errorf("failed to open stream for %v: %w", purposeValue, err)
	}
	defer stream.Close() // S'assurer que le flux est fermé

	logger := qc.logger.With("stream_id", stream.StreamID(), "purpose", purposeValue)
	writer := qc.writerFactory(stream, logger)
	reader := qc.readerFactory(stream, logger)

	// 1. Envoyer le StreamPurposeRequest
	purposeWrapper := &qwrappb.StreamPurposeRequest{Purpose: purposeValue, PlanIdContext: planIDContext}
	if err := writer.WriteMsg(streamCtx, purposeWrapper); err != nil { // Utiliser streamCtx
		return fmt.Errorf("failed to write stream purpose %v: %w", purposeValue, err)
	}

	// 2. Envoyer le message de requête applicatif (si fourni)
	if requestMsg != nil {
		if err := writer.WriteMsg(streamCtx, requestMsg); err != nil {
			return fmt.Errorf("failed to write request message for %v: %w", purposeValue, err)
		}
	}

	// À ce stade, le client a fini d'écrire pour une requête/réponse simple.
	// L'orchestrateur lira la/les requête(s) puis enverra sa réponse.
	// Fermer la partie écriture du flux du client n'est pas simple avec l'API quic-go stream.
	// stream.Close() ferme les deux côtés. On se fie à la lecture de la réponse.

	// 3. Lire le message de réponse applicatif (si attendu)
	if responseMsg != nil {
		if err := reader.ReadMsg(streamCtx, responseMsg); err != nil { // Utiliser streamCtx
			if errors.Is(err, io.EOF) && purposeValue == qwrappb.StreamPurposeRequest_CLIENT_REPORT {
				// Pour ClientReport, un EOF après l'envoi est normal, pas de contenu de réponse attendu.
				logger.Debug("EOF received after sending ClientReport, as expected.")
				return nil
			}
			return fmt.Errorf("failed to read response message for %v: %w", purposeValue, err)
		}
	}
	return nil
}

func (qc *quicCommsImpl) RequestInitialPlan(ctx context.Context, req *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error) {
	qc.logger.Info("Requesting initial plan from orchestrator", "request_id", req.RequestId)
	resp := &qwrappb.TransferPlan{}
	// doRequestResponse utilise déjà les factories pour reader/writer
	err := qc.doRequestResponse(ctx, qwrappb.StreamPurposeRequest_TRANSFER_REQUEST, "", req, resp)
	if err != nil {
		return nil, fmt.Errorf("RequestInitialPlan stream/comms failed: %w", err)
	}
	if resp.ErrorMessage != "" {
		qc.logger.Error("Orchestrator returned an error in TransferPlan", "plan_id", resp.PlanId, "client_request_id", resp.ClientRequestId, "error_msg", resp.ErrorMessage)
		return resp, fmt.Errorf("orchestrator error for request %s: %s", resp.ClientRequestId, resp.ErrorMessage)
	}
	qc.logger.Info("Initial plan received successfully from orchestrator", "plan_id", resp.PlanId, "num_assignments", len(resp.ChunkAssignments))
	return resp, nil
}

func (qc *quicCommsImpl) ReportTransferStatus(ctx context.Context, report *qwrappb.ClientReport) error {
	qc.logger.Debug("Reporting transfer status", "plan_id", report.PlanId, "num_chunk_statuses", len(report.ChunkStatuses))
	// Pas de message de réponse attendu pour un rapport, responseMsg est nil
	err := qc.doRequestResponse(ctx, qwrappb.StreamPurposeRequest_CLIENT_REPORT, report.PlanId, report, nil)
	if err != nil {
		return fmt.Errorf("ReportTransferStatus failed: %w", err)
	}
	return nil
}

func (qc *quicCommsImpl) ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error {
	conn, err := qc.getConnection(ctx) // Assurer une connexion active
	if err != nil {
		return fmt.Errorf("cannot register for plan updates, connection failed: %w", err)
	}

	// Utiliser le contexte parent (ctx) pour OpenStreamSync, car ce flux est long-lived.
	// Le timeout pour l'ouverture elle-même peut être plus court.
	openCtx, openCancel := context.WithTimeout(ctx, 10*time.Second)
	updateStream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		qc.invalidateConnection("open stream for plan updates failed")
		return fmt.Errorf("failed to open stream for plan updates (plan %s): %w", planID, err)
	}
	qc.logger.Info("Opened stream for plan updates", "plan_id", planID, "stream_id", updateStream.StreamID())

	logger := qc.logger.With("plan_id", planID, "update_stream_id", updateStream.StreamID())
	writer := qc.writerFactory(updateStream, logger) // Pour envoyer la requête d'enregistrement

	// 1. Envoyer la requête StreamPurposeRequest pour s'enregistrer
	regReq := &qwrappb.StreamPurposeRequest{
		Purpose:       qwrappb.StreamPurposeRequest_REGISTER_FOR_PLAN_UPDATES,
		PlanIdContext: planID,
	}
	// Utiliser le contexte du stream (ctx) pour l'écriture de la requête d'enregistrement.
	// Si le stream se ferme, ctx sera annulé.
	writePurposeCtx, writePurposeCancel := context.WithTimeout(ctx, 5*time.Second)
	err = writer.WriteMsg(writePurposeCtx, regReq)
	writePurposeCancel()
	if err != nil {
		_ = updateStream.Close() // Fermer le stream si l'enregistrement échoue
		return fmt.Errorf("failed to send register for plan updates request (plan %s): %w", planID, err)
	}
	qc.logger.Debug("Sent registration request for plan updates", "plan_id", planID)
	// Le client a fini d'écrire sur ce flux. L'orchestrateur va maintenant pousser des messages.

	// 2. Lancer une goroutine pour lire continuellement les UpdatedTransferPlan
	go func() {
		// Le defer updateStream.Close() est crucial pour nettoyer quand la goroutine se termine
		// (soit par ctx.Done(), soit par une erreur de lecture fatale).
		defer updateStream.Close()
		defer qc.logger.Info("Plan update listener goroutine stopped", "plan_id", planID, "stream_id", updateStream.StreamID())
		defer close(updatedPlanChan)
		reader := qc.readerFactory(updateStream, qc.logger.With("plan_id", planID, "update_stream_id", updateStream.StreamID())) // Reader pour ce flux

		for {
			select {
			case <-ctx.Done(): // Contexte du Downloader (ou du client principal) annulé
				qc.logger.Info("Plan update listener stopping due to parent context cancellation", "plan_id", planID)
				updateStream.CancelRead(quic.StreamErrorCode(0)) // Signaler à l'autre bout
				return
			default:
			}

			var planUpdate qwrappb.UpdatedTransferPlan
			// Lire avec le contexte du stream. Si le stream est fermé par le serveur ou la connexion meurt,
			// ctx (le contexte de la goroutine RegisterForPlanUpdates) sera annulé.
			// ReadMsg doit respecter ce contexte.
			errRead := reader.ReadMsg(ctx, &planUpdate) // Utiliser le contexte `ctx` de la goroutine

			if errRead != nil {
				logFields := []any{"plan_id", planID, "stream_id", updateStream.StreamID(), "error", errRead}
				if !(errors.Is(errRead, io.EOF) || errors.Is(errRead, context.Canceled) || strings.Contains(errRead.Error(), "connection is closed") || strings.Contains(errRead.Error(), "stream reset")) {
					qc.logger.Error("Error reading updated plan from stream, stopping listener.", "plan_id", planID, "error", errRead)
				} else if _, ok := errRead.(*quic.IdleTimeoutError); ok {
					qc.logger.Warn("Plan update stream idle timeout while reading", logFields...)
				} else if appErr, ok := errRead.(*quic.ApplicationError); ok {
					qc.logger.Warn("Plan update stream closed by orchestrator with application error", append([]any{"app_error_code", appErr.ErrorCode}, logFields...)...)
				} else {
					qc.logger.Info("Plan update stream ended or context cancelled while reading.", "plan_id", planID, "error", errRead)
				}
				return // Sortir de la goroutine
			}

			qc.logger.Debug("Client received updated plan from orchestrator", "plan_id", planUpdate.PlanId, "num_new_assign", len(planUpdate.NewOrUpdatedAssignments))
			select {
			case updatedPlanChan <- &planUpdate: // Envoyer au channel du Downloader
			case <-ctx.Done():
				qc.logger.Info("Context cancelled while sending updated plan to internal channel", "plan_id", planID)
				return
			case <-time.After(2 * time.Second): // Éviter de bloquer indéfiniment si le downloader est coincé
				qc.logger.Warn("Timeout sending updated plan to downloader's internal channel, dropping update.", "plan_id", planID)
			}
		}
	}()
	return nil
}

func (qc *quicCommsImpl) Close() error {
	qc.connMu.Lock()
	defer qc.connMu.Unlock()
	if qc.conn != nil {
		// Utiliser un code d'erreur applicatif et un message pour une fermeture normale.
		err := qc.conn.CloseWithError(quic.ApplicationErrorCode(0), "client orchestrator communication closed")
		qc.conn = nil // Important pour forcer une reconnexion si Connect est rappelé
		qc.logger.Info("Closed connection to orchestrator.")
		if err != nil && !strings.Contains(err.Error(), "closing an already closed connection") { // quic-go peut retourner cette erreur
			return fmt.Errorf("error closing orchestrator connection: %w", err)
		}
		return nil
	}
	return errors.New("connection already closed or never established")
}

// invalidateConnection ferme la connexion actuelle pour forcer une reconnexion au prochain appel.
func (qc *quicCommsImpl) invalidateConnection(reason string) {
	qc.connMu.Lock()
	if qc.conn != nil {
		qc.logger.Warn("Invalidating orchestrator connection", "reason", reason)
		_ = qc.conn.CloseWithError(quic.ApplicationErrorCode(1), "invalidating connection: "+reason) // Code d'erreur applicatif
		qc.conn = nil
	}
	qc.connMu.Unlock()
}

var _ Comms = (*quicCommsImpl)(nil) // Vérification de l'interface
