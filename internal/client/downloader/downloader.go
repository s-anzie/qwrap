package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"qwrap/internal/client/orchestratorclient"
	"qwrap/internal/framing" // Ajout de l'import pour les factories par défaut
	"qwrap/pkg/qwrappb"

	"google.golang.org/protobuf/proto"
)

type downloaderConfig struct {
	Concurrency             int
	MaxLocalRetries         int
	MaxTotalRetriesPerChunk int
	RetryBaseDelay          time.Duration
	ChunkRequestTimeout     time.Duration
	ReassignmentWaitTimeout time.Duration
	Logger                  *slog.Logger
	ConnManager             ConnectionManager
	OrchestratorComms       orchestratorclient.Comms
	WriterFactory           func(io.Writer, *slog.Logger) framing.Writer // Ajouté
	ReaderFactory           func(io.Reader, *slog.Logger) framing.Reader // Ajouté
}

func (c *downloaderConfig) setDefaults(logger *slog.Logger) {
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.MaxLocalRetries < 0 {
		c.MaxLocalRetries = MaxLocalRetriesPerAssign
	}
	if c.MaxTotalRetriesPerChunk <= 0 {
		c.MaxTotalRetriesPerChunk = MaxTotalRetriesPerChunk
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = DefaultRetryBaseDelayChunk
	}
	if c.ChunkRequestTimeout <= 0 {
		c.ChunkRequestTimeout = ChunkRequestTimeout
	}
	if c.ReassignmentWaitTimeout <= 0 {
		c.ReassignmentWaitTimeout = ReassignmentWaitTimeout
	}

	if logger == nil {
		c.Logger = slog.Default().With("component", "downloader")
	} else {
		c.Logger = logger.With("component", "downloader")
	}

	if c.ConnManager == nil {
		panic("ConnectionManager is required for downloader")
	}
	if c.OrchestratorComms == nil {
		c.OrchestratorComms = orchestratorclient.NewNoOpComms(c.Logger)
		c.Logger.Warn("OrchestratorComms not provided to downloader, using NoOpComms. Plan updates and reporting will be simulated.")
	}

	// Factories par défaut si non fournies
	if c.WriterFactory == nil {
		c.WriterFactory = func(w io.Writer, l *slog.Logger) framing.Writer {
			if l == nil {
				l = slog.Default()
			} // Sécurité
			return framing.NewMessageWriter(w, l.With("subcomponent", "dl_msg_writer_default"))
		}
	}
	if c.ReaderFactory == nil {
		c.ReaderFactory = func(r io.Reader, l *slog.Logger) framing.Reader {
			if l == nil {
				l = slog.Default()
			} // Sécurité
			return framing.NewMessageReader(r, l.With("subcomponent", "dl_msg_reader_default"))
		}
	}
}

type pipelineDownloader struct {
	config      downloaderConfig // Contient maintenant les factories
	connManager ConnectionManager
	orchComms   orchestratorclient.Comms

	shutdownOnce sync.Once
	stopCh       chan struct{}
	mainWg       sync.WaitGroup
}

// NewDownloader (interface publique)
func NewDownloader(
	connMgr ConnectionManager,
	orchComms orchestratorclient.Comms,
	logger *slog.Logger,
	concurrency int,
	// Ajout des factories ici pour qu'elles puissent être passées depuis main
	writerFactory func(io.Writer, *slog.Logger) framing.Writer,
	readerFactory func(io.Reader, *slog.Logger) framing.Reader,
) Downloader {
	cfg := downloaderConfig{
		ConnManager:       connMgr,
		OrchestratorComms: orchComms,
		Concurrency:       concurrency,
		WriterFactory:     writerFactory, // Passer les factories fournies
		ReaderFactory:     readerFactory, // Passer les factories fournies
	}
	cfg.setDefaults(logger) // Applique les défauts y compris pour les factories si elles étaient nil

	return &pipelineDownloader{
		config:      cfg, // config contient maintenant des factories valides
		connManager: connMgr,
		orchComms:   orchComms,
		stopCh:      make(chan struct{}),
	}
}

// Download (reste identique)
func (d *pipelineDownloader) Download(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	dest DestinationWriter,
) (<-chan ProgressInfo, <-chan error) {
	progressChan := make(chan ProgressInfo, 100)
	finalErrorChan := make(chan error, 1)

	d.mainWg.Add(1)
	go func() {
		defer d.mainWg.Done()
		defer close(progressChan)
		defer close(finalErrorChan)

		downloadCtx, downloadCancel := context.WithCancel(ctx)
		defer downloadCancel()

		go func() {
			select {
			case <-d.stopCh:
				d.config.Logger.Debug("Downloader stopCh closed, cancelling download context.")
				downloadCancel()
			case <-downloadCtx.Done():
			}
		}()

		finalErrorChan <- d.manageDownloadLifecycle(downloadCtx, initialTransferReq, dest, progressChan)
	}()

	return progressChan, finalErrorChan
}

// Shutdown (reste identique)
func (d *pipelineDownloader) Shutdown(timeout time.Duration) error {
	d.config.Logger.Info("Shutdown requested for downloader pipeline.")
	d.shutdownOnce.Do(func() {
		close(d.stopCh)
	})

	done := make(chan struct{})
	go func() {
		d.mainWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.config.Logger.Info("Downloader pipeline shutdown completed.")
		return nil
	case <-time.After(timeout):
		d.config.Logger.Error("Downloader pipeline shutdown timed out.")
		return errors.New("downloader pipeline shutdown timed out")
	}
}

// manageDownloadLifecycle (l'appel à newDownloadWorker est maintenant correct)
func (d *pipelineDownloader) manageDownloadLifecycle(
	ctx context.Context,
	initialTransferReq *qwrappb.TransferRequest,
	dest DestinationWriter,
	progressChan chan<- ProgressInfo,
) (errPipeline error) {
	l := d.config.Logger.With("request_id", initialTransferReq.RequestId, "op", "manageDownloadLifecycle")
	l.Info("Download lifecycle starting.")

	defer func() {
		if r := recover(); r != nil {
			l.Error("Panic recovered in manageDownloadLifecycle", "panic", r)
			errPipeline = fmt.Errorf("panic in download lifecycle: %v", r)
		}
		l.Info("Download lifecycle finishing.", "final_error", errPipeline)
	}()

	if len(initialTransferReq.GetFilesToTransfer()) == 0 || initialTransferReq.GetFilesToTransfer()[0] == nil {
		return fmt.Errorf("invalid transfer request: no files specified")
	}
	mainFileToDownload := initialTransferReq.GetFilesToTransfer()[0]

	jobsForWorkersChan := make(chan downloadJob, d.config.Concurrency)
	rawResultsFromWorkersChan := make(chan *chunkResult, d.config.Concurrency*2)
	orderedChunksToWriteChan := make(chan fileWriterJob, WriteBufferSize)
	planUpdatesFromOrchestratorChan := make(chan *qwrappb.UpdatedTransferPlan, PlanUpdateBufferSize)
	reportsToOrchestratorChan := make(chan *qwrappb.ClientReport, 10)

	var actorsWg sync.WaitGroup
	actorsCtx, cancelActors := context.WithCancel(ctx)
	defer cancelActors()

	liaison := newOrchestratorLiaison(d.orchComms, reportsToOrchestratorChan, planUpdatesFromOrchestratorChan, l)
	actorsWg.Add(1)
	go liaison.run(actorsCtx, &actorsWg, initialTransferReq)

	var initialPlan *qwrappb.TransferPlan
	var planErr error
	select {
	case plan, ok := <-liaison.initialPlanChan:
		if !ok {
			planErr = errors.New("orchestrator liaison closed initial plan channel unexpectedly")
		} else {
			initialPlan = plan
		}
	case <-time.After(OrchestratorCommsTimeout * 2):
		planErr = errors.New("timeout waiting for initial plan")
	case <-ctx.Done():
		return ctx.Err()
	}

	if planErr != nil {
		l.Error("Failed to get initial plan", "error", planErr)
		return planErr
	}
	if initialPlan.ErrorMessage != "" {
		l.Error("Orchestrator returned error in initial plan", "error", initialPlan.ErrorMessage)
		return fmt.Errorf("orchestrator plan error: %s", initialPlan.ErrorMessage)
	}
	if len(initialPlan.SourceFileMetadata) == 0 || initialPlan.SourceFileMetadata[0] == nil {
		return fmt.Errorf("%w: initial plan missing source file metadata", ErrOrchestratorPlan)
	}
	if initialPlan.SourceFileMetadata[0].FileId != mainFileToDownload.FileId {
		return fmt.Errorf("%w: initial plan is for file %s, but request was for %s", ErrOrchestratorPlan, initialPlan.SourceFileMetadata[0].FileId, mainFileToDownload.FileId)
	}
	mainFileToDownload = initialPlan.SourceFileMetadata[0]

	l = l.With("plan_id", initialPlan.PlanId, "file_id", mainFileToDownload.FileId)
	liaison.setPlanID(initialPlan.PlanId)
	actorsWg.Add(1)
	go liaison.listenForPlanUpdates(actorsCtx, &actorsWg)

	if mainFileToDownload.TotalSize == 0 && len(initialPlan.ChunkAssignments) == 0 {
		l.Info("File is empty, download considered complete.")
		if err := dest.Truncate(0); err != nil {
			return fmt.Errorf("failed to truncate destination for empty file: %w", err)
		}
		progressChan <- ProgressInfo{TotalSizeBytes: 0, DownloadedSizeBytes: 0, TotalChunks: 0, CompletedChunks: 0}
		return nil
	}
	if len(initialPlan.ChunkAssignments) == 0 && mainFileToDownload.TotalSize > 0 {
		return fmt.Errorf("%w: no chunk assignments for non-empty file", ErrOrchestratorPlan)
	}

	if mainFileToDownload.TotalSize > 0 {
		if err := dest.Truncate(mainFileToDownload.TotalSize); err != nil {
			l.Warn("Failed to pre-allocate destination file", "error", err)
		}
	}

	dispatcher := newJobDispatcher(jobsForWorkersChan, planUpdatesFromOrchestratorChan, l)
	actorsWg.Add(1)
	go dispatcher.run(actorsCtx, &actorsWg, initialPlan)

	for i := 0; i < d.config.Concurrency; i++ {
		worker := newDownloadWorker(
			i+1,
			d.connManager,
			jobsForWorkersChan,
			rawResultsFromWorkersChan,
			d.config.Logger,        // Utiliser le logger de config du downloader
			d.config.WriterFactory, // Passer la factory de la config
			d.config.ReaderFactory, // Passer la factory de la config
			d.config.ChunkRequestTimeout,
		)
		actorsWg.Add(1)
		go worker.run(actorsCtx, &actorsWg)
	}

	aggregator := newResultAggregator(
		initialPlan,
		rawResultsFromWorkersChan,
		orderedChunksToWriteChan,
		reportsToOrchestratorChan,
		progressChan,
		l,
		d.config.MaxLocalRetries,
		d.config.MaxTotalRetriesPerChunk,
		d.config.ReassignmentWaitTimeout,
		dispatcher.retryJobChan,
	)
	actorsWg.Add(1)
	go aggregator.run(actorsCtx, &actorsWg)

	fileWriter := newSequentialFileWriter(orderedChunksToWriteChan, dest, l)
	actorsWg.Add(1)
	go fileWriter.run(actorsCtx, &actorsWg)

	var finalErrorLoop error
	select {
	case <-aggregator.doneChan:
		finalErrorLoop = aggregator.finalError
		l.Info("ResultAggregator finished.", "error", finalErrorLoop)
	case err := <-fileWriter.errorChan:
		finalErrorLoop = err
		l.Error("SequentialFileWriter reported a fatal error.", "error", finalErrorLoop)
		cancelActors()
	case <-ctx.Done():
		finalErrorLoop = ctx.Err()
		l.Info("Download context cancelled, stopping actors.", "error", finalErrorLoop)
		cancelActors()
	}

	l.Info("Primary actors signaled completion or context cancelled. Waiting for all actors to exit.", "reason_error", finalErrorLoop)

	waitTimeout := ActorShutdownTimeout
	if finalErrorLoop != nil && errors.Is(finalErrorLoop, context.DeadlineExceeded) {
		waitTimeout = 2 * time.Second
	}

	doneWaitingActors := make(chan struct{})
	go func() {
		actorsWg.Wait()
		close(doneWaitingActors)
	}()

	select {
	case <-doneWaitingActors:
		l.Info("All actors finished.")
	case <-time.After(waitTimeout):
		l.Error("Timeout waiting for actors to finish.")
		if finalErrorLoop == nil {
			finalErrorLoop = errors.New("timeout waiting for actors to shutdown")
		}
	}
	return finalErrorLoop
}

// mergePlan (reste inchangé)
func mergePlan(current *qwrappb.TransferPlan, update *qwrappb.UpdatedTransferPlan) *qwrappb.TransferPlan {
	if current == nil {
		return &qwrappb.TransferPlan{
			PlanId:           update.PlanId,
			ChunkAssignments: update.NewOrUpdatedAssignments,
		}
	}
	if update == nil || current.PlanId != update.PlanId {
		return current
	}
	newPlan := proto.Clone(current).(*qwrappb.TransferPlan)
	assignmentsMap := make(map[uint64]*qwrappb.ChunkAssignment)
	for _, assign := range newPlan.ChunkAssignments {
		if assign != nil && assign.ChunkInfo != nil {
			assignmentsMap[assign.ChunkInfo.ChunkId] = assign
		}
	}
	for _, newAssign := range update.NewOrUpdatedAssignments {
		if newAssign != nil && newAssign.ChunkInfo != nil {
			assignmentsMap[newAssign.ChunkInfo.ChunkId] = newAssign
		}
	}
	for _, invalidated := range update.InvalidatedChunks {
		if invalidated != nil {
			delete(assignmentsMap, invalidated.ChunkId)
		}
	}
	finalAssignments := make([]*qwrappb.ChunkAssignment, 0, len(assignmentsMap))
	for _, assign := range assignmentsMap {
		finalAssignments = append(finalAssignments, assign)
	}
	sort.Slice(finalAssignments, func(i, j int) bool {
		if finalAssignments[i].ChunkInfo == nil || finalAssignments[j].ChunkInfo == nil {
			return false
		}
		return finalAssignments[i].ChunkInfo.ChunkId < finalAssignments[j].ChunkInfo.ChunkId
	})
	newPlan.ChunkAssignments = finalAssignments
	return newPlan
}
