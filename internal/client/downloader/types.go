package downloader

import (
	"errors"
	"time"

	"qwrap/pkg/qwrappb"
)

// downloadJob représente une tâche de téléchargement pour un chunk spécifique par un worker.
// Ces compteurs sont ceux *au moment où le job est créé* pour le worker.
type downloadJob struct {
	assignment               *qwrappb.ChunkAssignment
	attemptForThisAssignment int // Tentative actuelle pour CETTE assignation (commence à 1)
	totalAttemptsForChunk    int // Nombre total de tentatives pour CE CHUNK jusqu'à présent
}

// chunkResult (inchangé par rapport à la dernière correction)
type chunkResult struct {
	job          downloadJob
	data         []byte
	err          error
	downloadTime time.Duration
	agentID      string
	agentAddress string
	// isVerified   bool // Checksum ignoré pour l'instant
	chunkInfo *qwrappb.ChunkInfo
}

// fileWriterJob (inchangé)
type fileWriterJob struct {
	chunkID uint64
	offset  int64
	data    []byte
}

// chunkState représente l'état d'un chunk dans le pipeline du ResultAggregator.
type chunkState struct {
	info                        *qwrappb.ChunkInfo
	currentAssignment           *qwrappb.ChunkAssignment
	currentAssignmentAttempts   int // Compteur de tentatives sur l'assignation actuelle
	totalOverallAttempts        int // Compteur total de tentatives pour ce chunk (toutes assignations)
	lastFailureTime             time.Time
	status                      qwrappb.TransferEvent
	awaitingReassignment        bool
	lastReassignmentRequestTime time.Time
	isCompleted                 bool
	isPermanentlyFailed         bool
	data                        []byte
}

// Constants (inchangées, mais `progressReportInterval` sera utilisé par main.go)
const (
	DefaultConcurrency         = 10 // Renommé pour clarté et pour éviter conflit si importé
	MaxLocalRetriesPerAssign   = 2
	MaxTotalRetriesPerChunk    = 5
	DefaultRetryBaseDelayChunk = 250 * time.Millisecond
	ReassignRequestBackoff     = 5 * time.Second
	ReassignmentWaitTimeout    = 30 * time.Second
	ProgressReportInterval     = 2 * time.Second // Exporté implicitement car en majuscule

	InitialJobBufferSize = 100
	ResultBufferSize     = 100
	WriteBufferSize      = 50
	PlanUpdateBufferSize = 10
	DefaultPermits       = 10
	// progressReportInterval est défini ci-dessus
	FileWriterTimeout        = 30 * time.Second
	ActorShutdownTimeout     = 10 * time.Second
	LifecycleTimeout         = 5 * time.Minute
	MinChunkSizeForBuffer    = 16 * 1024
	MaxTempChunkBufferSize   = 50 * 1024 * 1024
	OrchestratorCommsTimeout = 15 * time.Second
	ChunkRequestTimeout      = 20 * time.Second
)

var ( // Erreurs (inchangées)
	ErrDownloadCancelled     = errors.New("download cancelled by context")
	ErrChecksumMismatch      = errors.New("checksum mismatch")
	ErrOrchestratorPlan      = errors.New("invalid transfer plan from orchestrator")
	ErrFileWriteFailed       = errors.New("failed to write to destination file")
	ErrChunkDownloadFailed   = errors.New("chunk download failed after all attempts")
	ErrOrchestratorCommsFail = errors.New("communication with orchestrator failed")
	errNoMoreAgentsForChunk  = errors.New("no more agents available or suitable for this chunk after retries")
	errWriterChannelFull     = errors.New("file writer channel is full, backpressure engaged")
	errJobChannelFull        = errors.New("job channel is full")
	errReassignmentTimeout   = errors.New("timed out waiting for chunk reassignment")
)
