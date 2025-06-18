package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"

	"qwrap/internal/framing"
	"qwrap/pkg/qwrappb"
)

// --- Mocks & Test Helpers ---

// mockConnManager simule un gestionnaire de connexion.
type mockConnManager struct {
	mock.Mock
}

func (m *mockConnManager) GetOrConnect(ctx context.Context, addr string) (quic.Connection, error) {
	args := m.Called(ctx, addr)
	conn, _ := args.Get(0).(quic.Connection)
	return conn, args.Error(1)
}

func (m *mockConnManager) Invalidate(ctx context.Context, addr string) {}

// mockOrchestratorComms simule la communication avec l'orchestrateur.
// Il implémente l'interface orchestratorclient.Comms pour les tests.
type mockOrchestratorComms struct {
	mock.Mock
}

func (m *mockOrchestratorComms) RequestInitialPlan(ctx context.Context, req *qwrappb.TransferRequest) (*qwrappb.TransferPlan, error) {
	args := m.Called(ctx, req)
	plan, _ := args.Get(0).(*qwrappb.TransferPlan)
	return plan, args.Error(1)
}

func (m *mockOrchestratorComms) ListenForUpdatedPlans(ctx context.Context, planID string, updatedPlanChan chan<- *qwrappb.UpdatedTransferPlan) error {
	args := m.Called(ctx, planID, updatedPlanChan)
	// Simule une écoute jusqu'à la fin du contexte.
	<-ctx.Done()
	return args.Error(0)
}

func (m *mockOrchestratorComms) ReportChunkTransferStatus(ctx context.Context, status *qwrappb.ChunkTransferStatus) error {
	// Pour ce test, nous n'avons pas besoin de suivre les rapports, donc nous ne faisons rien.
	m.Called(ctx, status)
	return nil
}

func (m *mockOrchestratorComms) ReportTransferStatus(ctx context.Context, report *qwrappb.ClientReport) error {
    m.Called(ctx, report)
    return nil
}

func (m *mockOrchestratorComms) Close() error {
	return nil
}

func (m *mockOrchestratorComms) Connect(ctx context.Context, target string) error {
	m.Called(ctx, target)
	return nil
}

// slowDestWriter simule un disque lent en implémentant DestinationWriter.
type slowDestWriter struct {
	f        *os.File
	slowdown time.Duration
	wg       sync.WaitGroup
}

func newSlowWriter(t *testing.T, path string, slowdown time.Duration) *slowDestWriter {
	f, err := os.Create(path)
	assert.NoError(t, err)
	return &slowDestWriter{f: f, slowdown: slowdown}
}

func (sw *slowDestWriter) WriteAt(p []byte, off int64) (n int, err error) {
	sw.wg.Add(1)
	defer sw.wg.Done()
	time.Sleep(sw.slowdown) // Simule la latence du disque
	return sw.f.WriteAt(p, off)
}

func (sw *slowDestWriter) Truncate(size int64) error {
	return sw.f.Truncate(size)
}

func (sw *slowDestWriter) Close() error {
	sw.wg.Wait() // Attendre la fin de toutes les écritures en suspens
	return sw.f.Close()
}

// --- Cas de Test ---

func TestDownloader_TokenBackpressure(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const (
		numChunks    = 20
		chunkSize    = 1024
		fileSize     = numChunks * chunkSize
		tokenPermits = 4 // Moins de jetons que de chunks pour forcer la backpressure
		diskSlowdown = 50 * time.Millisecond
	)

	// 1. Configuration des Mocks
	mockConnMgr := new(mockConnManager)
	mockOrch := new(mockOrchestratorComms)
	mockConnection := newMockQuicConnection(t, chunkSize)

	mockConnMgr.On("GetOrConnect", mock.Anything, "agent-addr:1234").Return(mockConnection, nil)

	// 2. Création du plan de transfert que l'orchestrateur simulé renverra
	assignments := make([]*qwrappb.ChunkAssignment, numChunks)
	for i := 0; i < numChunks; i++ {
		assignments[i] = &qwrappb.ChunkAssignment{
			AgentAddress: "agent-addr:1234", // Champ corrigé
			ChunkInfo: &qwrappb.ChunkInfo{
				FileId:  "file-1",
				ChunkId: uint64(i),
				Range:   &qwrappb.ByteRange{Offset: int64(i * chunkSize), Length: chunkSize},
			},
		}
	}
	transferPlan := &qwrappb.TransferPlan{
		PlanId:             "plan-123",
		ChunkAssignments:   assignments,
		SourceFileMetadata: []*qwrappb.FileMetadata{{FileId: "file-1", TotalSize: fileSize}},
	}

	mockOrch.On("RequestInitialPlan", mock.Anything, mock.Anything).Return(transferPlan, nil)
	mockOrch.On("ListenForUpdatedPlans", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockOrch.On("ReportChunkTransferStatus", mock.Anything, mock.Anything).Return(nil)

	// 3. Initialisation du Downloader avec le constructeur public
	dl := NewDownloader(
		mockConnMgr,
		mockOrch,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		tokenPermits, // Concurrency
		nil, nil,     // Utiliser les usines de reader/writer par défaut
	)
	assert.NotNil(t, dl)

	// 4. Préparation de la destination de fichier lente
	tempDir := t.TempDir()
	destPath := fmt.Sprintf("%s/testfile.out", tempDir)
	slowWriter := newSlowWriter(t, destPath, diskSlowdown)
	defer slowWriter.Close()

	// 5. Démarrage du téléchargement
	transferReq := &qwrappb.TransferRequest{
		RequestId:       "req-1",
		FilesToTransfer: []*qwrappb.FileMetadata{{FileId: "file-1"}},
	}

	startTime := time.Now()
	progressChan, finalErrorChan := dl.Download(ctx, transferReq, slowWriter)

	// 6. Consommation des résultats
	var totalProgress int64
	for p := range progressChan {
		totalProgress = p.DownloadedSizeBytes // Utiliser le champ de progression totale
	}
	duration := time.Since(startTime)

	err := <-finalErrorChan
	assert.NoError(t, err)

	// 7. Validation
	info, err := os.Stat(destPath)
	assert.NoError(t, err)
	assert.Equal(t, int64(fileSize), info.Size(), "La taille du fichier téléchargé est incorrecte.")
	assert.Equal(t, int64(fileSize), totalProgress, "Le rapport de progression est incorrect.")

	// La validation la plus importante : la backpressure a-t-elle fonctionné ?
	expectedBatches := (numChunks + tokenPermits - 1) / tokenPermits // Division entière par excès
	minDuration := time.Duration(expectedBatches) * diskSlowdown

	assert.True(t, duration >= minDuration,
		fmt.Sprintf("Le téléchargement a été trop rapide (%v), la backpressure pourrait ne pas fonctionner. Attendu au moins %v", duration, minDuration))

	t.Logf("Téléchargement terminé en %v, durée minimale attendue %v", duration, minDuration)

	// Nettoyage
	mockConnection.CloseWithError(0, "")
}

// --- Implémentation du mock QUIC ---

type mockQuicConnection struct {
	quic.Connection // Implémente l'interface de base
	t          *testing.T
	chunkSize  int
	closeOnce  sync.Once
	closed     chan struct{}
	requestsWg sync.WaitGroup
}

func newMockQuicConnection(t *testing.T, chunkSize int) *mockQuicConnection {
	return &mockQuicConnection{
		t:         t,
		chunkSize: chunkSize,
		closed:    make(chan struct{}),
	}
}

// OpenStreamSync est la méthode clé appelée par le downloader pour obtenir un nouveau flux.
func (m *mockQuicConnection) OpenStreamSync(ctx context.Context) (quic.Stream, error) {
	m.t.Log("mockQuicConnection: OpenStreamSync called by client")
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.closed:
		return nil, errors.New("mock connection is closed")
	default:
	}

	// Crée un nouveau flux mocké.
	stream, err := newMockQuicStream(ctx)
	if err != nil {
		return nil, err
	}

	// Démarre le handler pour ce flux dans une goroutine pour simuler la réponse du serveur.
	m.requestsWg.Add(1)
	go m.handleStream(stream, m.chunkSize)

	m.t.Logf("mockQuicConnection: OpenStreamSync created and returning new stream %p", stream)
	return stream, nil
}

// handleStream simule la logique de l'agent qui reçoit une requête et envoie les données du chunk.
func (m *mockQuicConnection) handleStream(s quic.Stream, chunkSize int) {
	defer m.requestsWg.Done()
	defer s.Close()

	mockStream := s.(*mockQuicStream)
	serverEnd := mockStream.serverEnd()
	reader := framing.NewMessageReader(serverEnd, slog.Default())

	// 1. Lire la requête de chunk envoyée par le downloader.
	req := &qwrappb.ChunkRequest{}
	if err := reader.ReadMsg(s.Context(), req); err != nil {
		m.t.Logf("mock agent handleStream: failed to read chunk request: %v", err)
		return
	}

	// 2. Envoyer les données brutes du chunk en réponse.
	rawData := make([]byte, chunkSize)
	if _, err := serverEnd.Write(rawData); err != nil {
		m.t.Logf("mock agent handleStream: failed to write chunk data: %v", err)
	}
}

// AcceptStream n'est pas utilisé côté client.
func (m *mockQuicConnection) AcceptStream(ctx context.Context) (quic.Stream, error) {
	return nil, errors.New("AcceptStream should not be called on the client-side mock connection")
}

func (m *mockQuicConnection) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	m.closeOnce.Do(func() {
		close(m.closed)
		m.requestsWg.Wait() // S'assurer que tous les handlers de flux sont terminés
	})
	return nil
}

type mockQuicStream struct {
	quic.Stream
	ctx         context.Context
	cancel      context.CancelFunc
	clientPipeR io.Reader
	clientPipeW io.WriteCloser
	serverPipeR io.Reader
	serverPipeW io.WriteCloser
}

func newMockQuicStream(ctx context.Context) (*mockQuicStream, error) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	streamCtx, cancel := context.WithCancel(ctx)
	return &mockQuicStream{
		ctx: streamCtx, cancel: cancel,
		clientPipeR: sr, clientPipeW: cw,
		serverPipeR: cr, serverPipeW: sw,
	}, nil
}

func (m *mockQuicStream) Read(p []byte) (n int, err error)  { return m.clientPipeR.Read(p) }
func (m *mockQuicStream) Write(p []byte) (n int, err error) { return m.clientPipeW.Write(p) }
func (m *mockQuicStream) Close() error {
	m.cancel()
	_ = m.clientPipeW.Close()
	_ = m.serverPipeW.Close()
	return nil
}
func (m *mockQuicStream) Context() context.Context { return m.ctx }
func (m *mockQuicStream) serverEnd() io.ReadWriteCloser {
	return &struct {
		io.Reader
		io.WriteCloser
	}{
		Reader:      m.serverPipeR,
		WriteCloser: m.serverPipeW,
	}
}

func (m *mockQuicStream) StreamID() quic.StreamID {
	return 1
}

func (m *mockQuicStream) CancelRead(code quic.StreamErrorCode) {
	m.serverPipeW.Close()
}

func (m *mockQuicStream) CancelWrite(code quic.StreamErrorCode) {
	m.clientPipeW.Close()
}
