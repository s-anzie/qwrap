package kuik

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"

	"qwrap/internal/client/downloader"
	connectionmanager "qwrap/internal/client/manager"
	"qwrap/internal/client/orchestratorclient"
	"qwrap/internal/framing"
)

// Transport is a mock of the quic-go Transport, providing an entry point for dialing connections.
// In our implementation, it will set up the underlying qwrap client infrastructure.
type Transport struct {
	Logger          *slog.Logger
	OrchestratorTLS *tls.Config
	AgentTLS        *tls.Config
}

// DialContext establishes a new QUIC connection to a server.
// It mimics the quic-go API.
// Behind the scenes, this will initialize and return a qwrap-based connection.
func (t *Transport) DialContext(ctx context.Context, addr string, config *Config) (Connection, error) {
	mainCtx, cancel := context.WithCancel(ctx)

	// Default logger if not provided
	logger := t.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	// This is a simplified setup based on cmd/client/main.go
	connMgrConfig := connectionmanager.Config{TLSClientConfig: t.AgentTLS, Logger: logger}
	connMgr := connectionmanager.NewConnectionManager(connMgrConfig)

	writerFactory := func(w io.Writer, l *slog.Logger) framing.Writer {
		return framing.NewMessageWriter(w, l)
	}
	readerFactory := func(r io.Reader, l *slog.Logger) framing.Reader {
		return framing.NewMessageReader(r, l)
	}

	orchComms, err := orchestratorclient.NewQuicComms(addr, t.OrchestratorTLS, logger, writerFactory, readerFactory)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create orchestrator comms: %w", err)
	}

	concurrency := 10 // Default concurrency
	if config.QwrapConcurrency > 0 {
		concurrency = config.QwrapConcurrency
	}
	dl := downloader.NewDownloader(connMgr, orchComms, logger, concurrency, writerFactory, readerFactory)

	remoteTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to resolve remote address: %w", err)
	}

	conn := &qwrapConnection{
		downloader: dl,
		cancelFunc: cancel,
		ctx:        mainCtx,
		remoteAddr: remoteTCPAddr,
		config:     config, // Store config for later use
	}

	return conn, nil
}
