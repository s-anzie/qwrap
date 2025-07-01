package qwrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// mqcImpl est le nom interne pour managedQuicConnectionImpl.
type mqcImpl struct {
	targetAddr      string
	connMgr         ConnectionManagerProvider
	logger          *slog.Logger
	currentQuicConn quic.Connection
	connMutex       sync.RWMutex
}

// NewManagedQuicConnection (inchangé)
func NewManagedQuicConnection(targetAddr string, manager ConnectionManagerProvider, logger *slog.Logger) ManagedQuicConnection {
	if logger == nil {
		logger = slog.Default()
	}
	return &mqcImpl{
		targetAddr: targetAddr,
		connMgr:    manager,
		logger:     logger.With("component", "ManagedQuicConnection", "target_addr", targetAddr),
	}
}

// getActiveConnection (inchangé)
func (mqc *mqcImpl) getActiveConnection(ctx context.Context) (quic.Connection, error) {
	mqc.connMutex.RLock()
	conn := mqc.currentQuicConn
	mqc.connMutex.RUnlock()

	if conn != nil && conn.Context().Err() == nil {
		return conn, nil
	}

	mqc.connMutex.Lock()
	defer mqc.connMutex.Unlock()

	if mqc.currentQuicConn != nil && mqc.currentQuicConn.Context().Err() == nil {
		return mqc.currentQuicConn, nil
	}

	newConn, err := mqc.connMgr.GetOrConnect(ctx, mqc.targetAddr)
	if err != nil {
		mqc.currentQuicConn = nil
		return nil, err
	}
	mqc.currentQuicConn = newConn
	return newConn, nil
}

// invalidateAndClear (inchangé)
func (mqc *mqcImpl) invalidateAndClear(reason string) {
	mqc.connMutex.Lock()
	if mqc.currentQuicConn != nil {
		mqc.logger.Debug("Invalidating and clearing cached connection", "reason", reason)
		invalidateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		mqc.connMgr.Invalidate(invalidateCtx, mqc.targetAddr)
		cancel()
		mqc.currentQuicConn = nil
	}
	mqc.connMutex.Unlock()
}

// --- Implémentation de ManagedQuicConnection ---

func (mqc *mqcImpl) OpenStream() (ManagedQuicBidirectionalStream, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("openstream: %w", err)
	}

	stream, err := conn.OpenStream() // Retourne quic.Stream
	if err != nil {
		mqc.invalidateAndClear("OpenStream failed")
		return nil, err
	}
	return &msBidiImpl{stream: stream, connWrapper: mqc, logger: mqc.logger}, nil
}

func (mqc *mqcImpl) OpenStreamSync(ctx context.Context) (ManagedQuicBidirectionalStream, error) {
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("openstreamsync: %w", err)
	}
	stream, err := conn.OpenStreamSync(ctx) // Retourne quic.Stream
	if err != nil {
		mqc.invalidateAndClear("OpenStreamSync failed")
		return nil, err
	}
	return &msBidiImpl{stream: stream, connWrapper: mqc, logger: mqc.logger}, nil
}

func (mqc *mqcImpl) OpenUniStream() (ManagedQuicSendStream, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("openunistream: %w", err)
	}
	sendStream, err := conn.OpenUniStream() // Retourne quic.SendStream
	if err != nil {
		mqc.invalidateAndClear("OpenUniStream failed")
		return nil, err
	}
	return &msSendImpl{sendStream: sendStream, connWrapper: mqc, logger: mqc.logger}, nil
}

func (mqc *mqcImpl) OpenUniStreamSync(ctx context.Context) (ManagedQuicSendStream, error) {
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("openunistreamsync: %w", err)
	}
	sendStream, err := conn.OpenUniStreamSync(ctx) // Retourne quic.SendStream
	if err != nil {
		mqc.invalidateAndClear("OpenUniStreamSync failed")
		return nil, err
	}
	return &msSendImpl{sendStream: sendStream, connWrapper: mqc, logger: mqc.logger}, nil
}

func (mqc *mqcImpl) AcceptStream(ctx context.Context) (ManagedQuicBidirectionalStream, error) {
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("acceptstream: %w", err)
	}
	stream, err := conn.AcceptStream(ctx) // Retourne quic.Stream
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) && err != quic.ErrServerClosed {
			mqc.invalidateAndClear("AcceptStream failed with non-standard error")
		}
		return nil, err
	}
	return &msBidiImpl{stream: stream, connWrapper: mqc, logger: mqc.logger}, nil
}

func (mqc *mqcImpl) AcceptUniStream(ctx context.Context) (ManagedQuicReceiveStream, error) {
	conn, err := mqc.getActiveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("acceptunistream: %w", err)
	}
	receiveStream, err := conn.AcceptUniStream(ctx) // Retourne quic.ReceiveStream
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) && err != quic.ErrServerClosed {
			mqc.invalidateAndClear("AcceptUniStream failed with non-standard error")
		}
		return nil, err
	}
	return &msRecvImpl{receiveStream: receiveStream, connWrapper: mqc, logger: mqc.logger}, nil
}

// CloseWithError, Context, RemoteAddr, LocalAddr, ConnectionState (inchangés)
func (mqc *mqcImpl) CloseWithError(appErrorCode quic.ApplicationErrorCode, reason string) error {
	mqc.connMutex.Lock()
	conn := mqc.currentQuicConn
	mqc.currentQuicConn = nil
	mqc.connMutex.Unlock()

	if conn != nil {
		invalidateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		mqc.connMgr.Invalidate(invalidateCtx, mqc.targetAddr)
		cancel()
		return conn.CloseWithError(appErrorCode, reason)
	}
	return nil
}

func (mqc *mqcImpl) Context() context.Context {
	mqc.connMutex.RLock()
	conn := mqc.currentQuicConn
	mqc.connMutex.RUnlock()
	if conn != nil {
		return conn.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func (mqc *mqcImpl) RemoteAddr() net.Addr {
	mqc.connMutex.RLock()
	conn := mqc.currentQuicConn
	mqc.connMutex.RUnlock()
	if conn != nil {
		return conn.RemoteAddr()
	}
	udpAddr, _ := net.ResolveUDPAddr("udp", mqc.targetAddr)
	return udpAddr
}

func (mqc *mqcImpl) LocalAddr() net.Addr {
	mqc.connMutex.RLock()
	conn := mqc.currentQuicConn
	mqc.connMutex.RUnlock()
	if conn != nil {
		return conn.LocalAddr()
	}
	return nil
}

func (mqc *mqcImpl) ConnectionState() quic.ConnectionState {
	mqc.connMutex.RLock()
	conn := mqc.currentQuicConn
	mqc.connMutex.RUnlock()
	if conn != nil {
		return conn.ConnectionState()
	}
	return quic.ConnectionState{}
}

// --- Implémentations des interfaces de flux managés ---

// msBidiImpl implémente ManagedQuicBidirectionalStream
type msBidiImpl struct {
	stream      quic.Stream
	connWrapper *mqcImpl
	logger      *slog.Logger
}

func (ms *msBidiImpl) Read(p []byte) (n int, err error) {
	n, err = ms.stream.Read(p)
	if err != nil && ms.connWrapper != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			var streamErr *quic.StreamError
			if !errors.As(err, &streamErr) || (streamErr.Remote == false) {
				ms.logger.Warn("Read error on bidi stream, invalidating connection", "stream_id", ms.stream.StreamID(), "error", err)
				ms.connWrapper.invalidateAndClear(fmt.Sprintf("read error on bidi stream %d: %v", ms.stream.StreamID(), err))
			}
		}
	}
	return n, err
}

func (ms *msBidiImpl) Write(p []byte) (n int, err error) {
	n, err = ms.stream.Write(p)
	if err != nil && ms.connWrapper != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			var streamErr *quic.StreamError
			if !errors.As(err, &streamErr) || (streamErr.Remote == false) {
				ms.logger.Warn("Write error on bidi stream, invalidating connection", "stream_id", ms.stream.StreamID(), "error", err)
				ms.connWrapper.invalidateAndClear(fmt.Sprintf("write error on bidi stream %d: %v", ms.stream.StreamID(), err))
			}
		}
	}
	return n, err
}
func (ms *msBidiImpl) Close() error                          { return ms.stream.Close() }
func (ms *msBidiImpl) StreamID() quic.StreamID               { return ms.stream.StreamID() }
func (ms *msBidiImpl) Context() context.Context              { return ms.stream.Context() }
func (ms *msBidiImpl) SetReadDeadline(t time.Time) error     { return ms.stream.SetReadDeadline(t) }
func (ms *msBidiImpl) SetWriteDeadline(t time.Time) error    { return ms.stream.SetWriteDeadline(t) }
func (ms *msBidiImpl) SetDeadline(t time.Time) error         { return ms.stream.SetDeadline(t) }
func (ms *msBidiImpl) CancelRead(code quic.StreamErrorCode)  { ms.stream.CancelRead(code) }
func (ms *msBidiImpl) CancelWrite(code quic.StreamErrorCode) { ms.stream.CancelWrite(code) }

// msSendImpl implémente ManagedQuicSendStream
type msSendImpl struct {
	sendStream  quic.SendStream
	connWrapper *mqcImpl
	logger      *slog.Logger
}

func (ms *msSendImpl) Write(p []byte) (n int, err error) {
	n, err = ms.sendStream.Write(p)
	if err != nil && ms.connWrapper != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			var streamErr *quic.StreamError
			if !errors.As(err, &streamErr) || (streamErr.Remote == false) {
				ms.logger.Warn("Write error on send-only stream, invalidating connection", "stream_id", ms.sendStream.StreamID(), "error", err)
				ms.connWrapper.invalidateAndClear(fmt.Sprintf("write error on send-only stream %d: %v", ms.sendStream.StreamID(), err))
			}
		}
	}
	return n, err
}
func (ms *msSendImpl) Close() error                          { return ms.sendStream.Close() }
func (ms *msSendImpl) StreamID() quic.StreamID               { return ms.sendStream.StreamID() }
func (ms *msSendImpl) Context() context.Context              { return ms.sendStream.Context() }
func (ms *msSendImpl) SetWriteDeadline(t time.Time) error    { return ms.sendStream.SetWriteDeadline(t) }
func (ms *msSendImpl) CancelWrite(code quic.StreamErrorCode) { ms.sendStream.CancelWrite(code) }

// msRecvImpl implémente ManagedQuicReceiveStream
type msRecvImpl struct {
	receiveStream quic.ReceiveStream
	connWrapper   *mqcImpl
	logger        *slog.Logger
}

func (ms *msRecvImpl) Read(p []byte) (n int, err error) {
	n, err = ms.receiveStream.Read(p)
	if err != nil && ms.connWrapper != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			var streamErr *quic.StreamError
			if !errors.As(err, &streamErr) || (streamErr.Remote == false) {
				ms.logger.Warn("Read error on recv-only stream, invalidating connection", "stream_id", ms.receiveStream.StreamID(), "error", err)
				ms.connWrapper.invalidateAndClear(fmt.Sprintf("read error on recv-only stream %d: %v", ms.receiveStream.StreamID(), err))
			}
		}
	}
	return n, err
}

func (ms *msRecvImpl) StreamID() quic.StreamID              { return ms.receiveStream.StreamID() }
func (ms *msRecvImpl) SetReadDeadline(t time.Time) error    { return ms.receiveStream.SetReadDeadline(t) }
func (ms *msRecvImpl) CancelRead(code quic.StreamErrorCode) { ms.receiveStream.CancelRead(code) }
