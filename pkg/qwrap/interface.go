package qwrap

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// ManagedQuicConnection (inchangée, mais ses méthodes retourneront des types de flux plus spécifiques)
type ManagedQuicConnection interface {
	OpenStream() (ManagedQuicBidirectionalStream, error)
	OpenStreamSync(ctx context.Context) (ManagedQuicBidirectionalStream, error)
	OpenUniStream() (ManagedQuicSendStream, error)
	OpenUniStreamSync(ctx context.Context) (ManagedQuicSendStream, error)
	AcceptStream(ctx context.Context) (ManagedQuicBidirectionalStream, error)
	AcceptUniStream(ctx context.Context) (ManagedQuicReceiveStream, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
	Context() context.Context
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	ConnectionState() quic.ConnectionState
}

// ManagedQuicBidirectionalStream interface pour les flux bidirectionnels.
type ManagedQuicBidirectionalStream interface {
	io.ReadWriteCloser
	StreamID() quic.StreamID
	Context() context.Context
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetDeadline(t time.Time) error
	CancelRead(code quic.StreamErrorCode)
	CancelWrite(code quic.StreamErrorCode)
}

// ManagedQuicSendStream interface pour les flux unidirectionnels d'envoi.
type ManagedQuicSendStream interface {
	io.WriteCloser
	StreamID() quic.StreamID
	Context() context.Context
	SetWriteDeadline(t time.Time) error
	CancelWrite(code quic.StreamErrorCode)
}

// ManagedQuicReceiveStream interface pour les flux unidirectionnels de réception.
type ManagedQuicReceiveStream interface {
	io.Reader // Seulement Read
	StreamID() quic.StreamID
	SetReadDeadline(t time.Time) error
	CancelRead(code quic.StreamErrorCode)
}

// ConnectionManagerProvider (inchangée)
type ConnectionManagerProvider interface {
	GetOrConnect(ctx context.Context, addr string) (quic.Connection, error)
	Invalidate(ctx context.Context, addr string)
}
