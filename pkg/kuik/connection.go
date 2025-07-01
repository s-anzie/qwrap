package kuik

import (
	"context"
	"net"
)

// ApplicationErrorCode is a generic application error code, mirroring quic-go.
type ApplicationErrorCode uint64

// Connection is an interface that mimics the quic-go Connection interface.
// It represents a single qwrap file transfer session.
type Connection interface {
	// OpenStreamSync opens a new bidirectional QUIC stream. It blocks until the stream can be opened.
	OpenStreamSync(context.Context) (Stream, error)

	// CloseWithError closes the connection with an error.
	// The error string is optional and may be omitted.
	CloseWithError(ApplicationErrorCode, string) error

	// LocalAddr returns the local address.
	LocalAddr() net.Addr

	// RemoteAddr returns the address of the peer.
	RemoteAddr() net.Addr

	// Context returns a context that is canceled when the connection is closed.
	Context() context.Context
}
