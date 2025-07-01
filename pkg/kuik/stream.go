package kuik

import (
	"io"
	"time"
)

// Stream is an interface that mimics the quic-go Stream interface.
// For qwrap, this will represent the flow of data for a single file download.
type Stream interface {
	// Read reads data from the stream.
	io.Reader
	// Write writes data to the stream.
	io.Writer
	// Closer closes the stream.
	io.Closer

	// StreamID returns the stream ID.
	StreamID() int64

	// SetReadDeadline sets the deadline for future Read calls.
	SetReadDeadline(t time.Time) error
	// SetWriteDeadline sets the deadline for future Write calls.
	SetWriteDeadline(t time.Time) error
}
