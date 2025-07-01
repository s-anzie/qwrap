package kuik

import "time"

// Config contains the configuration for a single QUIC connection.
// This will be used to pass qwrap-specific parameters like FileID in a way
// that is abstracted from the main client logic.
type Config struct {
	// HandshakeIdleTimeout is the idle timeout before the handshake fails.
	HandshakeIdleTimeout time.Duration
	// MaxIdleTimeout is the maximum idle timeout.
	MaxIdleTimeout time.Duration

	// QwrapFileID is a custom parameter to specify which file to download.
	// This is part of the "illusion" - passing implementation-specific details
	// through the abstracted configuration.
	QwrapFileID string

	// QwrapDestPath is the destination path for the downloaded file.
	QwrapDestPath string

	// QwrapFileSize is the size of the file to be downloaded.
	QwrapFileSize int64

	// QwrapConcurrency is the number of concurrent connections to use.
	QwrapConcurrency int
}
