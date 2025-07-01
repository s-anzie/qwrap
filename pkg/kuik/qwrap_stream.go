package kuik

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"qwrap/internal/client/downloader"
)

// qwrapStream is the concrete implementation of the Stream interface.
// It wraps the progress channel of the qwrap downloader to simulate a data stream.
type qwrapStream struct {
	progressChan <-chan downloader.ProgressInfo
	errorChan    <-chan error
	destWriter   io.WriteCloser
	readBuffer   []byte // Buffer for handling partial reads
}

// Read will block and provide progress updates as if they were data from a stream.
// Each progress update is a JSON object followed by a newline.
func (s *qwrapStream) Read(p []byte) (n int, err error) {
	// If there's data in our buffer, serve it first.
	if len(s.readBuffer) > 0 {
		n = copy(p, s.readBuffer)
		s.readBuffer = s.readBuffer[n:]
		return n, nil
	}

	// Buffer is empty, get the next progress update.
	progress, ok := <-s.progressChan
	if !ok {
		// Progress channel is closed, which means the download has finished.
		// Check the error channel for the final status.
		finalErr := <-s.errorChan
		if finalErr != nil {
			return 0, finalErr
		}
		return 0, io.EOF // Clean finish
	}

	// We have a new progress update. Marshal it to JSON.
	progressBytes, jsonErr := json.Marshal(progress)
	if jsonErr != nil {
		return 0, fmt.Errorf("failed to serialize progress info: %w", jsonErr)
	}

	// Append a newline to act as a message delimiter and store it in our buffer.
	s.readBuffer = append(progressBytes, '\n')

	// Now that the buffer is populated, copy data to the caller's slice.
	n = copy(p, s.readBuffer)
	s.readBuffer = s.readBuffer[n:]
	return n, nil
}

// Write is a no-op for a download stream.
func (s *qwrapStream) Write(p []byte) (n int, err error) {
	return len(p), nil // Pretend we wrote the data
}

// Close will close the underlying destination file writer.
func (s *qwrapStream) Close() error {
	return s.destWriter.Close()
}

// StreamID returns a dummy stream ID.
func (s *qwrapStream) StreamID() int64 {
	return 1
}

// SetReadDeadline is a no-op.
func (s *qwrapStream) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline is a no-op.
func (s *qwrapStream) SetWriteDeadline(t time.Time) error {
	return nil
}
