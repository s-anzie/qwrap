package downloader

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

// fileWriter is a dedicated, self-contained goroutine for writing chunks to disk.
// It receives completed chunks via a channel and manages its own internal buffer
// to write them to the destination file in the correct order. This design completely
// decouples disk I/O from the main downloader loop, preventing deadlocks.
func (d *downloaderImpl) fileWriter(
	ctx context.Context,
	wg *sync.WaitGroup,
	destFile *os.File,
	writeChan <-chan chunkToWrite,
	fileID string,
) {
	defer wg.Done()
	l := d.config.Logger.With("component", "fileWriter", "file_id", fileID)
	l.Info("File writer goroutine started.")

	// Internal buffer for out-of-order chunks.
	// Key: chunkID, Value: chunk data
	pendingChunks := make(map[uint64]chunkToWrite)
	var nextChunkToWriteID uint64 = 0

	for {
		select {
		case chunk, ok := <-writeChan:
			if !ok {
				// Channel is closed. This is the signal that the download is complete.
				// We attempt to write any remaining chunks from our buffer.
				l.Info("Write channel closed. Writing any remaining buffered chunks.")
				d.flushBuffer(destFile, pendingChunks, &nextChunkToWriteID, l)
				l.Info("File writer goroutine finished.")
				return
			}

			// A new chunk has arrived. Add it to our pending buffer.
			pendingChunks[chunk.chunkID] = chunk
			l.Debug("Received and buffered chunk", "chunk_id", chunk.chunkID)

			// Try to write any sequential chunks we now have.
			d.flushBuffer(destFile, pendingChunks, &nextChunkToWriteID, l)

		case <-ctx.Done():
			l.Info("Context cancelled, file writer shutting down.")
			// Attempt a final flush on cancellation.
			d.flushBuffer(destFile, pendingChunks, &nextChunkToWriteID, l)
			return
		}
	}
}

// flushBuffer attempts to write contiguous chunks from the buffer to the disk.
func (d *downloaderImpl) flushBuffer(
	destFile *os.File,
	pendingChunks map[uint64]chunkToWrite,
	nextChunkToWriteID *uint64,
	l *slog.Logger,
) {
	for {
		// Check if the next sequential chunk is in our buffer.
		chunk, exists := pendingChunks[*nextChunkToWriteID]
		if !exists {
			// The next required chunk hasn't arrived yet. We have to wait.
			break
		}

		// We have the next chunk, let's write it.
		_, err := destFile.WriteAt(chunk.data, chunk.offset)
		if err != nil {
			// This is a critical failure. The download cannot succeed.
			// In a real-world scenario, we might want to signal this failure back to the main loop.
			// For now, we log it as a fatal error. The main loop will eventually time out or finish.
			l.Error("CRITICAL: Failed to write chunk to file. Aborting writer.", "chunk_id", chunk.chunkID, "offset", chunk.offset, "error", err)
			// We clear the buffer to prevent further writes on a corrupted file.
			clear(pendingChunks)
			return
		}

			l.Debug("Successfully wrote chunk to disk", "chunk_id", chunk.chunkID, "offset", chunk.offset, "size", len(chunk.data))

		// The chunk has been written, so we can remove it from the buffer.
			delete(pendingChunks, *nextChunkToWriteID)

		// And increment the counter to look for the next one.
			*nextChunkToWriteID++
	}
}
