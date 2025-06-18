package downloader

import (
	"log/slog"
	"os"
)

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
