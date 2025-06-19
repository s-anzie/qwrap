package downloader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

type sequentialFileWriter struct {
	jobsInChan <-chan fileWriterJob
	dest       DestinationWriter
	logger     *slog.Logger

	// Interne pour l'ordonnancement
	mu                  sync.Mutex
	pendingWrites       map[uint64]fileWriterJob // Buffer pour les chunks reçus dans le désordre
	nextExpectedChunkID uint64
	errorChan           chan error // Pour signaler une erreur fatale d'écriture
}

func newSequentialFileWriter(
	jobsIn <-chan fileWriterJob,
	dest DestinationWriter,
	logger *slog.Logger,
) *sequentialFileWriter {
	return &sequentialFileWriter{
		jobsInChan:    jobsIn,
		dest:          dest,
		logger:        logger,
		pendingWrites: make(map[uint64]fileWriterJob),
		errorChan:     make(chan error, 1), // Buffer 1 pour ne pas bloquer l'envoi
	}
}

func (fw *sequentialFileWriter) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	fw.logger.Info("SequentialFileWriter starting...")
	defer fw.logger.Info("SequentialFileWriter stopped.")

	for {
		select {
		case <-ctx.Done():
			fw.logger.Info("Context cancelled, SequentialFileWriter shutting down.")
			// Tenter d'écrire les chunks restants en buffer si le contexte le permet encore un peu
			// Ou simplement abandonner si c'est une annulation dure.
			// Pour l'instant, on abandonne.
			return
		case job, ok := <-fw.jobsInChan:
			if !ok { // Channel fermé
				fw.logger.Info("Jobs channel closed. Writing any remaining buffered chunks.")
				// Tenter d'écrire ce qui reste dans pendingWrites
				fw.writePendingChunks(true) // Force write
				if len(fw.pendingWrites) > 0 {
					fw.logger.Error("SequentialFileWriter finished but some chunks remain unwritten in buffer", "count", len(fw.pendingWrites))
					// Signaler une erreur si des chunks n'ont pas pu être écrits.
					select {
					case fw.errorChan <- ErrFileWriteFailed: // Non-bloquant
					default:
					}
				}
				return
			}

			fw.mu.Lock()
			if job.chunkID == fw.nextExpectedChunkID {
				fw.logger.Debug("Received expected chunk for writing", "chunk_id", job.chunkID)
				if err := fw.writeChunk(job); err != nil {
					fw.logger.Error("Fatal: Failed to write chunk to disk", "chunk_id", job.chunkID, "error", err)
					select {
					case fw.errorChan <- err: // Signaler l'erreur fatale
					default: // Éviter de bloquer si errorChan est déjà plein
					}
					fw.mu.Unlock()
					return // Arrêter le writer en cas d'erreur d'écriture
				}
				fw.nextExpectedChunkID++
				// Tenter d'écrire les chunks suivants qui étaient en attente
				fw.writePendingChunks(false)
			} else if job.chunkID > fw.nextExpectedChunkID {
				fw.logger.Debug("Received out-of-order chunk, buffering", "chunk_id", job.chunkID, "expected_chunk_id", fw.nextExpectedChunkID)
				fw.pendingWrites[job.chunkID] = job
				// Gestion de la taille du buffer pour éviter OOM
				if len(fw.pendingWrites)*int(job.data[0]) /* approximation of chunk size */ > MaxTempChunkBufferSize { // TODO: Better size tracking
					fw.logger.Error("Pending write buffer exceeded max size, potential stall or OOM", "num_pending", len(fw.pendingWrites))
					// Stratégie: abandonner ou signaler erreur fatale
					select {
					case fw.errorChan <- errors.New("pending write buffer overflow"):
					default:
					}
					fw.mu.Unlock()
					return
				}

			} else { // chunkID < fw.nextExpectedChunkID
				fw.logger.Warn("Received already written or stale chunk, ignoring", "chunk_id", job.chunkID, "expected_chunk_id", fw.nextExpectedChunkID)
			}
			fw.mu.Unlock()
		}
	}
}

// writePendingChunks tente d'écrire les chunks en attente séquentiellement.
// Doit être appelé avec fw.mu détenu.
func (fw *sequentialFileWriter) writePendingChunks(forceAll bool) {
	for {
		job, exists := fw.pendingWrites[fw.nextExpectedChunkID]
		if !exists {
			break // Le prochain chunk attendu n'est pas dans le buffer
		}

		fw.logger.Debug("Writing buffered chunk", "chunk_id", job.chunkID)
		if err := fw.writeChunk(job); err != nil {
			fw.logger.Error("Fatal: Failed to write buffered chunk to disk", "chunk_id", job.chunkID, "error", err)
			select {
			case fw.errorChan <- err:
			default:
			}
			// En cas d'erreur ici, il est difficile de récupérer. On arrête.
			return
		}
		delete(fw.pendingWrites, fw.nextExpectedChunkID)
		fw.nextExpectedChunkID++

		if !forceAll && len(fw.jobsInChan) == 0 && len(fw.pendingWrites) > 20 { // Heuristique pour ne pas bloquer trop longtemps
			fw.logger.Debug("Pausing pending chunk write to allow more chunks to arrive.")
			break
		}
	}
}

// writeChunk effectue l'écriture physique.
// Doit être appelé avec fw.mu détenu si des états partagés sont modifiés (ici, juste pour la cohérence du log).
func (fw *sequentialFileWriter) writeChunk(job fileWriterJob) error {
	// Simuler un délai pour tester la backpressure, si nécessaire
	// time.Sleep(50 * time.Millisecond)
	_, err := fw.dest.WriteAt(job.data, job.offset)
	if err != nil {
		return fmt.Errorf("writing chunk %d at offset %d: %w", job.chunkID, job.offset, err)
	}
	fw.logger.Debug("Chunk written to disk", "chunk_id", job.chunkID, "offset", job.offset, "size", len(job.data))
	return nil
}
