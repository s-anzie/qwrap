package framing

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	// MaxMessageSize définit la taille maximale autorisée pour un message Protobuf (payload).
	// 10MB comme spécifié.
	MaxMessageSize = 10 * 1024 * 1024

	// readByteTimeout est un timeout court pour lire un seul octet (pour Uvarint).
	// Utile si le flux est très lent ou bloqué.
	readByteTimeout = 5 * time.Second
)

var (
	ErrMessageTooLarge    = errors.New("protobuf message exceeds maximum allowed size")
	ErrInvalidUvarint     = errors.New("malformed uvarint length prefix")
	ErrIncompleteMessage  = errors.New("incomplete message read (less data than specified by length prefix)")
	ErrBufferPoolDepleted = errors.New("buffer pool depleted (should not happen with proper sizing)")
	// ErrCorruptedMessage n'est pas directement levé ici, car la corruption de payload
	// serait détectée par proto.Unmarshal et retournerait une erreur de cette librairie.
)

// defaultBufferPoolSize est la taille du pool de buffers.
const defaultBufferPoolSize = 128 // Nombre de buffers à garder en pool
// defaultBufferSize est la taille initiale des buffers. Ils peuvent grandir.
const defaultInitialBufferSize = 4 * 1024 // 4KB

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, defaultInitialBufferSize)
		return &b
	},
}

func GetBuffer() *[]byte {
	bufPtr := bufferPool.Get().(*[]byte)
	// Réinitialiser la longueur, mais garder la capacité
	*bufPtr = (*bufPtr)[:0]
	return bufPtr
}

func PutBuffer(bufPtr *[]byte) {
	// Optionnel: vérifier si le buffer a trop grandi avant de le remettre dans le pool
	// pour éviter de garder en mémoire des buffers géants inutilement.
	// if cap(*bufPtr) > someSensibleMaxSizeForPool {
	//    *bufPtr = make([]byte, 0, defaultInitialBufferSize) // Recréer avec taille par défaut
	// }
	bufferPool.Put(bufPtr)
}

// --- Implémentation du Writer ---

type messageWriter struct {
	w      io.Writer
	logger *slog.Logger
	// (Futur) Métriques
}

// NewMessageWriter crée un nouveau Writer pour écrire des messages Protobuf framés.
func NewMessageWriter(w io.Writer, logger *slog.Logger) Writer {
	if logger == nil {
		logger = slog.Default().With("component", "framing_writer")
	}
	return &messageWriter{w: w, logger: logger}
}

func (mw *messageWriter) WriteMsg(ctx context.Context, msg proto.Message) error {
	if msg == nil {
		return errors.New("cannot write nil message")
	}

	// 1. Sérialiser le message Protobuf
	// Utiliser un buffer du pool pour la sérialisation afin de réduire les allocations.
	protoDataBufPtr := GetBuffer()
	defer PutBuffer(protoDataBufPtr)

	// Utiliser proto.MarshalAppend pour écrire directement dans notre buffer.
	// Cela évite une allocation intermédiaire que proto.Marshal ferait.
	var err error
	*protoDataBufPtr, err = proto.MarshalOptions{}.MarshalAppend(*protoDataBufPtr, msg)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf message: %w", err)
	}
	protoData := *protoDataBufPtr

	msgLen := len(protoData)
	if msgLen == 0 {
		// Envoyer un message de longueur zéro est valide (Uvarint 0 puis rien).
		// Le reader doit pouvoir le gérer.
		mw.logger.Debug("Writing zero-length protobuf message")
	}
	if msgLen > MaxMessageSize {
		return fmt.Errorf("%w: message size %d, max %d", ErrMessageTooLarge, msgLen, MaxMessageSize)
	}

	// 2. Préparer le préfixe de longueur Uvarint
	// Un Uvarint pour 10MB (10 * 1024 * 1024) prendra au max 4 octets.
	// binary.MaxVarintLen64 (10 octets) est pour int64, pour uint64 (longueur) c'est 9 octets.
	// Pour 10MB = 10,485,760, le uvarint prend 4 octets.
	lenBuf := make([]byte, binary.MaxVarintLen32) // MaxVarintLen32 est 5 octets, suffisant pour MaxMessageSize
	n := binary.PutUvarint(lenBuf, uint64(msgLen))
	lenPrefix := lenBuf[:n]

	// Utiliser un io.Writer contextuel si possible, ou gérer le contexte manuellement.
	// Pour simplifier, on vérifie le contexte avant chaque écriture bloquante.
	// Une meilleure approche serait d'utiliser un writer qui respecte le contexte.

	// 3. Écrire le préfixe de longueur
	if err := mw.writeWithContext(ctx, lenPrefix); err != nil {
		return fmt.Errorf("failed to write length prefix: %w", err)
	}

	// 4. Écrire le message Protobuf lui-même
	if msgLen > 0 {
		if err := mw.writeWithContext(ctx, protoData); err != nil {
			return fmt.Errorf("failed to write protobuf data: %w", err)
		}
	}
	return nil
}

// writeWithContext est un helper pour écrire des données en respectant le contexte.
func (mw *messageWriter) writeWithContext(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// Tenter d'écrire en une seule fois.
	// Si le writer sous-jacent est un `net.Conn`, il pourrait y avoir des écritures partielles.
	// Boucler jusqu'à ce que tout soit écrit ou qu'une erreur/annulation se produise.
	totalWritten := 0
	for totalWritten < len(data) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("write cancelled: %w", ctx.Err())
		default:
			// Non-bloquant, on continue
		}

		// (Optionnel) Si le writer supporte SetWriteDeadline, l'utiliser avec le contexte.
		// if deadlineWriter, ok := mw.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
		// 	if dl, ok := ctx.Deadline(); ok {
		// 		deadlineWriter.SetWriteDeadline(dl)
		// 	} else {
		// 		deadlineWriter.SetWriteDeadline(time.Time{}) // No deadline
		// 	}
		// }

		n, err := mw.w.Write(data[totalWritten:])
		totalWritten += n
		if err != nil {
			// Vérifier si l'erreur est due à l'annulation du contexte (si le writer est sensible au contexte)
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return fmt.Errorf("write context error: %w", err)
			}
			return fmt.Errorf("write error after %d bytes: %w", totalWritten, err)
		}
	}
	return nil
}

// --- Implémentation du Reader ---

type messageReader struct {
	r      io.Reader // Utiliser bufio.Reader pour ReadByte et une lecture bufferisée
	logger *slog.Logger
	// (Futur) Métriques
	// (Futur) Buffer interne pour les lectures partielles de Uvarint si non bufio.Reader
}

// NewMessageReader crée un nouveau Reader pour lire des messages Protobuf framés.
// Il est recommandé que 'r' soit déjà un io.ByteReader (comme bufio.Reader) pour l'efficacité de ReadUvarint.
func NewMessageReader(r io.Reader, logger *slog.Logger) Reader {
	if logger == nil {
		logger = slog.Default().With("component", "framing_reader")
	}
	// S'assurer que nous avons un io.ByteReader pour binary.ReadUvarint
	br, ok := r.(io.ByteReader)
	if !ok {
		logger.Debug("Input reader is not an io.ByteReader, wrapping with bufio.Reader")
		br = bufio.NewReader(r) // r est maintenant ce bufio.Reader
		r = br.(io.Reader)      // Mettre à jour r pour être le bufio.Reader pour io.ReadFull
	}

	return &messageReader{r: r, logger: logger}
}

func (mr *messageReader) ReadMsg(ctx context.Context, msg proto.Message) error {
	if msg == nil {
		return errors.New("cannot read into nil message")
	}

	// 1. Lire la longueur Uvarint du message
	// binary.ReadUvarint attend un io.ByteReader.
	var msgLen uint64
	var err error

	// Gérer le contexte pour ReadUvarint qui peut bloquer sur chaque ReadByte.
	// On ne peut pas facilement passer un contexte à binary.ReadUvarint.
	// On utilise une goroutine et un select avec timeout/contexte.
	// Si le reader sous-jacent est un `net.Conn`, on pourrait mettre un ReadDeadline.

	// Si r est un net.Conn, on peut utiliser SetReadDeadline
	if conn, ok := mr.r.(interface{ SetReadDeadline(time.Time) error }); ok {
		deadline := time.Now().Add(readByteTimeout * time.Duration(binary.MaxVarintLen32+1)) // Timeout global pour lire le uvarint
		if dl, ctxHasDeadline := ctx.Deadline(); ctxHasDeadline && dl.Before(deadline) {
			deadline = dl
		}
		conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{}) // Effacer la deadline
	}
	// Note: ce timeout s'applique à l'ensemble de la lecture du uvarint.
	// ReadUvarint peut faire plusieurs appels à ReadByte.

	byteReader, ok := mr.r.(io.ByteReader)
	if !ok {
		// Cela ne devrait pas arriver si NewMessageReader a bien wrappé.
		return errors.New("internal error: reader is not an io.ByteReader")
	}

	msgLen, err = binary.ReadUvarint(byteReader)

	if err != nil {
		// Vérifier si l'erreur est due à l'annulation du contexte (si le reader est sensible au contexte)
		if ctx.Err() != nil && (errors.Is(err, ctx.Err()) || errors.Is(err, io.EOF)) { // EOF peut être un signe d'annulation
			return fmt.Errorf("read length cancelled: %w", ctx.Err())
		}
		if errors.Is(err, io.EOF) { // EOF avant de lire un Uvarint complet
			return io.EOF // Propager EOF si le flux est terminé proprement
		}
		return fmt.Errorf("%w: %w", ErrInvalidUvarint, err)
	}

	// 2. Valider la longueur
	if msgLen > MaxMessageSize {
		// Consommer les octets du message pour ne pas désynchroniser le flux si possible,
		// mais avec une limite pour éviter de lire indéfiniment un message corrompu.
		// (Optionnel, peut être dangereux si la longueur annoncée est malicieusement énorme)
		// io.CopyN(io.Discard, mr.r, int64(msgLen))
		return fmt.Errorf("%w: message claims size %d, max %d", ErrMessageTooLarge, msgLen, MaxMessageSize)
	}

	if msgLen == 0 {
		// Message vide valide, rien à lire de plus.
		// Désérialiser un message vide dans `msg` (le mettra à son état par défaut).
		proto.Reset(msg)
		mr.logger.Debug("Read zero-length protobuf message")
		return nil
	}

	// 3. Lire le payload du message
	// Utiliser un buffer du pool pour la lecture.
	payloadBufPtr := GetBuffer()
	defer PutBuffer(payloadBufPtr)
	payloadBuf := *payloadBufPtr

	// S'assurer que le buffer a assez de capacité.
	if cap(payloadBuf) < int(msgLen) {
		payloadBuf = make([]byte, msgLen)
	} else {
		payloadBuf = payloadBuf[:msgLen]
	}

	// io.ReadFull lit exactement len(buf) octets ou retourne une erreur.
	// Gérer le contexte pour io.ReadFull.

	// Si r est un net.Conn, on peut utiliser SetReadDeadline
	if conn, ok := mr.r.(interface{ SetReadDeadline(time.Time) error }); ok {
		deadline := time.Now().Add(readByteTimeout) // Timeout pour la lecture du payload
		if dl, ctxHasDeadline := ctx.Deadline(); ctxHasDeadline && dl.Before(deadline) {
			deadline = dl
		}
		conn.SetReadDeadline(deadline)
		// Le defer de SetReadDeadline(time.Time{}) est déjà en place
	}

	n, err := io.ReadFull(mr.r, payloadBuf)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) { // EOF ou UnexpectedEOF avant la fin du message annoncé
			return fmt.Errorf("%w: expected %d bytes, got %d: %w", ErrIncompleteMessage, msgLen, n, err)
		}
		// Vérifier si l'erreur est due à l'annulation du contexte
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return fmt.Errorf("read payload cancelled: %w", ctx.Err())
		}
		return fmt.Errorf("failed to read message payload (expected %d bytes): %w", msgLen, err)
	}
	if uint64(n) != msgLen {
		// Ne devrait pas arriver avec io.ReadFull sans erreur, mais par sécurité.
		return fmt.Errorf("%w: read %d bytes, but length prefix indicated %d", ErrIncompleteMessage, n, msgLen)
	}

	// 4. Désérialiser le message Protobuf
	if err := proto.Unmarshal(payloadBuf[:msgLen], msg); err != nil {
		// L'erreur ici est probablement ErrCorruptedMessage (ou un type d'erreur de la lib proto)
		return fmt.Errorf("failed to unmarshal protobuf message: %w", err)
	}

	// (Optionnel) Validation UTF-8 si on sait que c'est un message texte
	// Cette validation est spécifique à l'application. Le framing général ne le fait pas.
	// Si vous avez des champs string dans vos protos, la lib proto gère l'UTF-8.

	return nil
}
