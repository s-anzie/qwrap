package connectionmanager // Ou connection_manager selon vos conventions de nommage de package

import (
	"container/list"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	defaultDialTimeout          = 5 * time.Second
	defaultHandshakeIdleTimeout = 10 * time.Second
	defaultMaxIdleTimeout       = 30 * time.Second
	defaultMaxRetries           = 3
	defaultRetryBaseDelay       = 500 * time.Millisecond
	defaultMaxConnections       = 64 // Taille par défaut du cache LRU
	alpnProtocol                = "qwrap"
	closeErrorCodeNoError       = 0 // Application level error code for graceful close
	closeReasonManagerShutdown  = "connection manager shutdown"
	closeReasonInvalidated      = "connection invalidated by client"
)

var (
	ErrMaxRetriesExceeded = errors.New("exceeded maximum dial retries")
	ErrManagerClosed      = errors.New("connection manager is closed")
)

// Config contient la configuration pour le ConnectionManager.
type Config struct {
	MaxConnections       int           // Nombre maximum de connexions à maintenir dans le cache LRU.
	DialTimeout          time.Duration // Timeout pour l'établissement initial de la connexion.
	HandshakeIdleTimeout time.Duration // Timeout pour le handshake TLS QUIC.
	MaxIdleTimeout       time.Duration // Timeout d'inactivité pour une connexion QUIC.
	MaxRetries           int           // Nombre maximum de tentatives de dial.
	RetryBaseDelay       time.Duration // Délai de base pour le backoff exponentiel des tentatives.
	TLSClientConfig      *tls.Config   // Configuration TLS personnalisée (optionnel).
	Logger               *slog.Logger
}

func (c *Config) setDefaults() {
	if c.MaxConnections <= 0 {
		c.MaxConnections = defaultMaxConnections
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.HandshakeIdleTimeout <= 0 {
		c.HandshakeIdleTimeout = defaultHandshakeIdleTimeout
	}
	if c.MaxIdleTimeout <= 0 {
		c.MaxIdleTimeout = defaultMaxIdleTimeout
	}
	if c.MaxRetries < 0 { // 0 signifie une seule tentative (pas de retry)
		c.MaxRetries = defaultMaxRetries
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = defaultRetryBaseDelay
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type managedConnection struct {
	conn     quic.Connection
	lruEntry *list.Element // Pointeur vers son entrée dans lruList pour suppression rapide
}

// connManagerImpl implémente l'interface ConnectionManager.
type connManagerImpl struct {
	mu          sync.RWMutex
	connections map[string]*managedConnection // Clé: addr
	lruList     *list.List                    // Liste pour la politique LRU (les plus récents en tête)
	config      Config
	closed      bool
}

// NewConnectionManager crée une nouvelle instance de ConnectionManager.
func NewConnectionManager(config Config) ConnectionManager {
	config.setDefaults()
	return &connManagerImpl{
		connections: make(map[string]*managedConnection),
		lruList:     list.New(),
		config:      config,
	}
}

// GetOrConnect récupère une connexion QUIC existante et active pour l'adresse donnée
// ou en établit une nouvelle.
func (cm *connManagerImpl) GetOrConnect(ctx context.Context, addr string) (quic.Connection, error) {
	cm.mu.RLock()
	if cm.closed {
		cm.mu.RUnlock()
		return nil, ErrManagerClosed
	}

	if mc, ok := cm.connections[addr]; ok {
		// La connexion existe, vérifions si elle est toujours active.
		// Ouvrir un flux test est une manière de vérifier.
		// quic.Connection.Context().Err() peut aussi indiquer si la connexion est fermée.
		if mc.conn.Context().Err() == nil {
			// Tentative d'ouvrir un flux bidirectionnel pour tester la connexion
			// Le contexte pour OpenStreamSync doit être court et distinct du contexte principal
			// pour ne pas bloquer indéfiniment si la connexion est "zombie".
			openCtx, openCancel := context.WithTimeout(ctx, cm.config.DialTimeout/2) // Un timeout plus court pour le test
			stream, err := mc.conn.OpenStreamSync(openCtx)
			openCancel()

			if err == nil {
				// Fermer immédiatement le flux test.
				// On ne veut pas que ce flux interfère.
				_ = stream.Close() // Ignorer l'erreur de fermeture du flux test

				// Déplacer l'élément au début de la liste LRU
				cm.mu.RUnlock() // Libérer le RLock avant de prendre le Lock complet
				cm.mu.Lock()
				if !cm.closed { // Revérifier cm.closed après avoir obtenu le Lock complet
					cm.lruList.MoveToFront(mc.lruEntry)
				}
				cm.mu.Unlock()
				cm.config.Logger.Debug("Reusing existing QUIC connection", "address", addr)
				return mc.conn, nil
			}
			cm.config.Logger.Warn("Existing connection seems dead, attempting to remove", "address", addr, "error", err)
			// La connexion est morte, nous allons la supprimer après avoir relâché RLock
		} else {
			cm.config.Logger.Warn("Existing connection context shows error, attempting to remove", "address", addr, "error", mc.conn.Context().Err())
		}
		// La connexion est dans la map mais morte, il faut la supprimer.
		// Nous le ferons en dehors du RLock pour éviter un deadlock si dialAndStore est appelé.
		cm.mu.RUnlock()
		cm.removeConnection(addr, nil) // mc.conn peut déjà être fermé ou dans un état indéfini
		// Continuer pour essayer de composer une nouvelle connexion
	} else {
		cm.mu.RUnlock()
	}

	// Aucune connexion existante ou la connexion existante était morte. Tentative de Dial.
	// Le dialAndStore prend son propre Lock complet.
	return cm.dialAndStore(ctx, addr)
}

func (cm *connManagerImpl) dialAndStore(ctx context.Context, addr string) (quic.Connection, error) {
	cm.mu.Lock() // Verrouillage complet car nous allons potentiellement modifier la map et la liste LRU
	defer cm.mu.Unlock()

	if cm.closed {
		return nil, ErrManagerClosed
	}

	// Double-vérification: une autre goroutine a-t-elle établi la connexion pendant que nous attendions le verrou ?
	if mc, ok := cm.connections[addr]; ok {
		if mc.conn.Context().Err() == nil {
			cm.lruList.MoveToFront(mc.lruEntry)
			cm.config.Logger.Debug("Connection established by another goroutine", "address", addr)
			return mc.conn, nil
		}
		// La connexion établie par une autre goroutine est déjà morte, la supprimer.
		cm.config.Logger.Warn("Stale connection found during double-check, removing", "address", addr)
		cm.evictElement(mc.lruEntry, mc.conn, "stale connection during dial")
	}

	// Configuration TLS
	tlsConf := cm.config.TLSClientConfig
	if tlsConf == nil {
		tlsConf = &tls.Config{
			InsecureSkipVerify: false, // NE PAS METTRE À TRUE EN PRODUCTION SANS RAISON VALABLE
			// RootCAs: // Charger les CAs de confiance si nécessaire
		}
	}
	// S'assurer que NextProtos est bien défini pour qwrap
	// On fait une copie pour ne pas modifier la config partagée si elle existe
	tlsConfCopy := tlsConf.Clone()
	tlsConfCopy.NextProtos = []string{alpnProtocol}

	// Configuration QUIC
	quicConf := &quic.Config{
		MaxIdleTimeout:       cm.config.MaxIdleTimeout,
		HandshakeIdleTimeout: cm.config.HandshakeIdleTimeout,
		// KeepAlivePeriod: // peut être utile pour maintenir les connexions actives à travers les NATs
	}

	var conn quic.Connection
	var lastErr error

	for i := 0; i <= cm.config.MaxRetries; i++ {
		dialCtx, dialCancel := context.WithTimeout(ctx, cm.config.DialTimeout)
		cm.config.Logger.Debug("Attempting to dial QUIC connection", "address", addr, "attempt", i+1)

		var err error
		conn, err = quic.DialAddr(dialCtx, addr, tlsConfCopy, quicConf)
		dialCancel() // Libérer les ressources du contexte de dial

		if err == nil {
			cm.config.Logger.Info("Successfully established QUIC connection", "address", addr)
			lastErr = nil
			break
		}
		lastErr = err
		cm.config.Logger.Warn("Failed to dial QUIC connection", "address", addr, "attempt", i+1, "error", err)

		if i < cm.config.MaxRetries {
			// Vérifier si le contexte principal est annulé avant d'attendre
			if ctx.Err() != nil {
				cm.config.Logger.Info("Context cancelled during dial retries", "address", addr, "error", ctx.Err())
				return nil, fmt.Errorf("dial context cancelled: %w", ctx.Err())
			}
			delay := cm.config.RetryBaseDelay * (1 << i) // Backoff exponentiel
			cm.config.Logger.Debug("Waiting before next dial attempt", "address", addr, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				cm.config.Logger.Info("Context cancelled during retry delay", "address", addr, "error", ctx.Err())
				return nil, fmt.Errorf("dial context cancelled during retry: %w", ctx.Err())
			}
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("%w to %s: %w", ErrMaxRetriesExceeded, addr, lastErr)
	}

	// Éviction LRU si nécessaire
	if cm.lruList.Len() >= cm.config.MaxConnections {
		lruElement := cm.lruList.Back()
		if lruElement != nil {
			addrToEvict := lruElement.Value.(string)
			cm.config.Logger.Info("Max connections reached, evicting LRU connection", "address", addrToEvict)
			if mcToEvict, ok := cm.connections[addrToEvict]; ok {
				cm.evictElement(mcToEvict.lruEntry, mcToEvict.conn, "LRU eviction")
			}
		}
	}

	// Stocker la nouvelle connexion
	element := cm.lruList.PushFront(addr)
	mc := &managedConnection{
		conn:     conn,
		lruEntry: element,
	}
	cm.connections[addr] = mc

	return conn, nil
}

// Invalidate ferme la connexion à l'adresse spécifiée et la supprime du pool.
func (cm *connManagerImpl) Invalidate(ctx context.Context, addr string) {
	cm.config.Logger.Info("Invalidating connection", "address", addr)
	cm.removeConnection(addr, &quic.ApplicationError{
		ErrorCode:    closeErrorCodeNoError,
		ErrorMessage: closeReasonInvalidated,
	})
}

// removeConnection est une aide interne pour retirer une connexion.
// Prend un Lock complet.
func (cm *connManagerImpl) removeConnection(addr string, appErr *quic.ApplicationError) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return
	}

	if mc, ok := cm.connections[addr]; ok {
		reason := "unknown reason for removal"
		if appErr != nil {
			reason = appErr.ErrorMessage
		}
		cm.evictElement(mc.lruEntry, mc.conn, reason)
	}
}

// evictElement gère la logique de suppression d'un élément de la map et de la liste LRU,
// et ferme la connexion. DOIT être appelée sous un Lock complet.
func (cm *connManagerImpl) evictElement(element *list.Element, conn quic.Connection, reason string) {
	if element == nil || conn == nil {
		return
	}
	addr := element.Value.(string) // L'élément de la liste stocke l'adresse

	cm.lruList.Remove(element)
	delete(cm.connections, addr)

	cm.config.Logger.Debug("Closing and evicting connection", "address", addr, "reason", reason)
	// Fermer la connexion QUIC. Il est important de gérer l'erreur, même si souvent on ne peut rien y faire.
	// Utiliser un appErr si fourni, sinon une fermeture générique.
	closeReasonMsg := fmt.Sprintf("evicted: %s", reason)

	// Utiliser un contexte avec timeout pour la fermeture pour éviter un blocage infini
	_, closeCancel := context.WithTimeout(context.Background(), cm.config.DialTimeout/2)
	defer closeCancel()

	err := conn.CloseWithError(
		quic.ApplicationErrorCode(closeErrorCodeNoError),
		closeReasonMsg)
	if err != nil {
		// Une erreur lors de la fermeture peut se produire si la connexion est déjà dans un mauvais état.
		// On logue l'erreur mais on continue l'éviction.
		cm.config.Logger.Warn("Error closing evicted QUIC connection", "address", addr, "error", err, "reason", closeReasonMsg)
	}
}

// CloseAll ferme toutes les connexions actives gérées par le manager.
func (cm *connManagerImpl) CloseAll() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return ErrManagerClosed
	}
	cm.closed = true

	cm.config.Logger.Info("Closing all managed QUIC connections")
	var SlogAttr []slog.Attr
	for addr, mc := range cm.connections {
		// Utiliser un contexte avec timeout pour chaque fermeture
		_, closeCancel := context.WithTimeout(context.Background(), cm.config.DialTimeout/2)

		err := mc.conn.CloseWithError(
			quic.ApplicationErrorCode(closeErrorCodeNoError),
			closeReasonManagerShutdown)
		if err != nil {
			cm.config.Logger.Warn("Error closing QUIC connection during CloseAll", "address", addr, "error", err)
			SlogAttr = append(SlogAttr, slog.String("failed_close_"+addr, err.Error()))
		}
		closeCancel()
	}

	cm.connections = make(map[string]*managedConnection)
	cm.lruList.Init() // Réinitialise la liste

	if len(SlogAttr) > 0 {
		return fmt.Errorf("errors occurred while closing some connections: %v", SlogAttr)
	}
	return nil
}

// (Optionnel) Méthodes pour obtenir des métriques (à implémenter si besoin)
// func (cm *connManagerImpl) GetMetrics() map[string]ConnectionMetrics { ... }
// type ConnectionMetrics struct {
//    Latency time.Duration
//    OpenStreams int
//    // ...
// }
