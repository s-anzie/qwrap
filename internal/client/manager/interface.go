package manager

import (
	"context"

	"github.com/quic-go/quic-go"
)

// ConnectionManager gère un pool de connexions QUIC réutilisables vers les agents.
type ConnectionManager interface {
	// GetOrConnect récupère une connexion QUIC existante et active pour l'adresse donnée
	// ou en établit une nouvelle. Elle gère les tentatives de reconnexion et les timeouts.
	// Le contexte fourni contrôle la durée de l'ensemble de l'opération GetOrConnect,
	// y compris les tentatives de dial.
	GetOrConnect(ctx context.Context, addr string) (quic.Connection, error)

	// Invalidate ferme la connexion à l'adresse spécifiée et la supprime du pool.
	// Utile si le client détecte qu'une connexion est devenue irrécupérable.
	Invalidate(ctx context.Context, addr string)

	// CloseAll ferme toutes les connexions actives gérées par le manager.
	// Doit être appelée lors de l'arrêt du client pour libérer les ressources.
	CloseAll() error
}
