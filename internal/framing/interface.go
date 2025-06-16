package framing

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Writer est une interface pour écrire des messages Protobuf framés.
type Writer interface {
	// WriteMsg sérialise le message Protobuf, préfixe sa longueur en Uvarint,
	// et l'écrit dans le io.Writer sous-jacent.
	// Le contexte peut être utilisé pour les timeouts ou l'annulation.
	WriteMsg(ctx context.Context, msg proto.Message) error
}

// Reader est une interface pour lire des messages Protobuf framés.
type Reader interface {
	// ReadMsg lit la longueur Uvarint, lit le message Protobuf correspondant,
	// et le désérialise dans le message fourni.
	// Le contexte peut être utilisé pour les timeouts ou l'annulation.
	ReadMsg(ctx context.Context, msg proto.Message) error
}
