#!/bin/bash

# Script de génération de la structure du projet qwrap
# Usage: ./setup_qwrap_project.sh [nom_du_projet]

PROJECT_NAME=${1:-qwrap}
PROJECT_ROOT=$(pwd)/""

echo "🚀 Création de la structure du projet $PROJECT_NAME..."

# Création de la structure des répertoires
mkdir -p "$PROJECT_ROOT"/{cmd/{client,orchestrator,agent},internal/{client,orchestrator,agent,common,messaging},pkg/qwrappb,proto,configs,data}

echo "📁 Structure des répertoires créée"

# Création des fichiers Go dans cmd/
cat > "$PROJECT_ROOT/cmd/client/main.go" << 'EOF'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourusername/qwrap/internal/client"
)

func main() {
	var (
		orchestratorAddr = flag.String("orchestrator", "localhost:8080", "Adresse de l'orchestrateur")
		outputDir        = flag.String("output", "./downloads", "Répertoire de téléchargement")
		fileID           = flag.String("file", "", "ID du fichier à télécharger")
	)
	flag.Parse()

	if *fileID == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -file FILE_ID\n", os.Args[0])
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gestion gracieuse des signaux
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Arrêt du client...")
		cancel()
	}()

	// TODO: Initialiser et démarrer le client qwrap
	client := client.New(*orchestratorAddr, *outputDir)
	if err := client.DownloadFile(ctx, *fileID); err != nil {
		log.Fatalf("Erreur lors du téléchargement: %v", err)
	}

	fmt.Println("✅ Téléchargement terminé avec succès")
}
EOF

cat > "$PROJECT_ROOT/cmd/orchestrator/main.go" << 'EOF'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourusername/qwrap/internal/orchestrator"
)

func main() {
	var (
		listenAddr = flag.String("listen", ":8080", "Adresse d'écoute")
		dataDir    = flag.String("data", "./data", "Répertoire des données")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gestion gracieuse des signaux
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Arrêt de l'orchestrateur...")
		cancel()
	}()

	// TODO: Initialiser et démarrer l'orchestrateur
	orch := orchestrator.New(*listenAddr, *dataDir)
	if err := orch.Start(ctx); err != nil {
		log.Fatalf("Erreur lors du démarrage de l'orchestrateur: %v", err)
	}

	fmt.Println("🎭 Orchestrateur démarré sur", *listenAddr)
	<-ctx.Done()
}
EOF

cat > "$PROJECT_ROOT/cmd/agent/main.go" << 'EOF'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourusername/qwrap/internal/agent"
)

func main() {
	var (
		orchestratorAddr = flag.String("orchestrator", "localhost:8080", "Adresse de l'orchestrateur")
		listenAddr       = flag.String("listen", ":0", "Adresse d'écoute (0 = port aléatoire)")
		storageDir       = flag.String("storage", "./storage", "Répertoire de stockage")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gestion gracieuse des signaux
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Arrêt de l'agent...")
		cancel()
	}()

	// TODO: Initialiser et démarrer l'agent
	agent := agent.New(*orchestratorAddr, *listenAddr, *storageDir)
	if err := agent.Start(ctx); err != nil {
		log.Fatalf("Erreur lors du démarrage de l'agent: %v", err)
	}

	fmt.Println("🤖 Agent démarré et connecté à l'orchestrateur")
	<-ctx.Done()
}
EOF

echo "✅ Fichiers main.go créés"

# Création des fichiers internes
cat > "$PROJECT_ROOT/internal/client/connection_manager.go" << 'EOF'
package client

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/quic-go/quic-go"
)

// ConnectionManager gère les connexions QUIC réutilisables
type ConnectionManager struct {
	mu          sync.RWMutex
	connections map[string]quic.Connection
	tlsConfig   *tls.Config
}

// NewConnectionManager crée un nouveau gestionnaire de connexions
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]quic.Connection),
		tlsConfig: &tls.Config{
			InsecureSkipVerify: true, // TODO: Configurer correctement en production
			NextProtos:         []string{"qwrap"},
		},
	}
}

// GetConnection retourne une connexion existante ou en crée une nouvelle
func (cm *ConnectionManager) GetConnection(ctx context.Context, addr string) (quic.Connection, error) {
	cm.mu.RLock()
	if conn, exists := cm.connections[addr]; exists {
		cm.mu.RUnlock()
		return conn, nil
	}
	cm.mu.RUnlock()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check après acquisition du verrou d'écriture
	if conn, exists := cm.connections[addr]; exists {
		return conn, nil
	}

	// Créer une nouvelle connexion
	conn, err := quic.DialAddr(ctx, addr, cm.tlsConfig, nil)
	if err != nil {
		return nil, err
	}

	cm.connections[addr] = conn
	return conn, nil
}

// Close ferme toutes les connexions
func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, conn := range cm.connections {
		conn.CloseWithError(0, "shutting down")
	}
	cm.connections = make(map[string]quic.Connection)
	return nil
}
EOF

cat > "$PROJECT_ROOT/internal/client/downloader.go" << 'EOF'
package client

import (
	"context"
	"fmt"
	"sync"
)

// Downloader gère le téléchargement distribué de fichiers
type Downloader struct {
	connManager    *ConnectionManager
	maxConcurrency int
}

// NewDownloader crée un nouveau téléchargeur
func NewDownloader(maxConcurrency int) *Downloader {
	return &Downloader{
		connManager:    NewConnectionManager(),
		maxConcurrency: maxConcurrency,
	}
}

// DownloadFile télécharge un fichier de manière distribuée
func (d *Downloader) DownloadFile(ctx context.Context, fileID string) error {
	// TODO: Implémenter la logique de téléchargement distribué
	// 1. Contacter l'orchestrateur pour obtenir la liste des chunks
	// 2. Télécharger les chunks en parallèle depuis les agents
	// 3. Reconstituer le fichier final
	
	fmt.Printf("Téléchargement du fichier %s (non implémenté)\n", fileID)
	return nil
}

// downloadChunk télécharge un chunk spécifique depuis un agent
func (d *Downloader) downloadChunk(ctx context.Context, agentAddr string, chunkID string) ([]byte, error) {
	// TODO: Implémenter le téléchargement d'un chunk
	return nil, fmt.Errorf("non implémenté")
}

// Close ferme le téléchargeur et ses connexions
func (d *Downloader) Close() error {
	return d.connManager.Close()
}
EOF

# Création des autres fichiers internes
cat > "$PROJECT_ROOT/internal/orchestrator/scheduler.go" << 'EOF'
package orchestrator

import (
	"context"
	"fmt"
)

// Scheduler gère la planification et la distribution des tâches
type Scheduler struct {
	agents map[string]*AgentInfo
}

// AgentInfo contient les informations sur un agent
type AgentInfo struct {
	Address   string
	Available bool
	Load      int
}

// NewScheduler crée un nouveau planificateur
func NewScheduler() *Scheduler {
	return &Scheduler{
		agents: make(map[string]*AgentInfo),
	}
}

// RegisterAgent enregistre un nouvel agent
func (s *Scheduler) RegisterAgent(addr string) error {
	s.agents[addr] = &AgentInfo{
		Address:   addr,
		Available: true,
		Load:      0,
	}
	fmt.Printf("Agent enregistré: %s\n", addr)
	return nil
}

// ScheduleChunks planifie la distribution des chunks aux agents
func (s *Scheduler) ScheduleChunks(ctx context.Context, fileID string, chunks []string) error {
	// TODO: Implémenter l'algorithme de planification
	fmt.Printf("Planification de %d chunks pour le fichier %s\n", len(chunks), fileID)
	return nil
}
EOF

cat > "$PROJECT_ROOT/internal/orchestrator/state_manager.go" << 'EOF'
package orchestrator

import (
	"sync"
)

// StateManager gère l'état global du système
type StateManager struct {
	mu    sync.RWMutex
	files map[string]*FileInfo
}

// FileInfo contient les informations sur un fichier
type FileInfo struct {
	ID       string
	Name     string
	Size     int64
	Chunks   []ChunkInfo
	Complete bool
}

// ChunkInfo contient les informations sur un chunk
type ChunkInfo struct {
	ID     string
	Offset int64
	Size   int64
	Hash   string
	Agents []string
}

// NewStateManager crée un nouveau gestionnaire d'état
func NewStateManager() *StateManager {
	return &StateManager{
		files: make(map[string]*FileInfo),
	}
}

// AddFile ajoute un fichier à l'état
func (sm *StateManager) AddFile(fileInfo *FileInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.files[fileInfo.ID] = fileInfo
}

// GetFile retourne les informations d'un fichier
func (sm *StateManager) GetFile(fileID string) (*FileInfo, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	file, exists := sm.files[fileID]
	return file, exists
}
EOF

cat > "$PROJECT_ROOT/internal/agent/chunk_server.go" << 'EOF'
package agent

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/quic-go/quic-go"
)

// ChunkServer gère le service de chunks pour les clients
type ChunkServer struct {
	listenAddr string
	storageDir string
	listener   quic.Listener
}

// NewChunkServer crée un nouveau serveur de chunks
func NewChunkServer(listenAddr, storageDir string) *ChunkServer {
	return &ChunkServer{
		listenAddr: listenAddr,
		storageDir: storageDir,
	}
}

// Start démarre le serveur de chunks
func (cs *ChunkServer) Start(ctx context.Context) error {
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{}, // TODO: Configurer les certificats
		NextProtos:   []string{"qwrap"},
	}

	listener, err := quic.ListenAddr(cs.listenAddr, tlsConfig, nil)
	if err != nil {
		return fmt.Errorf("impossible de démarrer le listener QUIC: %w", err)
	}

	cs.listener = listener
	fmt.Printf("Serveur de chunks démarré sur %s\n", cs.listenAddr)

	go cs.acceptConnections(ctx)
	return nil
}

// acceptConnections accepte les connexions entrantes
func (cs *ChunkServer) acceptConnections(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := cs.listener.Accept(ctx)
			if err != nil {
				fmt.Printf("Erreur lors de l'acceptation de connexion: %v\n", err)
				continue
			}
			go cs.handleConnection(ctx, conn)
		}
	}
}

// handleConnection gère une connexion client
func (cs *ChunkServer) handleConnection(ctx context.Context, conn quic.Connection) {
	defer conn.CloseWithError(0, "done")
	
	// TODO: Implémenter la gestion des requêtes de chunks
	fmt.Printf("Nouvelle connexion depuis %s\n", conn.RemoteAddr())
}

// Stop arrête le serveur
func (cs *ChunkServer) Stop() error {
	if cs.listener != nil {
		return cs.listener.Close()
	}
	return nil
}
EOF

# Création des fichiers common
cat > "$PROJECT_ROOT/internal/common/config.go" << 'EOF'
package common

import (
	"time"
)

// Config contient la configuration globale de qwrap
type Config struct {
	// Configuration réseau
	MaxConnections    int           `json:"max_connections"`
	ConnectionTimeout time.Duration `json:"connection_timeout"`
	ReadTimeout       time.Duration `json:"read_timeout"`
	WriteTimeout      time.Duration `json:"write_timeout"`
	
	// Configuration des chunks
	ChunkSize      int64 `json:"chunk_size"`
	MaxConcurrency int   `json:"max_concurrency"`
	
	// Configuration de sécurité
	EnableTLS    bool   `json:"enable_tls"`
	CertFile     string `json:"cert_file"`
	KeyFile      string `json:"key_file"`
}

// DefaultConfig retourne une configuration par défaut
func DefaultConfig() *Config {
	return &Config{
		MaxConnections:    100,
		ConnectionTimeout: 30 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ChunkSize:         1024 * 1024, // 1MB
		MaxConcurrency:    10,
		EnableTLS:         false,
	}
}
EOF

cat > "$PROJECT_ROOT/internal/common/crypto.go" << 'EOF'
package common

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

// HashAlgorithm représente un algorithme de hachage
type HashAlgorithm string

const (
	SHA256 HashAlgorithm = "sha256"
)

// Hasher interface pour les algorithmes de hachage
type Hasher interface {
	Hash(data []byte) string
	HashReader(r io.Reader) (string, error)
}

// SHA256Hasher implémente le hachage SHA256
type SHA256Hasher struct{}

// NewHasher crée un nouveau hasher pour l'algorithme spécifié
func NewHasher(algo HashAlgorithm
