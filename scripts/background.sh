#!/bin/bash

# Script pour compiler et lancer les services d'arrière-plan qwrap
# (orchestrateur et agents).

# --- Configuration ---
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
LOG_DIR="${PROJECT_ROOT}/logs"
AGENT_DATA_BASE_DIR="${PROJECT_ROOT}/data/agents"

ORCHESTRATOR_LOG="${LOG_DIR}/orchestrator.log"

ORCHESTRATOR_ADDR="localhost:7878"
NUM_AGENTS=3
AGENT_BASE_PORT=8080 # Agent1 sur 8080, Agent2 sur 8081, etc.

# Options de débogage (debug, info, warn, error)
LOG_LEVEL="debug"
INSECURE_ORCH_FLAG="-insecure-orch"

# --- Fonctions Utilitaires ---
cleanup() {
    echo "
Arrêt des services en arrière-plan..."
    pkill -f "${BIN_DIR}/orchestrator"
    pkill -f "${BIN_DIR}/agent"
    echo "Nettoyage terminé."
}

# Intercepter Ctrl+C pour nettoyer
trap cleanup SIGINT SIGTERM

# --- Préparation ---
echo "Préparation de l'environnement de test..."
mkdir -p "${BIN_DIR}"
mkdir -p "${LOG_DIR}"
mkdir -p "${AGENT_DATA_BASE_DIR}"

# Compilation des composants
echo "Compilation des composants..."
echo "  Orchestrateur..."
go build -o "${BIN_DIR}/orchestrator" ./cmd/orchestrator/main.go || { echo "ERREUR: Échec compilation orchestrateur"; exit 1; }
echo "  Agent..."
go build -o "${BIN_DIR}/agent"        ./cmd/agent/main.go        || { echo "ERREUR: Échec compilation agent";        exit 1; }
echo "Compilation terminée."

# --- Démarrage des Composants ---

# 1. Démarrer l'Orchestrateur
echo "Démarrage de l'Orchestrateur sur ${ORCHESTRATOR_ADDR}..."
rm -f "${ORCHESTRATOR_LOG}"
"${BIN_DIR}/orchestrator" -listen "${ORCHESTRATOR_ADDR}" -loglevel "${LOG_LEVEL}" > "${ORCHESTRATOR_LOG}" 2>&1 &
ORCH_PID=$!
echo "Orchestrateur démarré (PID: ${ORCH_PID}). Logs dans ${ORCHESTRATOR_LOG}"
sleep 2

if ! ps -p ${ORCH_PID} > /dev/null; then
    echo "ERREUR: L'orchestrateur n'a pas pu démarrer. Vérifiez ${ORCHESTRATOR_LOG}"
    cat "${ORCHESTRATOR_LOG}"
    exit 1
fi
echo "Orchestrateur semble opérationnel."

# 2. Démarrer les Agents
for i in $(seq 1 ${NUM_AGENTS}); do
    AGENT_ID="agent$(printf "%03d" ${i})"
    AGENT_PORT=$((AGENT_BASE_PORT + i - 1))
    AGENT_LISTEN_ADDR="localhost:${AGENT_PORT}"
    AGENT_DATA_DIR="${AGENT_DATA_BASE_DIR}/${AGENT_ID}"
    AGENT_LOG="${LOG_DIR}/${AGENT_ID}.log"

    mkdir -p "${AGENT_DATA_DIR}"
    rm -f "${AGENT_LOG}"
    echo "Démarrage de l'Agent ${AGENT_ID} sur ${AGENT_LISTEN_ADDR}..."

    "${BIN_DIR}/agent" \
        -id "${AGENT_ID}" \
        -listen "${AGENT_LISTEN_ADDR}" \
        -data "${AGENT_DATA_DIR}" \
        -orchestrator "${ORCHESTRATOR_ADDR}" \
        ${INSECURE_ORCH_FLAG} \
        -loglevel "${LOG_LEVEL}" > "${AGENT_LOG}" 2>&1 &
    
    echo "Agent ${AGENT_ID} démarré (PID: $!). Logs dans ${AGENT_LOG}"
done

echo "
Services en arrière-plan démarrés. Appuyez sur Ctrl+C pour les arrêter."

# Attendre indéfiniment pour garder les processus en vie
wait
