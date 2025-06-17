#!/bin/bash

# Script pour lancer un client qwrap et télécharger un fichier.
# Ce script suppose que l'orchestrateur et les agents sont déjà en cours d'exécution
# (lancés via scripts/background.sh, par exemple).

# --- Configuration ---
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
LOG_DIR="${PROJECT_ROOT}/logs"
AGENT_DATA_BASE_DIR="${PROJECT_ROOT}/data/agents"

CLIENT_LOG="${LOG_DIR}/client.log"
ORCHESTRATOR_ADDR="localhost:7878"

TEST_FILE_NAME="sirene.mp4"
DOWNLOAD_DEST_FILE="${PROJECT_ROOT}/data/output/downloaded_${TEST_FILE_NAME}"

LOG_LEVEL="debug"
INSECURE_TLS_FLAG="-insecure"

# --- Exécution du Client ---

echo "Démarrage du Client pour télécharger ${TEST_FILE_NAME}..."

# Recompiler le client au cas où il y aurait eu des changements
echo "Compilation du client..."
go build -o "${BIN_DIR}/client" ./cmd/client/main.go || { echo "ERREUR: Échec compilation client"; exit 1; }

AGENT_INVENTORY_FILE="${AGENT_DATA_BASE_DIR}/agent001/inventory.json"
if [ ! -f "$AGENT_INVENTORY_FILE" ]; then
    echo "ERREUR: Fichier d'inventaire introuvable: ${AGENT_INVENTORY_FILE}"
    echo "Assurez-vous que les services d'arrière-plan sont démarrés (scripts/background.sh) et que les données ont été générées."
    exit 1
fi

CLIENT_FILE_ID=$(jq -r 'keys[0]' "$AGENT_INVENTORY_FILE")
ORIGINAL_FILE_PATH="${PROJECT_ROOT}/data/input/${TEST_FILE_NAME}"
if [ ! -f "$ORIGINAL_FILE_PATH" ]; then
    echo "ERREUR: Fichier source introuvable: ${ORIGINAL_FILE_PATH}"
    exit 1
fi
FILE_SIZE=$(stat -c%s "$ORIGINAL_FILE_PATH")

if [ -z "$CLIENT_FILE_ID" ] || [ "$CLIENT_FILE_ID" == "null" ]; then
    echo "ERREUR: Impossible de lire file_id depuis ${AGENT_INVENTORY_FILE}"
    exit 1
fi

echo "Demande de transfert pour le fichier: ${CLIENT_FILE_ID} (Taille: ${FILE_SIZE} octets)"

rm -f "${DOWNLOAD_DEST_FILE}"
rm -f "${CLIENT_LOG}"

"${BIN_DIR}/client" \
    -file "${CLIENT_FILE_ID}" \
    -o "${DOWNLOAD_DEST_FILE}" \
    -orchestrator "${ORCHESTRATOR_ADDR}" \
    ${INSECURE_TLS_FLAG} \
    -loglevel "${LOG_LEVEL}" > "${CLIENT_LOG}" 2>&1

# --- Résultat ---
echo
echo "Transfert terminé."
echo "Pour voir les détails du transfert, consultez les 20 dernières lignes du log client avec la commande :"
echo "tail -n 20 ${CLIENT_LOG}"
echo



echo "Fin du scénario de test."
echo "Logs disponibles dans le répertoire: ${LOG_DIR}"