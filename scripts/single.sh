#!/bin/bash

# Script pour compiler et lancer un scénario de test qwrap simple
# avec UN orchestrateur, UN agent, et UN client, utilisant un fichier source EXISTANT.

# --- Configuration ---
PROJECT_ROOT=$(pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
LOG_DIR="${PROJECT_ROOT}/logs"
AGENT_DATA_DIR_SINGLE="${PROJECT_ROOT}/agent_data_files/"

# >>> NOUVEAU: Chemin vers votre fichier source existant <<<
# Modifiez cette variable pour pointer vers votre fichier de test
EXISTING_SOURCE_FILE_PATH="${PROJECT_ROOT}/data/input/sirene.mp4" # EXEMPLE, à adapter !

ORCHESTRATOR_LOG="${LOG_DIR}/orchestrator_single.log"
AGENT_LOG_SINGLE="${LOG_DIR}/agent_single.log"
CLIENT_LOG_SINGLE="${LOG_DIR}/client_single.log"

ORCHESTRATOR_ADDR="localhost:7878"
AGENT_ID_SINGLE="agent_single_001"
AGENT_LISTEN_ADDR_SINGLE="localhost:8080"

# Le nom de fichier que l'agent et le client utiliseront (sera le nom de base du fichier source)
TEST_FILE_NAME=$(basename "${EXISTING_SOURCE_FILE_PATH}")
DOWNLOAD_DEST_FILE="${PROJECT_ROOT}/data/output/downloaded_${TEST_FILE_NAME}"

LOG_LEVEL="debug"
INSECURE_TLS_FLAG="-insecure"
INSECURE_ORCH_FLAG="-insecure-orch"

# --- Fonctions Utilitaires ---
cleanup() {
    echo "Nettoyage des processus qwrap (single agent test)..."
    if [ -d "${BIN_DIR}" ]; then
        pkill -f "${BIN_DIR}/orchestrator"
        pkill -f "${BIN_DIR}/agent"
        pkill -f "${BIN_DIR}/client"
    else
        pkill -f "orchestrator"; pkill -f "agent"; pkill -f "client"
    fi
    # Optionnel: supprimer le fichier copié/lié dans AGENT_DATA_DIR_SINGLE
    # rm -f "${AGENT_DATA_DIR_SINGLE}/${TEST_FILE_NAME}"
    echo "Nettoyage terminé."
}
trap cleanup SIGINT SIGTERM EXIT

# --- Préparation ---
echo "Préparation de l'environnement de test (single agent)..."
mkdir -p "${BIN_DIR}"
mkdir -p "${LOG_DIR}"
mkdir -p "${AGENT_DATA_DIR_SINGLE}"

# Vérifier si le fichier source existe
if [ ! -f "${EXISTING_SOURCE_FILE_PATH}" ]; then
    echo "ERREUR: Le fichier source spécifié '${EXISTING_SOURCE_FILE_PATH}' n'existe pas."
    echo "Veuillez modifier la variable EXISTING_SOURCE_FILE_PATH dans le script."
    exit 1
fi
TEST_FILE_SIZE_BYTES=$(stat -c%s "${EXISTING_SOURCE_FILE_PATH}") # Linux
# TEST_FILE_SIZE_BYTES=$(stat -f%z "${EXISTING_SOURCE_FILE_PATH}") # macOS
if [ -z "${TEST_FILE_SIZE_BYTES}" ] || [ "${TEST_FILE_SIZE_BYTES}" -eq 0 ]; then
    echo "ERREUR: Le fichier source '${EXISTING_SOURCE_FILE_PATH}' est vide ou sa taille n'a pas pu être déterminée."
    exit 1
fi
echo "Utilisation du fichier source existant: ${EXISTING_SOURCE_FILE_PATH} (Taille: ${TEST_FILE_SIZE_BYTES} octets)"

# Compilation
echo "Compilation des composants..."; echo "  Orchestrateur..."
go build -o "${BIN_DIR}/orchestrator" ./cmd/orchestrator/main.go || { echo "ERREUR: Échec compilation orchestrateur"; exit 1; }
echo "  Agent..."; go build -o "${BIN_DIR}/agent" ./cmd/agent/main.go || { echo "ERREUR: Échec compilation agent"; exit 1; }
echo "  Client..."; go build -o "${BIN_DIR}/client" ./cmd/client/main.go || { echo "ERREUR: Échec compilation client"; exit 1; }
echo "Compilation terminée."; echo

# --- Démarrage des Composants ---
# 1. Orchestrateur
echo "Démarrage de l'Orchestrateur sur ${ORCHESTRATOR_ADDR}..."
rm -f "${ORCHESTRATOR_LOG}"
"${BIN_DIR}/orchestrator" -listen "${ORCHESTRATOR_ADDR}" -loglevel "${LOG_LEVEL}" > "${ORCHESTRATOR_LOG}" 2>&1 &
ORCH_PID=$!; echo "Orchestrateur PID: ${ORCH_PID}. Logs: ${ORCHESTRATOR_LOG}"; sleep 3
if ! ps -p ${ORCH_PID} > /dev/null; then echo "ERREUR: Orchestrateur KO. Voir ${ORCHESTRATOR_LOG}"; cat "${ORCHESTRATOR_LOG}"; exit 1; fi
echo "Orchestrateur OK."; echo

# 2. Agent Unique
echo "Démarrage de l'Agent ${AGENT_ID_SINGLE} sur ${AGENT_LISTEN_ADDR_SINGLE}..."
rm -f "${AGENT_LOG_SINGLE}"

# Copier (ou lier) le fichier source dans le répertoire de données de l'agent
AGENT_TARGET_FILE_PATH="${AGENT_DATA_DIR_SINGLE}/${TEST_FILE_NAME}"
echo "Copie de '${EXISTING_SOURCE_FILE_PATH}' vers '${AGENT_TARGET_FILE_PATH}'..."
cp "${EXISTING_SOURCE_FILE_PATH}" "${AGENT_TARGET_FILE_PATH}" || { echo "ERREUR: Échec de copie du fichier source vers l'agent"; cleanup; exit 1; }
# Alternative (lien symbolique, plus rapide pour gros fichiers, mais l'agent doit pouvoir le suivre):
# ln -sf "${EXISTING_SOURCE_FILE_PATH}" "${AGENT_TARGET_FILE_PATH}" || { echo "ERREUR: Échec de création du lien symbolique"; cleanup; exit 1; }

"${BIN_DIR}/agent" -id "${AGENT_ID_SINGLE}" -listen "${AGENT_LISTEN_ADDR_SINGLE}" -data "${AGENT_DATA_DIR_SINGLE}" \
    -orchestrator "${ORCHESTRATOR_ADDR}" ${INSECURE_ORCH_FLAG} -loglevel "${LOG_LEVEL}" > "${AGENT_LOG_SINGLE}" 2>&1 &
AGENT_PID=$!; echo "Agent ${AGENT_ID_SINGLE} PID: ${AGENT_PID}. Logs: ${AGENT_LOG_SINGLE}"; sleep 5; echo

# 3. Client
echo "Démarrage du Client pour télécharger ${TEST_FILE_NAME}..."
rm -f "${DOWNLOAD_DEST_FILE}"; rm -f "${CLIENT_LOG_SINGLE}"
START_TIME=$(date +%s%N)
"${BIN_DIR}/client" -file "${TEST_FILE_NAME}" -o "${DOWNLOAD_DEST_FILE}" \
    -orchestrator "${ORCHESTRATOR_ADDR}" ${INSECURE_TLS_FLAG} -loglevel "${LOG_LEVEL}" > "${CLIENT_LOG_SINGLE}" 2>&1
CLIENT_EXIT_CODE=$?; END_TIME=$(date +%s%N)

# Analyse Résultat
echo
if [ ${CLIENT_EXIT_CODE} -eq 0 ]; then
    echo "SUCCÈS: Client (single) OK."
    if [ -f "${DOWNLOAD_DEST_FILE}" ]; then
        ACTUAL_BYTES=$(stat -c%s "${DOWNLOAD_DEST_FILE}") # Linux
        # ACTUAL_BYTES=$(stat -f%z "${DOWNLOAD_DEST_FILE}") # macOS
        echo "Fichier: ${DOWNLOAD_DEST_FILE}. Attendu: ${TEST_FILE_SIZE_BYTES} octets. Réel: ${ACTUAL_BYTES} octets."
        if [ "${ACTUAL_BYTES}" -eq "${TEST_FILE_SIZE_BYTES}" ]; then echo "VÉRIFICATION TAILLE: OK"
        else echo "VÉRIFICATION TAILLE: ÉCHEC"; fi
        # Vérification Checksum
        SOURCE_CS=$(sha256sum "${EXISTING_SOURCE_FILE_PATH}" | awk '{print $1}')
        DOWNLOAD_CS=$(sha256sum "${DOWNLOAD_DEST_FILE}" | awk '{print $1}')
        echo "Checksum Source    : ${SOURCE_CS}"
        echo "Checksum Téléchargé: ${DOWNLOAD_CS}"
        if [ "${SOURCE_CS}" == "${DOWNLOAD_CS}" ]; then echo "VÉRIFICATION CHECKSUM: OK"; else echo "VÉRIFICATION CHECKSUM: ÉCHEC"; fi
    else echo "ERREUR: Fichier destination non trouvé."; fi
else
    echo "ÉCHEC: Client (single) code ${CLIENT_EXIT_CODE}. Voir ${CLIENT_LOG_SINGLE}"; cat "${CLIENT_LOG_SINGLE}";
fi
DURATION_NS=$((END_TIME - START_TIME))
DURATION_S=$(awk "BEGIN {printf \"%.3f\", ${DURATION_NS}/1000000000}")
echo "Durée client (single): ${DURATION_S}s"; echo

cleanup
echo "Fin du test (single agent)."