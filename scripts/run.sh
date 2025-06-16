#!/bin/bash

# Script pour compiler et lancer un scénario de test qwrap
# avec un orchestrateur, plusieurs agents, et un client.
# Les logs sont redirigés vers des fichiers.

# --- Configuration ---
PROJECT_ROOT=$(pwd) # Suppose que le script est lancé depuis la racine du projet qwrap
BIN_DIR="${PROJECT_ROOT}/bin"
LOG_DIR="${PROJECT_ROOT}/logs"
AGENT_DATA_BASE_DIR="${PROJECT_ROOT}/agent_data"

ORCHESTRATOR_LOG="${LOG_DIR}/orchestrator.log"
CLIENT_LOG="${LOG_DIR}/client.log"

ORCHESTRATOR_ADDR="localhost:7878"
NUM_AGENTS=3
AGENT_BASE_PORT=8080 # Agent1 sur 8080, Agent2 sur 8081, etc.

# Fichier de test à créer et télécharger
TEST_FILE_NAME="testfile_10M.dat"
TEST_FILE_SIZE_MB=10
DOWNLOAD_DEST_FILE="${PROJECT_ROOT}/downloaded_output_${TEST_FILE_NAME}"

# Options de débogage (debug, info, warn, error)
LOG_LEVEL="debug"
INSECURE_TLS_FLAG="-insecure" 
INSECURE_ORCH_FLAG="-insecure-orch"


# --- Fonctions Utilitaires ---
cleanup() {
    echo "Nettoyage des processus qwrap..."
    # Tuer les processus. Utiliser pkill avec le chemin complet pour plus de spécificité.
    # Cela suppose que les binaires sont bien dans BIN_DIR.
    if [ -d "${BIN_DIR}" ]; then
        pkill -f "${BIN_DIR}/orchestrator"
        pkill -f "${BIN_DIR}/agent"
        pkill -f "${BIN_DIR}/client"
    else
        # Fallback si BIN_DIR n'existe pas (par exemple, si la compilation a échoué avant)
        pkill -f "orchestrator"
        pkill -f "agent"
        pkill -f "client"
    fi
    echo "Nettoyage terminé."
}

# Intercepter Ctrl+C et autres signaux pour nettoyer
trap cleanup SIGINT SIGTERM EXIT

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
echo "  Client..."
go build -o "${BIN_DIR}/client"       ./cmd/client/main.go       || { echo "ERREUR: Échec compilation client";       exit 1; }
echo "Compilation terminée."
echo # Ligne vide pour la lisibilité


# --- Démarrage des Composants ---

# 1. Démarrer l'Orchestrateur
echo "Démarrage de l'Orchestrateur sur ${ORCHESTRATOR_ADDR}..."
# Supprimer l'ancien log de l'orchestrateur
rm -f "${ORCHESTRATOR_LOG}"
"${BIN_DIR}/orchestrator" -listen "${ORCHESTRATOR_ADDR}" -loglevel "${LOG_LEVEL}" > "${ORCHESTRATOR_LOG}" 2>&1 &
ORCH_PID=$!
echo "Orchestrateur démarré (PID: ${ORCH_PID}). Logs dans ${ORCHESTRATOR_LOG}"
sleep 3 # Laisser un peu plus de temps, surtout si des certificats sont générés

# Vérifier si l'orchestrateur a bien démarré
if ! ps -p ${ORCH_PID} > /dev/null; then
    echo "ERREUR: L'orchestrateur n'a pas pu démarrer. Vérifiez ${ORCHESTRATOR_LOG}"
    cat "${ORCHESTRATOR_LOG}" # Afficher le log en cas d'échec
    exit 1
fi
echo "Orchestrateur semble opérationnel."
echo # Ligne vide

# 2. Démarrer les Agents
AGENT_PIDS=()
for i in $(seq 1 ${NUM_AGENTS}); do
    AGENT_ID="agent$(printf "%03d" ${i})"
    AGENT_PORT=$((AGENT_BASE_PORT + i - 1))
    AGENT_LISTEN_ADDR="localhost:${AGENT_PORT}" # Les agents écoutent sur localhost pour ce test
    AGENT_DATA_DIR="${AGENT_DATA_BASE_DIR}/${AGENT_ID}"
    AGENT_LOG="${LOG_DIR}/${AGENT_ID}.log"

    mkdir -p "${AGENT_DATA_DIR}"
    rm -f "${AGENT_LOG}" # Supprimer l'ancien log
    echo "Démarrage de l'Agent ${AGENT_ID} sur ${AGENT_LISTEN_ADDR}, data dans ${AGENT_DATA_DIR}..."

    # Créer le fichier de test pour cet agent (seulement pour le premier agent pour ce scénario)
    if [ "$i" -eq 1 ]; then
        TEST_FILE_PATH_AGENT="${AGENT_DATA_DIR}/${TEST_FILE_NAME}"
        echo "Création du fichier de test ${TEST_FILE_NAME} (${TEST_FILE_SIZE_MB}MB) dans ${AGENT_DATA_DIR}..."
        # Vérifier si dd est disponible
        if command -v dd &> /dev/null; then
            dd if=/dev/urandom of="${TEST_FILE_PATH_AGENT}" bs=1M count=${TEST_FILE_SIZE_MB} status=none \
                || { echo "ERREUR: Échec de création du fichier de test pour ${AGENT_ID}"; cleanup; exit 1; }
        else
            # Fallback simple si dd n'est pas là (crée un fichier plus petit)
            echo "dd non trouvé, création d'un fichier de test plus petit."
            head -c $((TEST_FILE_SIZE_MB * 1024 * 512)) < /dev/urandom > "${TEST_FILE_PATH_AGENT}" # Moitié de la taille pour être plus rapide
        fi
        echo "Fichier de test créé : ${TEST_FILE_PATH_AGENT}"
    fi

    "${BIN_DIR}/agent" \
        -id "${AGENT_ID}" \
        -listen "${AGENT_LISTEN_ADDR}" \
        -data "${AGENT_DATA_DIR}" \
        -orchestrator "${ORCHESTRATOR_ADDR}" \
        ${INSECURE_ORCH_FLAG} \
        -loglevel "${LOG_LEVEL}" > "${AGENT_LOG}" 2>&1 &
    
    CURRENT_AGENT_PID=$!
    AGENT_PIDS+=(${CURRENT_AGENT_PID})
    echo "Agent ${AGENT_ID} démarré (PID: ${CURRENT_AGENT_PID}). Logs dans ${AGENT_LOG}"
    sleep 1 # Petit délai entre les démarrages d'agent pour observer les logs
done

# Laisser le temps aux agents de s'enregistrer
echo "Attente de 5 secondes pour l'enregistrement des agents..."
sleep 5
echo # Ligne vide


# 3. Démarrer le Client pour télécharger le fichier
echo "Démarrage du Client pour télécharger ${TEST_FILE_NAME}..."
CLIENT_FILE_ID="${TEST_FILE_NAME}" 

rm -f "${DOWNLOAD_DEST_FILE}" # Supprimer l'ancien fichier de destination
rm -f "${CLIENT_LOG}"      # Supprimer l'ancien log client

START_TIME=$(date +%s%N)
"${BIN_DIR}/client" \
    -file "${CLIENT_FILE_ID}" \
    -o "${DOWNLOAD_DEST_FILE}" \
    -orchestrator "${ORCHESTRATOR_ADDR}" \
    ${INSECURE_TLS_FLAG} \
    -loglevel "${LOG_LEVEL}" > "${CLIENT_LOG}" 2>&1
CLIENT_EXIT_CODE=$?
END_TIME=$(date +%s%N)

# --- Analyse du Résultat du Client ---
echo # Ligne vide
if [ ${CLIENT_EXIT_CODE} -eq 0 ]; then
    echo "SUCCÈS: Le Client a terminé avec le code de sortie 0."
    echo "Fichier téléchargé dans ${DOWNLOAD_DEST_FILE}"
    if [ -f "${DOWNLOAD_DEST_FILE}" ]; then
        EXPECTED_BYTES=$((TEST_FILE_SIZE_MB * 1024 * 1024))
        # Adapter la commande stat pour macOS si nécessaire
        if [[ "$OSTYPE" == "darwin"* ]]; then
            ACTUAL_BYTES=$(stat -f%z "${DOWNLOAD_DEST_FILE}")
        else
            ACTUAL_BYTES=$(stat -c%s "${DOWNLOAD_DEST_FILE}")
        fi
        echo "Taille attendue: ${EXPECTED_BYTES} octets"
        echo "Taille réelle  : ${ACTUAL_BYTES} octets"
        if [ "${ACTUAL_BYTES}" -eq "${EXPECTED_BYTES}" ]; then
            echo "VÉRIFICATION DE TAILLE: OK"
        else
            echo "VÉRIFICATION DE TAILLE: ÉCHEC"
        fi
    else
        echo "ERREUR: Fichier de destination ${DOWNLOAD_DEST_FILE} non trouvé après le téléchargement."
    fi
else
    echo "ÉCHEC: Le Client a terminé avec le code de sortie ${CLIENT_EXIT_CODE}."
    echo "Consultez ${CLIENT_LOG} pour les détails."
    cat "${CLIENT_LOG}" # Afficher le log client en cas d'échec
fi

DURATION_NS=$((END_TIME - START_TIME))
# Convertir en secondes avec bc pour la précision flottante
DURATION_S=$(echo "scale=3; $DURATION_NS / 1000000000" | bc)
echo "Durée du téléchargement client: ${DURATION_S} secondes"
echo # Ligne vide


# --- Arrêt et Nettoyage ---
echo "Arrêt des composants..."
cleanup # Appelle la fonction de nettoyage qui tue les processus

echo "Fin du scénario de test."
echo "Logs disponibles dans le répertoire: ${LOG_DIR}"