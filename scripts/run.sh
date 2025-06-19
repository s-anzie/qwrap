#!/bin/bash

# run_client.sh
# Script pour compiler et exécuter le client qwrap, avec redirection des logs.

# Variables de configuration (à adapter si nécessaire)
ORCHESTRATOR_ADDR="localhost:7878"
FILE_ID_TO_DOWNLOAD="sirene.mp4" 
OUTPUT_FILE_PATH="./downloaded_files/${FILE_ID_TO_DOWNLOAD}"
LOG_DIR="./logs" # Répertoire pour les logs
LOG_FILE_PATH="${LOG_DIR}/client.log" # Chemin complet du fichier de log
LOG_LEVEL="debug" 
CONCURRENCY=12    
INSECURE_TLS="true" 

# --- Fin de la configuration ---

# Créer le répertoire de sortie pour les fichiers téléchargés
mkdir -p "$(dirname "$OUTPUT_FILE_PATH")"

# Créer le répertoire des logs s'il n'existe pas
mkdir -p "$LOG_DIR"

# Construire le client
echo "Building qwrap client..."
go build -o ./bin/qwrap_client ./cmd/client
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi
echo "Build successful."

# Préparer les arguments
ARGS=""
ARGS+=" -orchestrator=${ORCHESTRATOR_ADDR}"
ARGS+=" -file=${FILE_ID_TO_DOWNLOAD}"
ARGS+=" -o=${OUTPUT_FILE_PATH}"
ARGS+=" -loglevel=${LOG_LEVEL}"
ARGS+=" -concurrency=${CONCURRENCY}"
ARGS+=" -insecure=${INSECURE_TLS}"

# Exécuter le client et rediriger stdout et stderr vers tee
# tee dupliquera la sortie vers le fichier de log ET vers la console
echo "Running qwrap client. Logs will be in ${LOG_FILE_PATH} and on console."
echo "Command: ./bin/qwrap_client ${ARGS}"
echo "--- Client Output Start ---"

# 2>&1 redirige stderr vers stdout, puis | tee envoie stdout (qui contient maintenant aussi stderr)
# à la fois au fichier (en mode ajout -a) et à la sortie standard du script (la console).
./bin/qwrap_client ${ARGS} 2>&1 | tee -a "${LOG_FILE_PATH}"

# Récupérer le code de sortie du client qwrap
# La commande ci-dessus exécute qwrap_client et tee dans un pipeline.
# $? donnerait le code de sortie de tee. PIPESTATUS[0] donne le code de sortie de la première commande du pipeline.
CLIENT_EXIT_CODE=${PIPESTATUS[0]}

echo "--- Client Output End ---"

if [ ${CLIENT_EXIT_CODE} -ne 0 ]; then
    echo "Client exited with error code: ${CLIENT_EXIT_CODE}"
    echo "Check ${LOG_FILE_PATH} for details."
else
    echo "Client finished successfully."
    echo "Downloaded file: ${OUTPUT_FILE_PATH}"
    echo "Logs available in ${LOG_FILE_PATH}"
fi

exit ${CLIENT_EXIT_CODE}