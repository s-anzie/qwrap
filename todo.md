# Analyse de la logique de téléchargement : Limitations et deadlocks inévitables

## Architecture générale et composants

La logique de téléchargement implémentée dans ce code Go utilise une architecture concurrente complexe basée sur plusieurs composants interconnectés [1]. Le système est organisé autour d'une boucle principale (`manageDownloadLifecycle`) qui coordonne plusieurs goroutines spécialisées : des workers de téléchargement, un writer de fichiers, et un listener pour les mises à jour de l'orchestrateur [2].

### Composants principaux

- **Workers de téléchargement** : Téléchargent les chunks via des connexions QUIC
- **FileWriter** : Écrit les chunks téléchargés dans le fichier de destination
- **Orchestrateur** : Gère la réassignation des chunks en cas d'échec
- **Boucle principale** : Coordonne tous les composants via des channels

## Limitations fondamentales de l'architecture

### 1. Dépendances circulaires inévitables

L'architecture présente trois dépendances circulaires critiques qui créent des conditions de deadlock [3] :

**Worker ↔ MainLoop** : Les workers envoient des résultats via `resultsChan` et reçoivent des jobs via `jobsChan` [1]. Si `resultsChan` est plein et `jobsChan` vide, les workers se bloquent en attendant que la boucle principale traite les résultats, mais celle-ci ne peut pas envoyer de nouveaux jobs [2].

**FileWriter ↔ MainLoop** : Le FileWriter lit depuis `writeChan` tandis que la boucle principale y écrit [3]. Avec le mécanisme de backpressure (`diskWritePermits`), si tous les permits sont consommés et `writeChan` est plein, la boucle principale se bloque [1].

**OrchestratorComms ↔ MainLoop** : L'orchestrateur envoie des mises à jour via `updatedPlanChan` et reçoit des rapports de statut [2]. Si le channel est plein et que l'orchestrateur attend une confirmation, un deadlock peut survenir [3].

### 2. Problèmes de synchronisation des channels

Le code utilise plusieurs channels non-bufferisés et bufferisés avec des tailles inadéquates [4]. Les channels avec des buffers de taille 1 (`errChan`, `finalErrorChan`) créent des goulots d'étranglement critiques [5]. Quand plusieurs goroutines tentent d'envoyer simultanément, seule la première réussit, les autres se bloquent indéfiniment [6].

### 3. Mécanisme de backpressure défaillant

La stratégie de backpressure basée sur des tokens (`diskWritePermits`) introduit un point de contention supplémentaire [7]. Quand tous les permits sont consommés et que `writeChan` est saturé, l'ensemble du pipeline se bloque [1]. Cette approche ne respecte pas le principe de flow control asynchrone recommandé pour les systèmes haute performance [8].

## Conditions de deadlock inévitables

### 1. Saturation en cascade

Lorsque le FileWriter devient lent (I/O disque), `writeChan` se remplit [3]. La boucle principale ne peut plus envoyer de chunks à écrire, ce qui sature `resultsChan` [1]. Les workers ne peuvent plus envoyer de résultats, bloquant tout le système [2].

### 2. Contention sur les états partagés

L'accès concurrent à `chunkStates` via `muChunkStates` crée des conditions de race [9]. Si une goroutine détient le mutex pendant un appel bloquant à l'orchestrateur, toutes les autres goroutines se retrouvent bloquées [1].

### 3. Problème du "select statement" complexe

La boucle principale utilise un `select` avec de multiples cas, créant des conditions de priorité imprévisibles [10]. Selon l'ordre d'évaluation des cas, certains channels peuvent être négligés, causant des deadlocks [6].

## Limitations de performance spécifiques à QUIC

L'utilisation du protocole QUIC introduit des limitations supplémentaires [8]. QUIC souffre d'une dégradation de performance significative sur les connexions rapides (jusqu'à 45.2% plus lent que HTTP/2 au-delà de 1 Gbps) [8]. Cette limitation est due au traitement côté récepteur qui devient un goulot d'étranglement [8].

## Solutions alternatives recommandées

### 1. Architecture basée sur des pipelines asynchrones

Remplacer l'architecture actuelle par un pipeline asynchrone avec des stages découplés [10] :

- **Stage 1** : Réception des chunks sans backpressure
- **Stage 2** : Validation et assemblage en mémoire
- **Stage 3** : Écriture asynchrone sur disque
- **Stage 4** : Reporting de progression

### 2. Modèle Actor avec isolation des états

Implémenter un modèle Actor où chaque composant gère son état de manière isolée [10]. Chaque actor possède sa propre mailbox (channel bufferisé) et ne communique que par messages asynchrones [1].

### 3. Pool de connexions avec load balancing

Remplacer les connexions QUIC individuelles par un pool de connexions HTTP/2 avec load balancing [8]. Cette approche évite les limitations de performance de QUIC tout en maintenant la parallélisation [11].

### 4. Mécanisme de backpressure adaptatif

Implémenter une backpressure adaptative basée sur la mesure de latence plutôt que sur des tokens fixes [7]. Utiliser des métriques en temps réel pour ajuster dynamiquement le débit [10].

### 5. Architecture event-driven

Adopter une architecture event-driven avec un bus d'événements central [10]. Chaque composant émet des événements sans attendre de confirmation synchrone, éliminant les dépendances circulaires [1].

Cette approche alternative garantit une scalabilité horizontale et élimine les risques de deadlock inhérents à l'architecture actuelle tout en améliorant significativement les performances globales [12].

[1] https://github.com/sasha-s/go-deadlock
[2] https://stackoverflow.com/questions/30139279/golang-concurrent-download-deadlock
[3] https://blog.kevinhu.me/2019/02/09/Debugging-An-Interesting-Deadlock-In-Go/
[4] https://coffeebytes.dev/en/go-channels-understanding-the-goroutines-deadlocks/
[5] https://dev.to/divshekhar/golang-channel-deadlock-with-example-5203
[6] https://stackoverflow.com/questions/71228367/why-is-the-order-of-channels-receiving-causing-resolving-a-deadlock-in-golang
[7] https://docs.datastax.com/en/opscenter/6.8/online-help/enabling-backpressure.html
[8] https://arxiv.org/html/2310.09423v2
[9] https://www.santekno.com/en/knowing-deadlock-and-how-to-overcome-it-in-golang/
[10] https://cristiancurteanu.com/7-powerful-golang-concurrency-patterns-that-will-transform-your-code-in-2025/
[11] https://www.reddit.com/r/golang/comments/9ttjb9/how_to_download_single_file_concurrently/
[12] https://blog.devgenius.io/concurrent-file-download-with-go-495d7b946492
[13] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/29218445/ea115282-7dd4-4255-a4b1-9122f803d957/downloader.go
[14] https://www.reddit.com/r/rust/comments/q2a99j/downloading_multiple_files_concurrently_do_i_need/
[15] https://abhii85.hashnode.dev/understanding-concurrency-in-go-and-building-a-concurrent-file-downloader
[16] https://github.com/s3fs-fuse/s3fs-fuse/issues/2463
[17] https://pkg.go.dev/github.com/sb10/go-deadlock
[18] https://www.youtube.com/watch?v=5tYQhkL8LlM
[19] https://arxiv.org/pdf/2303.06324.pdf
[20] https://www.reddit.com/r/golang/comments/199yc0d/why_does_this_code_not_result_in_a_deadlock/
[21] https://www.youtube.com/watch?v=5iU57394pPA
[22] https://github.com/linkdata/deadlock
[23] https://dev.to/ietxaniz/go-deadlock-detection-delock-library-1eig
[24] https://www.compiralabs.com/post/quic-is-it-the-game-changer-for-internet-delivery
[25] https://gist.github.com/YumaInaura/8d52e73dac7dc361745bf568c3c4ba37
[26] https://labex.io/tutorials/go-how-to-avoid-deadlock-with-channel-select-464400
[27] https://www.craig-wood.com/nick/articles/deadlocks-in-go/