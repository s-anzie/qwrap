**Phase de Persistance de l'Inventaire de l'Agent - Terminé**

*   [x] **Développer un outil de génération de données (`datagen`) :**
    *   [x] Créer un outil en Go pour générer des données de test et l'inventaire des agents.
    *   [x] Implémenter le mode "génération" pour créer de nouveaux fichiers de données.
    *   [x] Implémenter le mode "fichier unique" pour diviser un fichier existant.
    *   [x] Implémenter le mode "répertoire" pour traiter récursivement les fichiers d'un dossier.
    *   [x] Générer un `inventory.json` précis pour chaque agent, contenant les métadonnées des portions de fichiers assignées (y compris `path_on_disk`).

*   [x] **Rendre le `ChunkServer` de l'Agent persistant :**
    *   [x] Réfondre le `ChunkServer` pour qu'il charge son inventaire depuis le fichier `inventory.json` au démarrage.
    *   [x] Supprimer le chargement de données d'inventaire en dur (`loadHardcodedPortions`).
    *   [x] Gérer correctement le cas où `inventory.json` n'existe pas (démarrage avec un inventaire vide).
    *   [x] Gérer les erreurs de parsing si `inventory.json` est malformé.

*   [x] **Tests et Validation :**
    *   [x] Ajouter une suite de tests complète pour l'outil `datagen` (`generator_test.go`).
    *   [x] Ajouter des tests unitaires pour le `ChunkServer` (`chunk_server_test.go`) afin de valider :
        *   [x] Le chargement réussi d'un `inventory.json` valide.
        *   [x] Le démarrage correct en l'absence du fichier d'inventaire.
        *   [x] La gestion d'erreur lors du chargement d'un fichier d'inventaire corrompu.

---

**Phase de Persistance du `StateManager` - Todo List**

1.  **Choix du Backend de Persistance Embarqué (Clé/Valeur) :**
    *   [ ] **Évaluer les options :**
        *   **BoltDB :** Plus simple, pure Go, transactionnel (ACID pour une goroutine ou avec gestion manuelle). Idéal si la concurrence en écriture sur l'état n'est pas extrêmement élevée.
        *   **BadgerDB :** Plus performant (surtout pour les écritures et scans), LSM-Tree, pure Go. Plus complexe à gérer (VLog, GC). Mieux si beaucoup de mises à jour d'état.
        *   *Autres options moins courantes pour l'embarqué : LevelDB (via cgo), etcd/bbolt (fork de BoltDB par etcd).*
    *   [ ] **Prendre une décision :** Pour un premier passage, **BoltDB** est souvent un bon choix pour sa simplicité d'intégration.

2.  **Conception de la Structure de Stockage dans la DB Clé/Valeur :**
    *   [ ] **Définir les "Buckets" (BoltDB) ou les préfixes de clés (BadgerDB) :**
        *   Exemple : Un bucket "transfers" où chaque clé est un `PlanID`.
    *   [ ] **Format de Sérialisation pour `TransferState` :**
        *   **Protobuf Binaire :** Préférable pour la compacité et la performance si vous avez déjà des définitions Protobuf pour vos états (ce qui n'est pas le cas actuellement pour `TransferState`, `FileState`, `ChunkState` qui sont des structs Go). Il faudrait définir des messages `.proto` équivalents à ces structs Go.
        *   **JSON (via `encoding/json`) :** Plus simple à mettre en œuvre si vos structs Go sont déjà bien définies, mais plus verbeux et potentiellement moins performant que Protobuf binaire.
        *   **Gob (via `encoding/gob`) :** Spécifique à Go, binaire, gère bien les types Go complexes. Moins portable si d'autres langages doivent accéder à la DB.
    *   [ ] **Décider si l'état complet `TransferState` est stocké sous une seule clé `PlanID`, ou si des sous-structures (comme `ChunkState`) sont stockées séparément** (plus complexe, mais peut être plus efficace pour les mises à jour partielles).
        *   *Recommandation initiale :* Stocker le `TransferState` complet sérialisé sous la clé `PlanID`.

3.  **Intégration du Backend DB dans `StateManager` :**
    *   [ ] **Ajouter la dépendance** à la librairie choisie dans `go.mod`.
    *   [ ] **Modifier `StateManagerConfig` :** Ajouter des champs pour le chemin du fichier de la base de données (ex: `DBPath string`).
    *   [ ] **Modifier `NewStateManager` :**
        *   Ouvrir la base de données à partir de `DBPath`.
        *   Stocker le handle de la DB (`*bolt.DB` ou `*badger.DB`) dans `stateManagerImpl`.
    *   [ ] **Ajouter une méthode `Close()` au `StateManager`** pour fermer proprement la base de données lors de l'arrêt de l'Orchestrateur. `cmd/orchestrator/main.go` devra appeler ce `Close()`.

4.  **Modification des Méthodes du `StateManager` pour la Persistance :**
    Pour chaque méthode qui modifie l'état (ex: `CreateTransfer`, `UpdateTransferStatus`, `LinkSchedulerPlan`, `UpdateChunkStatus`, `ReassignChunk`) :
    *   [ ] **Envelopper les opérations dans une transaction DB :**
        *   BoltDB : `db.Update(func(tx *bolt.Tx) error { ... })`.
        *   BadgerDB : `db.Update(func(txn *badger.Txn) error { ... })` ou `db.NewTransaction(true)`.
    *   [ ] **Opération 1 (Optionnel mais recommandé - WAL) :** Écrire l'intention de modification ou le delta dans un Write-Ahead Log (voir point 5).
    *   [ ] **Opération 2 : Modifier l'état en mémoire** (la map `sm.transfers`) comme c'est déjà le cas.
    *   [ ] **Opération 3 : Sérialiser** le `TransferState` affecté.
    *   [ ] **Opération 4 : Écrire l'état sérialisé dans la DB** à l'intérieur de la transaction.
        *   BoltDB : `bucket.Put([]byte(planID), serializedData)`.
        *   BadgerDB : `txn.Set([]byte(planID), serializedData)`.
    *   [ ] **Assurer l'atomicité :** Si l'écriture en DB échoue, la transaction doit être annulée (rollback), et l'état en mémoire ne devrait pas être considéré comme commis (ou devrait être reverti si possible, ou le système doit être dans un état où il peut se resynchroniser depuis la DB au prochain accès). Si un WAL est utilisé, le WAL n'est pas marqué comme commis.

5.  **Implémentation du Write-Ahead Log (WAL) (Optionnel pour une première version de persistance, mais crucial pour la durabilité ACID complète) :**
    *   [ ] **Choisir un format pour les entrées du WAL :** Peut être le message Protobuf de la requête originale, ou un message de "delta" décrivant le changement d'état.
    *   [ ] **Logique d'écriture dans le WAL :**
        *   Avant de modifier l'état en mémoire *et* avant de commettre la transaction DB.
        *   Ouvrir un fichier WAL en mode append.
        *   Écrire l'entrée sérialisée.
        *   `fsync()` le fichier WAL pour garantir l'écriture sur disque.
    *   [ ] **Pointer le dernier commit WAL :** Après un commit réussi en DB, mettre à jour un pointeur indiquant que toutes les entrées WAL jusqu'à ce point sont bien persistées en DB.
    *   [ ] **Gestion du WAL (Rotation, Purge) :** Nécessaire pour éviter que le WAL ne grossisse indéfiniment. Se fait généralement après un snapshot réussi de la DB.

6.  **Implémentation du Chargement de l'État au Démarrage (`LoadState`) :**
    *   [ ] **Modifier `NewStateManager` (ou ajouter une méthode `Initialize`)** pour appeler `LoadState`.
    *   [ ] **Logique de `LoadState` :**
        1.  **(Si WAL) :** Charger le dernier snapshot connu de la DB (voir point 7).
        2.  **(Si WAL) :** Rejouer toutes les entrées du WAL qui sont postérieures au snapshot. Pour chaque entrée WAL, désérialiser l'opération/delta et l'appliquer à l'état en mémoire (la map `sm.transfers`).
        3.  **(Si pas de WAL, ou après rejeu WAL) :** Lire tous les enregistrements de la DB (ex: itérer sur toutes les clés du bucket "transfers").
        4.  Pour chaque enregistrement, désérialiser le `TransferState` et le charger dans la map `sm.transfers`.
    *   [ ] **Gérer les erreurs de désérialisation** (données corrompues).

7.  **Implémentation du Snapshot de l'État (Optionnel avec WAL, mais bon pour réduire le temps de recovery) :**
    *   [ ] **Ajouter une méthode `SnapshotState` au `StateManager`.**
    *   [ ] **Logique de `SnapshotState` :**
        1.  Obtenir un état cohérent de la DB (peut nécessiter un read-lock ou une transaction read-only).
        2.  Copier le fichier de la DB (ou utiliser les mécanismes de backup de la DB si disponibles) vers un emplacement de snapshot.
        3.  Enregistrer le point jusqu'où le WAL a été couvert par ce snapshot.
        4.  Purger les anciennes entrées du WAL.
    *   [ ] **Planifier l'exécution de `SnapshotState`** (périodiquement via une goroutine dans le `StateManager` ou déclenché de l'extérieur).

8.  **Garbage Collection des Données Obsolètes (`GarbageCollect`) :**
    *   [ ] **Ajouter une méthode `GarbageCollect` au `StateManager`.**
    *   [ ] **Logique de `GarbageCollect` :**
        1.  Itérer sur les `sm.transfers` en mémoire (ou directement dans la DB).
        2.  Identifier les transferts terminés (COMPLETED, FAILED, CANCELLED) et plus vieux qu'un certain seuil (ex: `olderThan time.Time`).
        3.  Supprimer ces transferts de la map en mémoire ET de la base de données (dans une transaction).
        4.  (Si WAL) Écrire une entrée de suppression dans le WAL.
    *   [ ] **Planifier l'exécution de `GarbageCollect`** (périodiquement).

9.  **Tests Rigoureux :**
    *   [ ] **Tests unitaires** pour la sérialisation/désérialisation.
    *   [ ] **Tests d'intégration** pour les opérations CRUD sur l'état avec persistance.
    *   [ ] **Scénarios de récupération après panne :**
        *   Simuler un crash après écriture WAL mais avant commit DB. Vérifier que `LoadState` restaure correctement.
        *   Simuler un crash après commit DB mais avant purge WAL.
        *   Simuler un crash pendant un snapshot.
        *   Vérifier la corruption de la DB ou du WAL.
    *   [ ] **Benchmarks de performance** pour les opérations d'écriture et de lecture avec persistance.

10. **Considérations Avancées (pour plus tard) :**
    *   [ ] **Réplication de l'Orchestrateur :** Si vous avez plusieurs instances d'Orchestrateur, comment l'état est-il répliqué et maintenu cohérent (ex: Raft, Paxos, ou un service de DB distribué externe) ?
    *   [ ] **Migration de Schéma DB :** Si la structure de `TransferState` change, comment migrer les données existantes dans la DB ?


Le WAL et les snapshots ajoutent une complexité significative mais sont essentiels pour une durabilité forte.