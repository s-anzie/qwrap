Voici une **formulation claire, condensée et non redondante** de ta vision, parfaitement intégrable dans un document technique ou une spécification d’implémentation :

---

## 📘 Spécification — Répartition distribuée de fichiers par portions

### 🎯 Objectif

Permettre à un fichier global d’être réparti en **portions contiguës** détenues par différents agents, pour **paralléliser et distribuer les transferts**.

---

### 📦 Définition d'une portion

Une **portion** est un segment consécutif d’un fichier global. Chaque agent peut posséder **une ou plusieurs portions** d’un même fichier, décrites comme suit :

#### 🔖 Métadonnées minimales par portion détenue

| Champ                         | Description                                                        |
| ----------------------------- | ------------------------------------------------------------------ |
| `global_file_id`              | Identifiant unique du fichier global                               |
| `offset`                      | Position de début de la portion dans le fichier global (en octets) |
| `num_chunks`                  | Nombre de chunks dans cette portion (≥ 1)                          |
| `chunk_size`                  | Taille standard d’un chunk                                         |
| `last_chunk_size` (optionnel) | Taille du dernier chunk si différente                              |
| `chunk_index_start`           | Index du premier chunk global couvert                              |
| `chunk_index_end`             | Index du dernier chunk global couvert                              |
| `path_on_disk`                | Chemin local vers la donnée détenue                                |

**Contraintes :**

* Une portion contient **au moins un chunk**.
* Tous les chunks d’une portion sont **consécutifs** dans l’espace du fichier global.
* Aucun chevauchement entre portions, et chaque chunk appartient à **un seul agent** à la fois (sauf redondance volontaire).

---

### 🤖 Comportement attendu des composants

#### 🛰️ Agent (réponse à `GetFileMetadataRequest`)

* Évalue localement les portions qu’il détient pour un `global_file_id`.
* Renvoie une liste de `FilePortionInfo`, chacun décrivant une portion.
* Peut fournir ou non les métadonnées du fichier global (`global_file_metadata`).
* Doit garantir que les offsets et longueurs annoncés correspondent à la réalité disque.

#### 🧠 Orchestrateur

* Agrège les réponses de tous les agents.
* Construit une vision globale du fichier et de la disponibilité des portions.
* Déduit les métadonnées globales (`total_size`, etc.) si besoin.
* Transmet ces données au planificateur (scheduler).

#### 🗺️ Scheduler

* Décompose le fichier global en chunks.
* Pour chaque chunk, identifie les agents capables de le servir (grâce aux portions).
* Assigne chaque chunk à un agent valide (optimisation possible : charge, proximité, etc.).

#### 🧑‍💻 Client

* Exécute un plan de transfert multi-agent.
* Télécharge les chunks auprès des agents désignés.

---

## ✅ Plan d’action actualisé

### 📍 Étape 1 — Définir la structure `FilePortionInfo` dans `qwrap.proto`

* Inclure `offset`, `num_chunks`, `chunk_size`, etc.
* Mettre à jour `GetFileMetadataResponse` pour y intégrer `repeated FilePortionInfo available_portions`.

### 📍 Étape 2 — Côté Agent

* Implémenter un inventaire local des portions (codé en dur ou fichier JSON).
* Répondre dynamiquement à `GetFileMetadataRequest` avec les portions détenues.
* Fournir `global_file_metadata` si disponible (pas obligatoire).

### 📍 Étape 3 — Orchestrateur

* Modifier `discoverFileMetadataFromAgents` pour agréger les portions et déterminer les métadonnées globales.
* Transmettre toutes les portions agrégées au scheduler.

### 📍 Étape 4 — Scheduler

* Adapter `CreateTransferPlan` pour n’assigner un chunk qu’à un agent qui le détient.
* Prioriser agents selon heuristique (charge, redondance, etc.).

### 📍 Étape 5 — Test initial

* Simuler un fichier de 10 MB :

  * agent001 a les 0–5 MB (chunks 0–4)
  * agent002 a les 5–10 MB (chunks 5–9)
* Vérifier que chaque chunk est assigné à l’agent correct et transféré avec succès.

---

