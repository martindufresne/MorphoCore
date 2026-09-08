# MorphoCore

> **Moteur de Réseau Morphogénétique Concurrente, Décentralisé et Auto-Cicatrisant**  
> *Compatible Go standard, TinyGo bare-metal et WebAssembly (Wasm)*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Test](https://img.shields.io/badge/Go_Test-Pass-10b981?logo=go&logoColor=white)](.)
[![TinyGo](https://img.shields.io/badge/TinyGo-0.42.0-blue?logo=webassembly&logoColor=white)](.)
[![Wasm Size](https://img.shields.io/badge/Wasm_Binary-391_KB-purple)](.)
[![Zero Alloc](https://img.shields.io/badge/Post--Boot_Allocations-0_B%2Fop-emerald)](.)
[![Race Detector](https://img.shields.io/badge/Race_Detector-Clean-brightgreen)](.)

---

## Vue d'Ensemble

**MorphoCore** est une preuve de concept (PoC) explorant les architectures logicielles inspirées de la **morphogenèse biologique**. Le système simule un tissu de cellules concurrentes interconnectées en grille 2D mesh, gouvernées par un **ADN mathématique immuable**.

Face au bruit environnemental, aux corruptions mémoires stochastiques (bit-flips, coupures, falsifications), chaque cellule applique une politique de **tolérance zéro** :
1. **Apoptose immédiate** : dès qu'un invariant mathématique est violé, la cellule s'autodétruit, purge sa mémoire sensible (*memset*) et restitue son emplacement à un pool statique.
2. **Contournement dynamique** : le flux de messages est instantanément dévié par les voisins survivants via un routage par gradient adaptatif.
3. **Mitose décentralisée** : une cellule souche est élue de manière déterministe parmi les voisins immédiats pour réincarner le nœud défaillant sans aucun coordinateur central.
4. **Zero-Allocation Heap Post-Boot** : le moteur est durci pour les environnements embarqués / WebAssembly en éliminant toute allocation dynamique post-démarrage (`0 B/op`).

---

## Piliers d'Architecture

```
                           [Entrée Root (0,0)]
                                   │
                     ┌─────────────┼─────────────┐
                     ▼             ▼             ▼
              [Soma (0,0)] ── [Axone (1,0)] ── [Synapse (2,0)]
                     │             │             │
                     ▼             ▼             ▼
              [Soma (0,1)] ── [Axone (1,1)] ── [Synapse (2,1)]
                     │             │             │
                     ▼             ▼             ▼
              [Soma (0,2)] ── [Axone (1,2)] ── [Synapse (2,2)]
                                                 │
                                                 ▼
                                        [Sortie Sink (2,2)]
```

### 1. ADN Mathématique Immuable (`dna/`)
- Somme de contrôle stricte **FNV-1a 32-bit** et signatures mathématiques immuables.
- Typage biologique en trois familles fonctionnelles :
  - **Soma (x=0)** : réception, mise en forme et injection initiale.
  - **Axone (0 < x < N-1)** : propagation, transit et gradient de routage.
  - **Synapse (x=N-1)** : consolidation terminale et transmission vers la sortie.
- Zéro tolérance : toute corruption de bit ou troncature entraîne la fermeture instantanée du canal et le déclenchement de l'apoptose.

### 2. Élimination du Coordinateur Central (`cell/`, `protocol/`)
- Aucun chef d'orchestre ni goroutine maîtresse.
- Battement de cœur périodique P2P (`SignalPing`) et signaux typés (`SignalMitosisRequest`, `SignalMitosisAck`, `SignalRouteUpdate`).
- **Algorithme d'élection déterministe** : priorité cardinale `Ouest > Nord > Sud > Est` avec mécanisme de fallback différé (30 ms) pour absorber les pannes en cascade.

### 3. Topologie 2D Mesh & Contournement Dynamique (`cell/mesh.go`)
- Grille matricielle où chaque cellule maintient jusqu'à 4 liaisons cardinales.
- Routage par minimisation de la distance de Manhattan vers le point de sortie `(TargetX, TargetY)`.
- Résistance validée aux **pannes simultanées de nœuds adjacents** (ex: destruction simultanée de `(1,1)` et `(1,2)` avec déviation du trafic par les bordures sans perte de message).

### 4. Slab Allocator Statique & Durcissement TinyGo (`pool/`)
- **Réservoir statique pré-alloué** de taille fixe (`storage [18]CellSlot`).
- **0.00 B/op post-boot** : vérifié par `testing.AllocsPerRun`.
- **Purge explicite de la mémoire (*memset*)** : écrasement octet par octet à `0x00` de l'état sensible de la cellule à l'apoptose avant restitution du slot.
- Canaux et liaisons de voisinage `[4]*PeerLink` réutilisés sur place sans instanciation de maps ni appel à `new()`.

### 5. Arène WebAssembly & Passerelle JavaScript (`cmd/wasm/`)
- Module WebAssembly compilé via TinyGo avec un binaire final de **391 Ko** (< 500 Ko).
- Passerelle `syscall/js` exposant les primitives du réseau au DOM :
  - `morphoInjectPayload(str|bytes)` : injection de paquets dans la racine.
  - `morphoKillCell(x, y)` : apoptose forcée pour tests de résilience.
  - `morphoSetChaosRate(rate)` : injection stochastique d'erreurs en temps réel.
  - `morphoGetTelemetry()` : export structuré des métriques d'homéostasie et du pool.
- Émission bidirectionnelle d'événements vers `window.onMorphoCellEvent(x, y, state, generation, mttr)`.

---

## Arborescence du Projet

```text
morphocore/
├── README.md               # Documentation complète de l'architecture
├── go.mod                  # Déclaration de module Go
├── main.go                 # Banc d'essai natif CLI (1 000 cycles d'endurance)
├── dna/
│   ├── dna.go              # Invariants mathématiques, FNV-1a, familles biologiques
│   └── dna_test.go         # Validation des invariants et zéro-allocation ADN
├── cell/
│   ├── cell.go             # Cycle de vie cellulaire, canaux, apoptose et memset
│   ├── peer.go             # Liens de voisinage P2P et accusés de réception
│   ├── mesh.go             # Topologie grille 2D, routage par gradient, élection souche
│   ├── cell_test.go        # Tests unitaires du cycle de vie et de l'apoptose
│   └── mesh_test.go        # Tests de pannes simultanées et de contournement 2D
├── pool/
│   ├── pool.go             # Slab Allocator statique (CellPool, 18 slots fixes)
│   └── pool_test.go        # Validation stricte du 0 B/op, memset et concurrence
├── protocol/
│   ├── heartbeat.go        # Définition des signaux inter-cellules P2P
│   └── heartbeat_test.go   # Validation unitaire des charges utiles de signalisation
├── harness/
│   ├── chaos.go            # ChaosMonkey stochastique (bit-flips, troncature, déphasage)
│   ├── chaos_test.go       # Tests de distribution stochastique des anomalies
│   ├── telemetry.go        # Calculateur d'homéostasie, débit, MTTR et export JSON
│   └── telemetry_test.go   # Validation des métriques temporelles
└── cmd/
    └── wasm/
        ├── main.go         # Point d'entrée WebAssembly (pont syscall/js)
        ├── index.html      # Arène interactive WebAssembly (grille 3x3, logs, graphiques)
        ├── wasm_exec.js    # Runtime glue officiel TinyGo 0.42.0
        └── morphocore.wasm # Binaire WebAssembly compilé et optimisé (391 Ko)
```

---

## Installation & Prérequis

- **Go** : version 1.22 ou supérieure
- **TinyGo** : version 0.35+ (testé avec TinyGo 0.42.0)
- **Navigateur Web récent** : Chrome, Firefox, Safari ou Edge avec support WebAssembly.

---

## Guide d'Utilisation

### 1. Exécution des Tests Unitaires & Concurrence (Go standard)

Pour vérifier l'intégrité de tous les composants avec le détecteur de race conditions :

```bash
go test -v -count=1 -race ./...
```

### 2. Exécution des Tests avec TinyGo

```bash
tinygo test ./...
```

### 3. Validation de l'Allocateur Statique (Zéro Allocation)

```bash
go test -v -run TestPool_ZeroAllocationsPostBoot ./pool
```
*Sortie attendue : `0.00 B/op (0 allocs)`.*

### 4. Lancement de la Démonstration CLI Native (1 000 Cycles)

```bash
go run main.go
```

Ce banc d'essai exécute :
- **Phase 1** : Transit nominal de 200 paquets.
- **Phase 2** : Attaque groupée simultanée sur `(1,1)` et `(1,2)` sous flux continu intense.
- **Phase 3** : Attaque horizontale sur `(0,1)` et `(1,1)` avec élection décentralisée et fallback.
- **Bilan final** : Rapport complet d'homéostasie et état des slots du Slab Allocator.

---

## Arène WebAssembly Interactive

### Compilation du Binaire Wasm

```bash
tinygo build -no-debug -opt=z -o cmd/wasm/morphocore.wasm -target=wasm ./cmd/wasm/main.go
# Optimisation optionnelle via Binaryen
wasm-opt -Oz cmd/wasm/morphocore.wasm -o cmd/wasm/morphocore.wasm
```

### Démarrage de l'Arène Web

Lancez un serveur HTTP local servant le dossier `cmd/wasm` :

```bash
python3 -m http.server 8080 --directory cmd/wasm
```

Ouvrez ensuite votre navigateur sur : **[http://localhost:8080/index.html](http://localhost:8080/index.html)**

### Fonctionnalités de l'Arène :
- **Tir de précision** : Cliquez sur n'importe quelle cellule pour provoquer son apoptose ciblée.
- **Rafale simultanée** : Bouton d'attaque détruisant instantanément `(1,1)` et `(1,2)` pour observer en temps réel la déviation du gradient par les bordures.
- **Bombardement continu** : Curseur réglant le débit de messages (5 à 100 msg/s).
- **Curseur de Chaos** : Ajustement du taux d'injection d'erreurs stochastiques (0 à 50%).
- **Jauge Slab Allocator** : Visualisation en direct de l'occupation des 18 slots du pool et des purges mémoire *memset*.
- **Export de Session JSON** : Téléchargement direct des métriques de résilience.

---

## Résultats & Métriques de Résilience

| Métrique | Valeur Typique | Description |
|---|---|---|
| **MTTR Moyen (Temps de Cicatrisation)** | **~21 µs à 200 µs** | Temps écoulé entre l'apoptose et la reprise effective du flux |
| **Taux de Survie Réseau** | **100.0%** | Zéro crash runtime, absorption totale des pannes |
| **Allocations Post-Boot** | **0 B/op** | Aucune pression sur le Garbage Collector |
| **Capacité du Pool Statique** | **18 slots** | 9 cellules actives + 9 cellules de marge mitotique |
| **Taille du Binaire Wasm** | **391 Ko** | Chargement instantané (< 500 Ko) |

---

## Licence

Projet développé sous licence MIT. Libre d'utilisation, de modification et d'intégration.
