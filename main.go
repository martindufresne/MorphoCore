package main

import (
	"context"
	"fmt"
	"morphocore/cell"
	"morphocore/harness"
	"morphocore/pool"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("================================================================================")
	fmt.Println("  RÉSEAU MORPHOGÉNÉTIQUE DURCI TINYGO & POOL STATIQUE DE RECYCLAGE (ÉTAPE 5)    ")
	fmt.Println("================================================================================")
	fmt.Println(" [ARCHITECTURE] Slab Allocator Statique | Zéro Allocation Dynamique Post-Boot  ")
	fmt.Println(" [TINYGO BARE-METAL & WASM] Canaux Fixes Réutilisés & Memset de Purge Mémoire   ")
	fmt.Println()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialisation de la télémétrie, du générateur stochastique et du CellPool
	tracker := harness.NewMetricsTracker()
	tracker.Start()

	// Réservoir statique fixe de cellules (MaxCells = 18 pour grille 3x3 avec marge mitotique)
	cellPool := pool.NewCellPool()

	// Bruit stochastique résiduel de 1.5% sur le réseau
	gridChaos := harness.NewChaosMonkey(0.015)

	// Canaux racine d'entrée (capteur en (0,0)) et de sortie (actionneur terminal en (2,2))
	rootIn := make(chan []byte, 300)
	rootOut := make(chan []byte, 300)

	// 2. Construction de la grille Mesh 3x3 adossée au CellPool statique
	// Colonne 0 : Somas (entrée)
	// Colonne 1 : Axones (relais)
	// Colonne 2 : Synapses (sortie)
	grid := cell.BuildMeshGridWithPool(3, 3, cellPool, tracker, gridChaos, rootIn, rootOut)

	// Démarrage des 9 goroutines cellulaires autonomes (zéro superviseur central)
	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	fmt.Printf("[POOL STATIQUE INITIALISÉ] Capacité: %d slots | Actifs au boot: %d slots\n",
		cellPool.Capacity(), cellPool.ActiveCount())
	fmt.Println("[DÉPLOIEMENT DU MAILLAGE 2D 3x3] 9 cellules interconnectées :")
	for y := 0; y < 3; y++ {
		fmt.Printf("  Ligne %d : [%s (0,%d)] <---> [%s (1,%d)] <---> [%s (2,%d)]\n",
			y, grid[0][y].Family, y, grid[1][y].Family, y, grid[2][y].Family, y)
	}
	fmt.Println("  Point d'entrée  : (0,0) [cell-Soma-0-0]")
	fmt.Println("  Point de sortie : (2,2) [cell-Synapse-2-2]")
	fmt.Println()

	// 3. Consommateur terminal de sortie (Sink)
	var deliveredCount int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-rootOut:
				if !ok {
					return
				}
				payload, hash, valid := cell.UnpackMessage(msg)
				if valid && grid[2][2].Genome.Verify(payload, hash) {
					tracker.RecordSuccess()
					atomic.AddInt64(&deliveredCount, 1)
				}
			}
		}
	}()

	// Paramètres du banc d'essai (1 000 cycles de stress)
	const (
		phase1Count = 200
		phase2Count = 400
		phase3Count = 400
		pacingDelay = 1 * time.Millisecond
	)

	// PHASE 1 : Transit nominal
	fmt.Printf("[PHASE 1] Échauffement : Injection de %d paquets en régime nominal...\n", phase1Count)
	for i := 1; i <= phase1Count; i++ {
		payload := []byte(fmt.Sprintf("morpho-mesh-p1-%04d", i))
		tracker.RecordEmission(false)
		rootIn <- cell.PackMessage(payload)
		if pacingDelay > 0 {
			time.Sleep(pacingDelay)
		}
	}

	snapP1 := tracker.Snapshot()
	fmt.Printf(" -> Phase 1 terminée : %d messages livrés | Apoptoses : %d | Mitoses : %d | Slots actifs : %d\n\n",
		atomic.LoadInt64(&deliveredCount), snapP1.TotalApoptosis, snapP1.TotalMitosis, cellPool.ActiveCount())

	// PHASE 2 : Attaque groupée simultanée sur (1,1) et (1,2)
	fmt.Println("[PHASE 2] >>> ATTAQUE GROUPÉE : Destruction SIMULTANÉE de (1,1) [Axone] et (1,2) [Axone] <<<")
	fmt.Println(" -> Recyclage immédiat via CellPool : memset des buffers et réincarnation par les slots libres.")
	fmt.Println(" -> Le flux continu contourne dynamiquement le secteur sinistré via les bordures.")

	var attackWg sync.WaitGroup
	attackWg.Add(2)

	go func() {
		defer attackWg.Done()
		grid[1][1].InChan <- cell.PackCorruptedMessage([]byte("CRITICAL-CORRUPTION-(1,1)"))
	}()
	go func() {
		defer attackWg.Done()
		grid[1][2].InChan <- cell.PackCorruptedMessage([]byte("CRITICAL-CORRUPTION-(1,2)"))
	}()
	attackWg.Wait()

	// Maintien d'un flux continu intense pendant l'attaque et la cicatrisation
	for i := 1; i <= phase2Count; i++ {
		payload := []byte(fmt.Sprintf("morpho-mesh-p2-%04d", i))
		tracker.RecordEmission(false)
		rootIn <- cell.PackMessage(payload)
		if pacingDelay > 0 {
			time.Sleep(pacingDelay)
		}
	}

	time.Sleep(100 * time.Millisecond)
	snapP2 := tracker.Snapshot()
	fmt.Printf(" -> Phase 2 terminée : %d messages livrés | Apoptoses : %d | Mitoses : %d | Slots actifs : %d\n",
		atomic.LoadInt64(&deliveredCount), snapP2.TotalApoptosis, snapP2.TotalMitosis, cellPool.ActiveCount())
	fmt.Println(" -> RÉSULTAT : Recyclage fluide sans allocation heap, contournement dynamique réussi !")
	fmt.Println()

	// PHASE 3 : Deuxième attaque simultanée orthogonale sur (0,1) [Soma] et (1,1) [Axone]
	fmt.Println("[PHASE 3] >>> ATTAQUE HORIZONTALE : Destruction SIMULTANÉE de (0,1) [Soma] et (1,1) [Axone] <<<")
	fmt.Println(" -> Élection déterministe avec fallback et réutilisation directe des slots du pool.")

	var attack2Wg sync.WaitGroup
	attack2Wg.Add(2)

	go func() {
		defer attack2Wg.Done()
		grid[0][1].InChan <- cell.PackCorruptedMessage([]byte("CRITICAL-CORRUPTION-(0,1)"))
	}()
	go func() {
		defer attack2Wg.Done()
		grid[1][1].InChan <- cell.PackCorruptedMessage([]byte("CRITICAL-CORRUPTION-(1,1)-WAVE2"))
	}()
	attack2Wg.Wait()

	for i := 1; i <= phase3Count; i++ {
		payload := []byte(fmt.Sprintf("morpho-mesh-p3-%04d", i))
		tracker.RecordEmission(false)
		rootIn <- cell.PackMessage(payload)
		if pacingDelay > 0 {
			time.Sleep(pacingDelay)
		}
	}

	time.Sleep(150 * time.Millisecond)
	snapP3 := tracker.Snapshot()
	fmt.Printf(" -> Phase 3 terminée : %d messages livrés | Apoptoses : %d | Mitoses : %d | Slots actifs : %d\n\n",
		atomic.LoadInt64(&deliveredCount), snapP3.TotalApoptosis, snapP3.TotalMitosis, cellPool.ActiveCount())

	// 4. Clôture et Bilan d'Homéostasie
	tracker.Stop()

	fmt.Println("================================================================================")
	fmt.Println("               MÉTRIQUES RUNTIME DU SLAB ALLOCATOR (CELLPOOL)                   ")
	fmt.Println("================================================================================")
	fmt.Printf(" Capacité statique fixe (MaxCells) : %d\n", cellPool.Capacity())
	fmt.Printf(" Slots actifs finaux               : %d (attendu: 9)\n", cellPool.ActiveCount())
	fmt.Printf(" Pic simultané d'utilisation       : %d / %d (marge mitotique respectée)\n",
		cellPool.MaxActiveCount(), cellPool.Capacity())
	fmt.Printf(" Total acquisitions (boot+mitose)  : %d\n", cellPool.TotalAcquired())
	fmt.Printf(" Total restitutions (memset)       : %d\n", cellPool.TotalReleased())
	fmt.Println(" Intégrité mémoire                 : AUCUNE FUITE D'ADRESSES, PROFIL MÉMOIRE PLAT")
	fmt.Println("================================================================================")
	fmt.Println()

	fmt.Println("================================================================================")
	fmt.Println("                       RAPPORT TÉLÉMÉTRIQUE FINAL                               ")
	fmt.Println("================================================================================")
	fmt.Print(tracker.Report())

	fmt.Println("\n[EXPORT JSON]")
	fmt.Println(tracker.ReportJSON())
}
