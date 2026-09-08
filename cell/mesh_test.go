package cell_test

import (
	"context"
	"fmt"
	"morphocore/cell"
	"morphocore/harness"
	"morphocore/pool"
	"sync"
	"testing"
	"time"
)

// TestMesh_NominalGridRouting vérifie l'acheminement nominal de bout en bout sur une grille 3x3 (0,0 -> 2,2).
func TestMesh_NominalGridRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tracker := harness.NewMetricsTracker()
	tracker.Start()
	defer tracker.Stop()

	rootIn := make(chan []byte, 200)
	rootOut := make(chan []byte, 200)

	grid := cell.BuildMeshGrid(3, 3, tracker, nil, rootIn, rootOut)

	// Lancement des 9 goroutines de la grille 3x3
	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	const msgCount = 50
	doneCh := make(chan struct{})

	// Consommateur sur rootOut (cellule terminale (2,2))
	var receivedCount int
	go func() {
		defer close(doneCh)
		for i := 0; i < msgCount; i++ {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-rootOut:
				if !ok {
					return
				}
				payload, expectedHash, okFormat := cell.UnpackMessage(msg)
				if !okFormat {
					t.Errorf("Message #%d mal formé", i)
					return
				}
				if !grid[2][2].Genome.Verify(payload, expectedHash) {
					t.Errorf("Message #%d hash invalide", i)
					return
				}
				receivedCount++
			}
		}
	}()

	// Émission des messages nominaux sur rootIn (cellule d'entrée (0,0))
	for i := 0; i < msgCount; i++ {
		payload := []byte(fmt.Sprintf("mesh-packet-%03d", i))
		rootIn <- cell.PackMessage(payload)
	}

	select {
	case <-doneCh:
		if receivedCount != msgCount {
			t.Fatalf("Attendu %d messages reçus, obtenu %d", msgCount, receivedCount)
		}
	case <-ctx.Done():
		t.Fatalf("Timeout en attente des messages nominaux (reçus: %d/%d)", receivedCount, msgCount)
	}
}

// TestMesh_SingleNodeBypassAndRegeneration vérifie que lorsqu'un nœud central (1,1) subit une apoptose,
// le trafic contourne le nœud sans blocage et la cellule est régénérée de manière autonome.
func TestMesh_SingleNodeBypassAndRegeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tracker := harness.NewMetricsTracker()
	tracker.Start()
	defer tracker.Stop()

	rootIn := make(chan []byte, 200)
	rootOut := make(chan []byte, 200)

	grid := cell.BuildMeshGrid(3, 3, tracker, nil, rootIn, rootOut)

	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	// 1. Envoi de quelques messages nominaux initiaux
	for i := 0; i < 10; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("pre-kill-%02d", i)))
	}

	// 2. Destruction ciblée du nœud central (1,1) via injection d'un message corrompu
	killMsg := cell.PackCorruptedMessage([]byte("corrupted-payload-for-1-1"))
	grid[1][1].InChan <- killMsg

	// 3. Envoi continu de messages nominaux pendant l'apoptose et la mitose
	const postKillCount = 30
	for i := 0; i < postKillCount; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("post-kill-%02d", i)))
		time.Sleep(2 * time.Millisecond)
	}

	// 4. Vérification de la réception
	totalExpected := 10 + postKillCount
	var received int
	timeout := time.After(2 * time.Second)

	for received < totalExpected {
		select {
		case <-rootOut:
			received++
		case <-timeout:
			t.Fatalf("Timeout: reçu %d/%d messages. Le flux a calé pendant le contournement.", received, totalExpected)
		case <-ctx.Done():
			t.Fatalf("Context annulé avant la fin")
		}
	}

	// 5. Vérifie que l'apoptose et la mitose ont bien eu lieu
	snap := tracker.Snapshot()
	if snap.TotalApoptosis < 1 {
		t.Errorf("Attendu au moins 1 apoptose, obtenu %d", snap.TotalApoptosis)
	}
	if snap.TotalMitosis < 1 {
		t.Errorf("Attendu au moins 1 mitose P2P, obtenu %d", snap.TotalMitosis)
	}
}

// TestMesh_SimultaneousAdjacentFailure teste la destruction simultanée de deux cellules adjacentes
// (ex: (1,1) et (1,2)) en plein transit. Vérifie le contournement dynamique et la double mitose parallèle.
func TestMesh_SimultaneousAdjacentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	tracker := harness.NewMetricsTracker()
	tracker.Start()
	defer tracker.Stop()

	rootIn := make(chan []byte, 300)
	rootOut := make(chan []byte, 300)

	grid := cell.BuildMeshGrid(3, 3, tracker, nil, rootIn, rootOut)

	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	// 1. Échauffement nominal
	for i := 0; i < 15; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("warmup-%02d", i)))
	}

	// 2. Déclenchement d'une attaque groupée SIMULTANÉE sur deux nœuds adjacents (1,1) et (1,2)
	var attackWg sync.WaitGroup
	attackWg.Add(2)

	go func() {
		defer attackWg.Done()
		grid[1][1].InChan <- cell.PackCorruptedMessage([]byte("group-attack-(1,1)"))
	}()

	go func() {
		defer attackWg.Done()
		grid[1][2].InChan <- cell.PackCorruptedMessage([]byte("group-attack-(1,2)"))
	}()

	attackWg.Wait()

	// 3. Injection continue de charge pendant la double mitose et le contournement
	const stressCount = 40
	for i := 0; i < stressCount; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("stress-packet-%02d", i)))
		time.Sleep(2 * time.Millisecond)
	}

	// 4. Vérification de la collecte sans blocage
	totalExpected := 15 + stressCount
	var received int
	timeout := time.After(3 * time.Second)

	for received < totalExpected {
		select {
		case <-rootOut:
			received++
		case <-timeout:
			t.Fatalf("Timeout: reçu %d/%d messages. Le flux a calé lors de l'attaque groupée.", received, totalExpected)
		case <-ctx.Done():
			t.Fatalf("Context annulé")
		}
	}

	// 5. Vérification télémétrique : au moins 2 apoptoses et au moins 2 mitoses distinctes
	snap := tracker.Snapshot()
	if snap.TotalApoptosis < 2 {
		t.Errorf("Attendu au moins 2 apoptoses lors de l'attaque groupée, obtenu %d", snap.TotalApoptosis)
	}
	if snap.TotalMitosis < 2 {
		t.Errorf("Attendu au moins 2 mitoses menées en parallèle, obtenu %d", snap.TotalMitosis)
	}
}

// TestMesh_HorizontalAdjacentFailure teste la destruction simultanée de deux cellules adjacentes
// horizontalement (0,1) et (1,1). Vérifie que l'élection avec fallback garantit la régénération complète.
func TestMesh_HorizontalAdjacentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	tracker := harness.NewMetricsTracker()
	tracker.Start()
	defer tracker.Stop()

	rootIn := make(chan []byte, 300)
	rootOut := make(chan []byte, 300)

	grid := cell.BuildMeshGrid(3, 3, tracker, nil, rootIn, rootOut)

	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	// 1. Échauffement nominal
	for i := 0; i < 15; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("warmup-h-%02d", i)))
	}

	// 2. Attaque simultanée sur (0,1) et (1,1)
	var attackWg sync.WaitGroup
	attackWg.Add(2)

	go func() {
		defer attackWg.Done()
		grid[0][1].InChan <- cell.PackCorruptedMessage([]byte("group-attack-(0,1)"))
	}()

	go func() {
		defer attackWg.Done()
		grid[1][1].InChan <- cell.PackCorruptedMessage([]byte("group-attack-(1,1)"))
	}()

	attackWg.Wait()

	// 3. Injection continue pendant la régénération
	const stressCount = 35
	for i := 0; i < stressCount; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("stress-h-%02d", i)))
		time.Sleep(2 * time.Millisecond)
	}

	// 4. Collecte
	totalExpected := 15 + stressCount
	var received int
	timeout := time.After(3 * time.Second)

	for received < totalExpected {
		select {
		case <-rootOut:
			received++
		case <-timeout:
			t.Fatalf("Timeout: reçu %d/%d messages. Le flux a calé lors de l'attaque horizontale.", received, totalExpected)
		case <-ctx.Done():
			t.Fatalf("Context annulé")
		}
	}

	snap := tracker.Snapshot()
	if snap.TotalApoptosis < 2 {
		t.Errorf("Attendu au moins 2 apoptoses, obtenu %d", snap.TotalApoptosis)
	}
	if snap.TotalMitosis < 2 {
		t.Errorf("Attendu au moins 2 mitoses, obtenu %d", snap.TotalMitosis)
	}
}

// TestMesh_OppositeDirection vérifie la cohérence des directions cardinales opposées.
func TestMesh_OppositeDirection(t *testing.T) {
	cases := []struct {
		dir  cell.Direction
		opp  cell.Direction
		name string
	}{
		{cell.DirNorth, cell.DirSouth, "Nord/Sud"},
		{cell.DirSouth, cell.DirNorth, "Sud/Nord"},
		{cell.DirEast, cell.DirWest, "Est/Ouest"},
		{cell.DirWest, cell.DirEast, "Ouest/Est"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.dir.Opposite() != tc.opp {
				t.Fatalf("%s.Opposite() = %s, attendu %s", tc.dir, tc.dir.Opposite(), tc.opp)
			}
			if tc.dir.String() == "" {
				t.Fatalf("String() ne doit pas être vide pour %d", tc.dir)
			}
		})
	}
}

// TestMesh_WithCellPoolIntegration valide l'exécution complète d'une grille 3x3 adossée
// au pool statique, avec destruction et régénération cellulaire sans dépassement de capacité.
func TestMesh_WithCellPoolIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	tracker := harness.NewMetricsTracker()
	tracker.Start()
	defer tracker.Stop()

	cellPool := pool.NewCellPool()
	rootIn := make(chan []byte, 300)
	rootOut := make(chan []byte, 300)

	grid := cell.BuildMeshGridWithPool(3, 3, cellPool, tracker, nil, rootIn, rootOut)

	for x := 0; x < 3; x++ {
		for y := 0; y < 3; y++ {
			go grid[x][y].Run(ctx)
		}
	}

	// 9 cellules initialement allouées
	if cellPool.ActiveCount() != 9 {
		t.Fatalf("Attendu 9 cellules actives au boot, obtenu: %d", cellPool.ActiveCount())
	}

	// Phase 1 : 20 messages nominaux
	for i := 0; i < 20; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("pool-test-msg-%02d", i)))
	}

	// Phase 2 : Attaque ciblée sur nœud (1,1)
	grid[1][1].InChan <- cell.PackCorruptedMessage([]byte("kill-(1,1)"))

	// Phase 3 : Flux continu de 30 messages pendant le recyclage
	for i := 0; i < 30; i++ {
		rootIn <- cell.PackMessage([]byte(fmt.Sprintf("pool-post-kill-%02d", i)))
		time.Sleep(2 * time.Millisecond)
	}

	totalExpected := 50
	var received int
	timeout := time.After(3 * time.Second)

	for received < totalExpected {
		select {
		case <-rootOut:
			received++
		case <-timeout:
			t.Fatalf("Timeout: reçu %d/%d messages avec CellPool", received, totalExpected)
		case <-ctx.Done():
			t.Fatalf("Context annulé")
		}
	}

	// Attente brève de stabilisation
	time.Sleep(50 * time.Millisecond)

	// Validation du comportement du pool statique
	if cellPool.MaxActiveCount() > pool.MaxCells {
		t.Errorf("Le pic d'actifs (%d) a dépassé la capacité statique MaxCells (%d)",
			cellPool.MaxActiveCount(), pool.MaxCells)
	}
	if cellPool.TotalAcquired() <= 9 {
		t.Errorf("Attendu au moins 1 acquisition de mitose au-delà des 9 initiales, obtenu total: %d",
			cellPool.TotalAcquired())
	}
	if cellPool.TotalReleased() < 1 {
		t.Errorf("Attendu au moins 1 libération (apoptose), obtenu: %d", cellPool.TotalReleased())
	}
	if cellPool.ActiveCount() != 9 {
		t.Errorf("Le nombre de cellules actives final devrait être revenu à 9, obtenu: %d",
			cellPool.ActiveCount())
	}
}
