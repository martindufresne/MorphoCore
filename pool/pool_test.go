package pool_test

import (
	"morphocore/cell"
	"morphocore/dna"
	"morphocore/pool"
	"sync"
	"testing"
)

// TestPool_AcquireAndRelease valide l'acquisition et la restitution nominales dans le pool.
func TestPool_AcquireAndRelease(t *testing.T) {
	p := pool.NewCellPool()

	if p.Capacity() != pool.MaxCells {
		t.Fatalf("Capacité attendue: %d, obtenue: %d", pool.MaxCells, p.Capacity())
	}
	if p.ActiveCount() != 0 {
		t.Fatalf("ActiveCount attendu à 0, obtenu: %d", p.ActiveCount())
	}

	c, gen, err := p.Acquire(dna.FamilySoma, 0, 0)
	if err != nil {
		t.Fatalf("Acquire échoué: %v", err)
	}
	if c == nil {
		t.Fatalf("Pointeur de cellule nil retourné")
	}
	if gen == 0 {
		t.Fatalf("Génération invalide: %d", gen)
	}
	if c.Family != dna.FamilySoma {
		t.Fatalf("Famille attendue: %s, obtenue: %s", dna.FamilySoma, c.Family)
	}
	if c.X != 0 || c.Y != 0 {
		t.Fatalf("Coordonnées invalides: (%d,%d)", c.X, c.Y)
	}
	if p.ActiveCount() != 1 {
		t.Fatalf("ActiveCount attendu à 1, obtenu: %d", p.ActiveCount())
	}

	p.Release(c)

	if p.ActiveCount() != 0 {
		t.Fatalf("ActiveCount attendu à 0 après Release, obtenu: %d", p.ActiveCount())
	}
	if p.TotalAcquired() != 1 || p.TotalReleased() != 1 {
		t.Fatalf("Statistiques incohérentes: Acquired=%d, Released=%d",
			p.TotalAcquired(), p.TotalReleased())
	}
}

// TestPool_CapacityAndExhaustion vérifie que le pool sature à MaxCells et refuse de déborder.
func TestPool_CapacityAndExhaustion(t *testing.T) {
	p := pool.NewCellPool()

	cells := make([]*cell.Cell, pool.MaxCells)
	for i := 0; i < pool.MaxCells; i++ {
		c, _, err := p.Acquire(dna.FamilyAxone, uint8(i%3), uint8(i/3))
		if err != nil {
			t.Fatalf("Acquire #%d a échoué inopinément: %v", i, err)
		}
		cells[i] = c
	}

	if p.ActiveCount() != pool.MaxCells {
		t.Fatalf("ActiveCount attendu à %d, obtenu: %d", pool.MaxCells, p.ActiveCount())
	}

	// Tentative d'acquisition au-delà de la capacité
	overflowCell, _, err := p.Acquire(dna.FamilySynapse, 2, 2)
	if err != pool.ErrPoolExhausted || overflowCell != nil {
		t.Fatalf("Attendu ErrPoolExhausted, obtenu: %v, cell=%v", err, overflowCell)
	}

	// Relâche la première cellule acquise
	p.Release(cells[0])

	if p.ActiveCount() != pool.MaxCells-1 {
		t.Fatalf("ActiveCount attendu à %d après libération, obtenu: %d", pool.MaxCells-1, p.ActiveCount())
	}

	// Nouvelle acquisition désormais possible
	reacquired, _, err := p.Acquire(dna.FamilySynapse, 2, 2)
	if err != nil || reacquired == nil {
		t.Fatalf("Ré-acquisition échouée après libération: %v", err)
	}
}

// TestPool_MemsetPurge vérifie l'écrasement explicite octet par octet des données sensibles lors du Release.
func TestPool_MemsetPurge(t *testing.T) {
	p := pool.NewCellPool()

	c, _, err := p.Acquire(dna.FamilySynapse, 2, 2)
	if err != nil {
		t.Fatalf("Acquire échoué: %v", err)
	}

	// Écriture de données sensibles dans le buffer d'état
	for i := 0; i < len(c.StateBuf); i++ {
		c.StateBuf[i] = 0xAA
	}
	c.State = c.StateBuf[:32]

	// Restitution au pool avec memset obligatoire
	p.Release(c)

	// Vérification de la purge totale à zéro
	for i, b := range c.StateBuf {
		if b != 0x00 {
			t.Fatalf("Octet sensible #%d non purgé (valeur: 0x%02X)", i, b)
		}
	}
	if len(c.State) != 0 {
		t.Fatalf("Slice State non réinitialisée (len: %d)", len(c.State))
	}
}

// TestPool_ZeroAllocationsPostBoot vérifie strictement le critère 0 B/op après le boot.
func TestPool_ZeroAllocationsPostBoot(t *testing.T) {
	p := pool.NewCellPool()

	// Échauffement (warm-up)
	warmup, _, err := p.Acquire(dna.FamilySoma, 0, 0)
	if err != nil {
		t.Fatalf("Warmup échoué: %v", err)
	}
	p.Release(warmup)

	// Mesure des allocations sur 1 000 itérations consécutives
	allocs := testing.AllocsPerRun(1000, func() {
		cellRef, gen, acqErr := p.Acquire(dna.FamilyAxone, 1, 1)
		if acqErr != nil || cellRef == nil || gen == 0 {
			t.Fatalf("Acquire échoué dans AllocsPerRun")
		}
		p.Release(cellRef)
	})

	if allocs > 0 {
		t.Fatalf("ALERTE ALLOCATION: %f allocations/op détectées (Attendu: 0.0 B/op)", allocs)
	}
}

// TestPool_ConcurrentStress teste la robustesse sous concurrence intense et l'absence de race condition.
func TestPool_ConcurrentStress(t *testing.T) {
	p := pool.NewCellPool()

	const workers = 8
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				c, _, err := p.Acquire(dna.FamilyAxone, uint8(workerID%3), uint8(i%3))
				if err == nil && c != nil {
					p.Release(c)
				}
			}
		}(w)
	}

	wg.Wait()

	if p.ActiveCount() != 0 {
		t.Fatalf("Tous les slots devraient être libérés, restant actifs: %d", p.ActiveCount())
	}
	if p.MaxActiveCount() > pool.MaxCells {
		t.Fatalf("MaxActiveCount (%d) a dépassé la capacité statique (%d)",
			p.MaxActiveCount(), pool.MaxCells)
	}
}
