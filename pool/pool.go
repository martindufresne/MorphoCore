package pool

import (
	"errors"
	"morphocore/cell"
	"morphocore/dna"
	"sync"
)

// MaxCells dimensionne la capacité statique du pool pour une grille 3x3 (9 cellules)
// avec une marge de réincarnation fixe pour absorber le chevauchement mitotique sans allocation.
const MaxCells = 18

var (
	// ErrPoolExhausted est renvoyé lorsqu'aucun slot n'est disponible dans le pool statique.
	ErrPoolExhausted = errors.New("cell pool exhausted: no free slot available")
)

// CellSlot représente un conteneur statique fixe d'une cellule recyclable.
type CellSlot struct {
	InUse      bool
	SlotIndex  uint8
	Generation uint16
	Cell       cell.Cell
}

// CellPool implémente un Slab Allocator statique à zéro allocation dynamique post-bootstrap.
// Conforme aux contraintes strictes TinyGo (bare-metal et WebAssembly) :
// aucun appel à new(), aucune création dynamique incontrôlée, aucune réflexion.
type CellPool struct {
	mu            sync.Mutex
	storage       [MaxCells]CellSlot
	activeCount   int
	maxActive     int
	totalAcquired int
	totalReleased int
}

// NewCellPool initialise et retourne le réservoir statique de cellules pré-allouées.
func NewCellPool() *CellPool {
	p := &CellPool{}
	for i := 0; i < MaxCells; i++ {
		p.storage[i].SlotIndex = uint8(i)
		p.storage[i].InUse = false
		p.storage[i].Generation = 1
		p.storage[i].Cell.SlotID = i
		p.storage[i].Cell.Pool = p
		p.storage[i].Cell.State = p.storage[i].Cell.StateBuf[:0]
	}
	return p
}

// Acquire réserve un emplacement libre dans le pool statique sans aucune allocation sur le tas.
// Réinitialise les registres internes et incrémente la génération de l'emplacement.
func (p *CellPool) Acquire(family dna.Family, x, y uint8) (*cell.Cell, uint16, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < MaxCells; i++ {
		slot := &p.storage[i]
		if !slot.InUse {
			slot.InUse = true
			slot.Generation++
			if slot.Generation == 0 {
				slot.Generation = 1
			}

			// Réinitialisation des registres internes sans passage par l'allocateur général
			slot.Cell.Family = family
			slot.Cell.Generation = slot.Generation
			slot.Cell.X = int(x)
			slot.Cell.Y = int(y)
			slot.Cell.IsGrid = true
			slot.Cell.Genome = dna.NewGenome(family)
			slot.Cell.SlotID = i
			slot.Cell.Pool = p
			slot.Cell.State = slot.Cell.StateBuf[:0]

			p.activeCount++
			if p.activeCount > p.maxActive {
				p.maxActive = p.activeCount
			}
			p.totalAcquired++

			return &slot.Cell, slot.Generation, nil
		}
	}

	return nil, 0, ErrPoolExhausted
}

// Release exécute une purge explicite de la mémoire (memset) et restitue le slot au pool.
func (p *CellPool) Release(c *cell.Cell) {
	if c == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	idx := c.SlotID
	if idx >= 0 && idx < MaxCells {
		slot := &p.storage[idx]
		if slot.InUse {
			slot.InUse = false

			// Purge explicite octet par octet (memset) des données sensibles
			for i := range slot.Cell.StateBuf {
				slot.Cell.StateBuf[i] = 0
			}
			slot.Cell.State = slot.Cell.StateBuf[:0]

			p.activeCount--
			p.totalReleased++
		}
	}
}

// ActiveCount retourne le nombre de slots actuellement alloués.
func (p *CellPool) ActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeCount
}

// MaxActiveCount retourne le pic maximal de slots utilisés simultanément.
func (p *CellPool) MaxActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

// TotalAcquired retourne le nombre cumulé d'acquisitions de slots.
func (p *CellPool) TotalAcquired() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalAcquired
}

// TotalReleased retourne le nombre cumulé de restitutions de slots.
func (p *CellPool) TotalReleased() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalReleased
}

// Capacity retourne la capacité maximale statique du pool.
func (p *CellPool) Capacity() int {
	return MaxCells
}

// GetCellAt retourne la cellule active aux coordonnées (x, y) spécifiées, ou nil si aucune.
func (p *CellPool) GetCellAt(x, y int) *cell.Cell {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < MaxCells; i++ {
		if p.storage[i].InUse && p.storage[i].Cell.X == x && p.storage[i].Cell.Y == y {
			return &p.storage[i].Cell
		}
	}
	return nil
}
