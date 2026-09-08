package dna

// Family représente le type fonctionnel d'une cellule dans le réseau morphogénétique.
type Family uint8

const (
	FamilySoma Family = iota
	FamilyAxone
	FamilySynapse
)

// String retourne la représentation textuelle de la famille cellulaire.
func (f Family) String() string {
	switch f {
	case FamilySoma:
		return "Soma"
	case FamilyAxone:
		return "Axone"
	case FamilySynapse:
		return "Synapse"
	default:
		return "Unknown"
	}
}

// Genome définit l'ADN mathématique immuable qui régit la cellule et ses invariants.
type Genome struct {
	FamilyID Family
}

// NewGenome instancie un nouveau génome pour une famille donnée.
func NewGenome(family Family) Genome {
	return Genome{FamilyID: family}
}

const (
	fnvOffset32 uint32 = 2166136261
	fnvPrime32  uint32 = 16777619
)

// Checksum calcule une somme de contrôle stricte Fowler-Noll-Vo (FNV-1a) 32-bit du payload.
// Conforme TinyGo : 0 allocation mémoire, aucun appel CGO, exécution en registres.
func Checksum(payload []byte) uint32 {
	h := fnvOffset32
	for _, b := range payload {
		h ^= uint32(b)
		h *= fnvPrime32
	}
	return h
}

// Verify calcule la somme de contrôle stricte de payload et retourne false en cas de divergence
// avec expectedHash.
func (g Genome) Verify(payload []byte, expectedHash uint32) bool {
	if payload == nil {
		return false
	}
	return Checksum(payload) == expectedHash
}

// Mutate génère le hash mathématique invariant attendu pour la prochaine génération (generation + 1).
func (g Genome) Mutate(generation uint16) uint32 {
	return g.GenerationHash(generation + 1)
}

// GenerationHash calcule l'invariant mathématique déterministe pour une génération donnée et la famille associée.
func (g Genome) GenerationHash(generation uint16) uint32 {
	h := fnvOffset32
	h ^= uint32(g.FamilyID)
	h *= fnvPrime32
	h ^= uint32(generation >> 8)
	h *= fnvPrime32
	h ^= uint32(generation & 0xFF)
	h *= fnvPrime32
	return h
}

// Sign retourne le hash attendu pour un payload donné.
func (g Genome) Sign(payload []byte) uint32 {
	return Checksum(payload)
}
