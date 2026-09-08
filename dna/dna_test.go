package dna

import (
	"testing"
)

func TestFamilyConstants(t *testing.T) {
	if FamilySoma == FamilyAxone || FamilyAxone == FamilySynapse || FamilySoma == FamilySynapse {
		t.Fatalf("Family constants must be unique: Soma=%d, Axone=%d, Synapse=%d",
			FamilySoma, FamilyAxone, FamilySynapse)
	}

	if FamilySoma.String() != "Soma" {
		t.Errorf("expected 'Soma', got '%s'", FamilySoma.String())
	}
	if FamilyAxone.String() != "Axone" {
		t.Errorf("expected 'Axone', got '%s'", FamilyAxone.String())
	}
	if FamilySynapse.String() != "Synapse" {
		t.Errorf("expected 'Synapse', got '%s'", FamilySynapse.String())
	}
	if Family(99).String() != "Unknown" {
		t.Errorf("expected 'Unknown', got '%s'", Family(99).String())
	}
}

func TestVerify_NominalAndCorruption(t *testing.T) {
	genome := NewGenome(FamilySoma)
	payload := []byte("signal-morpho-42")
	expectedHash := Checksum(payload)

	// Cas nominal : hash attendu valide
	if !genome.Verify(payload, expectedHash) {
		t.Errorf("Verify failed for nominal payload and expectedHash")
	}

	// Cas corrompu : hash incorrect
	if genome.Verify(payload, expectedHash^0x12345678) {
		t.Errorf("Verify should have failed for altered hash")
	}

	// Cas corrompu : payload altéré d'un seul bit
	corruptedPayload := make([]byte, len(payload))
	copy(corruptedPayload, payload)
	corruptedPayload[0] ^= 0x01
	if genome.Verify(corruptedPayload, expectedHash) {
		t.Errorf("Verify should have failed for corrupted payload")
	}

	// Cas corrompu : payload nil
	if genome.Verify(nil, expectedHash) {
		t.Errorf("Verify should have failed for nil payload")
	}
}

func TestVerify_EmptyPayload(t *testing.T) {
	genome := NewGenome(FamilyAxone)
	emptyPayload := []byte{}
	hashEmpty := Checksum(emptyPayload)

	if !genome.Verify(emptyPayload, hashEmpty) {
		t.Errorf("Verify should succeed for empty payload with correct checksum")
	}
	if genome.Verify(emptyPayload, hashEmpty+1) {
		t.Errorf("Verify should fail for empty payload with wrong checksum")
	}
}

func TestMutate_Generations(t *testing.T) {
	genome := NewGenome(FamilyAxone)

	// Vérifie que Mutate(n) correspond à l'invariant attendu de la génération n+1
	gen1Next := genome.Mutate(1)
	gen2Expected := genome.GenerationHash(2)
	if gen1Next != gen2Expected {
		t.Errorf("Mutate(1) = 0x%08X; expected GenerationHash(2) = 0x%08X", gen1Next, gen2Expected)
	}

	// Vérifie l'évolution distincte sur plusieurs générations
	hashes := make(map[uint32]uint16)
	for g := uint16(1); g <= 100; g++ {
		h := genome.Mutate(g)
		if prevGen, exists := hashes[h]; exists {
			t.Fatalf("Collision detected between generation %d and %d: 0x%08X", prevGen, g, h)
		}
		hashes[h] = g
	}
}

func TestMutate_FamilyUniqueness(t *testing.T) {
	gSoma := NewGenome(FamilySoma)
	gAxone := NewGenome(FamilyAxone)
	gSynapse := NewGenome(FamilySynapse)

	gen := uint16(5)
	hSoma := gSoma.Mutate(gen)
	hAxone := gAxone.Mutate(gen)
	hSynapse := gSynapse.Mutate(gen)

	if hSoma == hAxone || hAxone == hSynapse || hSoma == hSynapse {
		t.Errorf("Mutations for different families at generation %d must produce different hashes: Soma=0x%08X, Axone=0x%08X, Synapse=0x%08X",
			gen, hSoma, hAxone, hSynapse)
	}
}

func TestZeroAllocations(t *testing.T) {
	genome := NewGenome(FamilySoma)
	payload := []byte("zero-alloc-check")
	h := Checksum(payload)

	allocs := testing.AllocsPerRun(1000, func() {
		_ = genome.Verify(payload, h)
	})
	if allocs != 0 {
		t.Errorf("expected 0 allocations for Verify, got %f", allocs)
	}

	allocsMutate := testing.AllocsPerRun(1000, func() {
		_ = genome.Mutate(1)
	})
	if allocsMutate != 0 {
		t.Errorf("expected 0 allocations for Mutate, got %f", allocsMutate)
	}
}
