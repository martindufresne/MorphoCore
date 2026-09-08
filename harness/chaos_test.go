package harness

import (
	"bytes"
	"morphocore/cell"
	"morphocore/dna"
	"testing"
)

func TestChaosMonkey_Boundaries(t *testing.T) {
	cmLow := NewChaosMonkey(-0.5)
	if cmLow.ErrorRate != 0.0 {
		t.Errorf("expected ErrorRate 0.0, got %f", cmLow.ErrorRate)
	}

	cmHigh := NewChaosMonkey(1.5)
	if cmHigh.ErrorRate != 1.0 {
		t.Errorf("expected ErrorRate 1.0, got %f", cmHigh.ErrorRate)
	}
}

func TestChaosMonkey_ZeroErrorRate(t *testing.T) {
	cm := NewChaosMonkeyWithSeed(0.0, 42)
	payload := []byte("pristine-message-content")
	packed := cell.PackMessage(payload)

	for i := 0; i < 100; i++ {
		res := cm.Inject(packed)
		if !bytes.Equal(res, packed) {
			t.Fatalf("With ErrorRate=0.0, Inject must not modify the payload")
		}
	}
}

func TestChaosMonkey_FullCorruption(t *testing.T) {
	cm := NewChaosMonkeyWithSeed(1.0, 1337)
	genome := dna.NewGenome(dna.FamilySoma)
	payload := []byte("deterministic-payload")
	packed := cell.PackMessage(payload)

	corruptedCount := 0
	trials := 200

	for i := 0; i < trials; i++ {
		corrupted := cm.Inject(packed)

		// Le message altéré doit soit échouer au déballage, soit échouer à la vérification d'invariant
		unpackedPayload, expectedHash, ok := cell.UnpackMessage(corrupted)
		if !ok || !genome.Verify(unpackedPayload, expectedHash) {
			corruptedCount++
		}
	}

	if corruptedCount != trials {
		t.Fatalf("With ErrorRate=1.0, expected %d corruptions, got %d", trials, corruptedCount)
	}
}

func TestChaosMonkey_EmptyPayload(t *testing.T) {
	cm := NewChaosMonkey(0.5)
	empty := []byte{}
	res := cm.Inject(empty)
	if len(res) != 0 {
		t.Errorf("Inject on empty slice should remain empty")
	}
}
