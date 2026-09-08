package cell

import (
	"bytes"
	"context"
	"morphocore/dna"
	"morphocore/harness"
	"morphocore/protocol"
	"sync"
	"testing"
	"time"
)

func TestCell_NominalLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inChan := make(chan []byte, 5)
	outChan := make(chan []byte, 5)
	dieChan := make(chan string, 1)
	spawnChan := make(chan CellConfig, 1)

	c := NewCell(CellConfig{
		ID:         "cell-test-soma",
		Family:     dna.FamilySoma,
		Generation: 1,
		InChan:     inChan,
		DieChan:    dieChan,
		SpawnChan:  spawnChan,
		OutChan:    outChan,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()

	payload := []byte("hello-morphocore")
	inChan <- PackMessage(payload)

	select {
	case outMsg := <-outChan:
		outPayload, _, ok := UnpackMessage(outMsg)
		if !ok {
			t.Fatalf("Failed to unpack output message")
		}
		if !bytes.Equal(outPayload, payload) {
			t.Errorf("Expected payload %s, got %s", payload, outPayload)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for nominal message forwarding")
	}

	// Vérifie qu'aucun signal de mort n'a été émis
	select {
	case dead := <-dieChan:
		t.Fatalf("Unexpected apoptosis signal for cell %s", dead)
	default:
	}

	cancel()
	wg.Wait()
}

func TestCell_ApoptosisOnCorruptedPayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inChan := make(chan []byte, 5)
	dieChan := make(chan string, 1)
	spawnChan := make(chan CellConfig, 1)

	c := NewCell(CellConfig{
		ID:         "cell-test-axone",
		Family:     dna.FamilyAxone,
		Generation: 1,
		InChan:     inChan,
		DieChan:    dieChan,
		SpawnChan:  spawnChan,
	})

	// Initialise un buffer d'état non nul pour vérifier sa purge
	c.State = []byte("sensitive-cellular-state")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()

	// Injection du message corrompu
	corrupted := PackCorruptedMessage([]byte("corrupted-data"))
	inChan <- corrupted

	// 1. Vérifie la réception de l'ID sur DieChan
	select {
	case deadID := <-dieChan:
		if deadID != "cell-test-axone" {
			t.Errorf("Expected dead ID 'cell-test-axone', got '%s'", deadID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for DieChan signal")
	}

	// 2. Vérifie la réception de la requête de mitose sur SpawnChan (Génération n+1)
	select {
	case spawnCfg := <-spawnChan:
		if spawnCfg.ID != "cell-test-axone" {
			t.Errorf("Expected spawn ID 'cell-test-axone', got '%s'", spawnCfg.ID)
		}
		if spawnCfg.Generation != 2 {
			t.Errorf("Expected next generation 2, got %d", spawnCfg.Generation)
		}
		if spawnCfg.Family != dna.FamilyAxone {
			t.Errorf("Expected family Axone, got %s", spawnCfg.Family)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for SpawnChan mitosis request")
	}

	// 3. Vérifie que la goroutine de la cellule se termine proprement
	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		// La goroutine a bien retourné
	case <-time.After(1 * time.Second):
		t.Fatalf("Cell goroutine did not exit after apoptosis")
	}

	// 4. Vérifie que la mémoire a été purgée à zéro
	if c.State != nil {
		t.Errorf("Expected cell state to be wiped (nil), got %v", c.State)
	}
}

func TestCell_ApoptosisOnTruncatedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inChan := make(chan []byte, 5)
	dieChan := make(chan string, 1)
	spawnChan := make(chan CellConfig, 1)

	c := NewCell(CellConfig{
		ID:         "cell-test-synapse",
		Family:     dna.FamilySynapse,
		Generation: 3,
		InChan:     inChan,
		DieChan:    dieChan,
		SpawnChan:  spawnChan,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()

	// Message de moins de 4 octets (invalide)
	inChan <- []byte{0x01, 0x02}

	select {
	case deadID := <-dieChan:
		if deadID != "cell-test-synapse" {
			t.Errorf("Expected 'cell-test-synapse', got '%s'", deadID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for DieChan on truncated message")
	}

	select {
	case spawnCfg := <-spawnChan:
		if spawnCfg.Generation != 4 {
			t.Errorf("Expected generation 4, got %d", spawnCfg.Generation)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for SpawnChan on truncated message")
	}

	wg.Wait()
}

func TestCell_MitosisAndRebirth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inChan := make(chan []byte, 5)
	outChan := make(chan []byte, 5)
	dieChan := make(chan string, 2)
	spawnChan := make(chan CellConfig, 2)

	// Génération 1
	cellGen1 := NewCell(CellConfig{
		ID:         "cell-mitosis",
		Family:     dna.FamilySoma,
		Generation: 1,
		InChan:     inChan,
		DieChan:    dieChan,
		SpawnChan:  spawnChan,
		OutChan:    outChan,
	})

	go cellGen1.Run(ctx)

	// Provoque l'apoptose
	inChan <- PackCorruptedMessage([]byte("poison"))

	var spawnReq CellConfig
	select {
	case <-dieChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for dieChan")
	}

	select {
	case spawnReq = <-spawnChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for spawnChan")
	}

	if spawnReq.Generation != 2 {
		t.Fatalf("Expected Generation 2, got %d", spawnReq.Generation)
	}

	// Instanciation de la remplaçante via la config de mitose
	cellGen2 := NewCell(spawnReq)
	go cellGen2.Run(ctx)

	// Envoi d'un message valide à la remplaçante
	nominalPayload := []byte("rebirth-success")
	inChan <- PackMessage(nominalPayload)

	select {
	case msg := <-outChan:
		payload, _, ok := UnpackMessage(msg)
		if !ok || !bytes.Equal(payload, nominalPayload) {
			t.Fatalf("Reborn cell failed to process valid message")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for reborn cell message")
	}
}

func TestPeer_DecentralizedMitosis_Cell2Regeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := harness.NewMetricsTracker()

	c1In := make(chan []byte, 10)
	c1ToC2 := make(chan []byte, 10)
	c2ToC3 := make(chan []byte, 10)
	c3Sink := make(chan []byte, 10)

	c1Ctrl := make(chan protocol.PeerSignal, 10)
	c2Ctrl := make(chan protocol.PeerSignal, 10)
	c3Ctrl := make(chan protocol.PeerSignal, 10)

	cell1 := NewCell(CellConfig{
		ID:         "c1-soma",
		Family:     dna.FamilySoma,
		Generation: 1,
		InChan:     c1In,
		OutChan:    c1ToC2,
		ControlIn:  c1Ctrl,
		NextPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	cell2 := NewCell(CellConfig{
		ID:         "c2-axone",
		Family:     dna.FamilyAxone,
		Generation: 1,
		InChan:     c1ToC2,
		OutChan:    c2ToC3,
		ControlIn:  c2Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c1-soma",
			Family:      dna.FamilySoma,
			ControlChan: c1Ctrl,
		},
		NextPeer: &PeerLink{
			CellID:      "c3-synapse",
			Family:      dna.FamilySynapse,
			ControlChan: c3Ctrl,
		},
		Tracker: tracker,
	})

	cell3 := NewCell(CellConfig{
		ID:          "c3-synapse",
		Family:      dna.FamilySynapse,
		Generation:  1,
		InChan:      c2ToC3,
		OutChan:     c3Sink,
		RootOutChan: c3Sink,
		ControlIn:   c3Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	go cell1.Run(ctx)
	go cell2.Run(ctx)
	go cell3.Run(ctx)

	// Envoi d'un premier message valide
	c1In <- PackMessage([]byte("first-valid"))
	select {
	case out := <-c3Sink:
		p, _, _ := UnpackMessage(out)
		if string(p) != "first-valid" {
			t.Fatalf("unexpected message: %s", string(p))
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout on first message")
	}

	// Corrompt délibérément Cell 2 (Axone)
	c1ToC2 <- PackCorruptedMessage([]byte("poison-for-cell-2"))

	// Attente brève de la prise en charge autonome par la cellule souche (Cell 1)
	time.Sleep(100 * time.Millisecond)

	// Émet un nouveau message valide à l'entrée
	c1In <- PackMessage([]byte("post-regeneration-p2p"))

	select {
	case out := <-c3Sink:
		p, _, _ := UnpackMessage(out)
		if string(p) != "post-regeneration-p2p" {
			t.Fatalf("expected 'post-regeneration-p2p', got '%s'", string(p))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for post-regeneration message through reborn Cell 2")
	}

	snap := tracker.Snapshot()
	if snap.TotalApoptosis < 1 || snap.TotalMitosis < 1 {
		t.Errorf("expected at least 1 apoptosis and 1 mitosis, got %d and %d",
			snap.TotalApoptosis, snap.TotalMitosis)
	}
}

func TestPeer_DecentralizedMitosis_Cell1Regeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := harness.NewMetricsTracker()

	c1In := make(chan []byte, 10)
	c1ToC2 := make(chan []byte, 10)
	c2ToC3 := make(chan []byte, 10)
	c3Sink := make(chan []byte, 10)

	c1Ctrl := make(chan protocol.PeerSignal, 10)
	c2Ctrl := make(chan protocol.PeerSignal, 10)
	c3Ctrl := make(chan protocol.PeerSignal, 10)

	cell1 := NewCell(CellConfig{
		ID:         "c1-soma",
		Family:     dna.FamilySoma,
		Generation: 1,
		InChan:     c1In,
		RootInChan: c1In,
		OutChan:    c1ToC2,
		ControlIn:  c1Ctrl,
		NextPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	cell2 := NewCell(CellConfig{
		ID:         "c2-axone",
		Family:     dna.FamilyAxone,
		Generation: 1,
		InChan:     c1ToC2,
		OutChan:    c2ToC3,
		ControlIn:  c2Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c1-soma",
			Family:      dna.FamilySoma,
			ControlChan: c1Ctrl,
		},
		NextPeer: &PeerLink{
			CellID:      "c3-synapse",
			Family:      dna.FamilySynapse,
			ControlChan: c3Ctrl,
		},
		Tracker: tracker,
	})

	cell3 := NewCell(CellConfig{
		ID:          "c3-synapse",
		Family:      dna.FamilySynapse,
		Generation:  1,
		InChan:      c2ToC3,
		OutChan:     c3Sink,
		RootOutChan: c3Sink,
		ControlIn:   c3Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	go cell1.Run(ctx)
	go cell2.Run(ctx)
	go cell3.Run(ctx)

	// Corrompt Cell 1 (tête de réseau)
	c1In <- PackCorruptedMessage([]byte("poison-for-cell-1"))

	// Attente de la mitose autonome opérée par Cell 2
	time.Sleep(100 * time.Millisecond)

	// Émission d'un message valide dans la cellule régénérée
	c1In <- PackMessage([]byte("msg-through-reborn-cell-1"))

	select {
	case out := <-c3Sink:
		p, _, _ := UnpackMessage(out)
		if string(p) != "msg-through-reborn-cell-1" {
			t.Fatalf("expected 'msg-through-reborn-cell-1', got '%s'", string(p))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for message through reborn Cell 1")
	}
}

func TestPeer_DecentralizedMitosis_Cell3Regeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := harness.NewMetricsTracker()

	c1In := make(chan []byte, 10)
	c1ToC2 := make(chan []byte, 10)
	c2ToC3 := make(chan []byte, 10)
	c3Sink := make(chan []byte, 10)

	c1Ctrl := make(chan protocol.PeerSignal, 10)
	c2Ctrl := make(chan protocol.PeerSignal, 10)
	c3Ctrl := make(chan protocol.PeerSignal, 10)

	cell1 := NewCell(CellConfig{
		ID:         "c1-soma",
		Family:     dna.FamilySoma,
		Generation: 1,
		InChan:     c1In,
		RootInChan: c1In,
		OutChan:    c1ToC2,
		ControlIn:  c1Ctrl,
		NextPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	cell2 := NewCell(CellConfig{
		ID:         "c2-axone",
		Family:     dna.FamilyAxone,
		Generation: 1,
		InChan:     c1ToC2,
		OutChan:    c2ToC3,
		ControlIn:  c2Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c1-soma",
			Family:      dna.FamilySoma,
			ControlChan: c1Ctrl,
		},
		NextPeer: &PeerLink{
			CellID:      "c3-synapse",
			Family:      dna.FamilySynapse,
			ControlChan: c3Ctrl,
		},
		Tracker: tracker,
	})

	cell3 := NewCell(CellConfig{
		ID:          "c3-synapse",
		Family:      dna.FamilySynapse,
		Generation:  1,
		InChan:      c2ToC3,
		OutChan:     c3Sink,
		RootOutChan: c3Sink,
		ControlIn:   c3Ctrl,
		PrevPeer: &PeerLink{
			CellID:      "c2-axone",
			Family:      dna.FamilyAxone,
			ControlChan: c2Ctrl,
		},
		Tracker: tracker,
	})

	go cell1.Run(ctx)
	go cell2.Run(ctx)
	go cell3.Run(ctx)

	// Corrompt Cell 3 (queue de réseau)
	c2ToC3 <- PackCorruptedMessage([]byte("poison-for-cell-3"))

	// Attente de la mitose autonome opérée par Cell 2
	time.Sleep(100 * time.Millisecond)

	// Émission d'un message valide traversant Cell 1 -> Cell 2 -> Cell 3 (Gen 2)
	c1In <- PackMessage([]byte("msg-through-reborn-cell-3"))

	select {
	case out := <-c3Sink:
		p, _, _ := UnpackMessage(out)
		if string(p) != "msg-through-reborn-cell-3" {
			t.Fatalf("expected 'msg-through-reborn-cell-3', got '%s'", string(p))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for message through reborn Cell 3")
	}
}
