package cell

import (
	"context"
	"fmt"
	"morphocore/dna"
	"morphocore/protocol"
	"time"
)

// PeerLink représente la connexion de contrôle et de données vers une cellule voisine immédiate.
type PeerLink struct {
	CellID           string
	Family           dna.Family
	Generation       uint16
	Direction        Direction
	X                int
	Y                int
	ControlChan      chan protocol.PeerSignal
	DataChan         chan []byte
	IsAlive          bool
	IsMitosisPending bool
}

// SetPrevPeer configure le pair amont (précédent dans la chaîne linéaire).
func (c *Cell) SetPrevPeer(link *PeerLink) {
	c.PrevPeer = link
}

// SetNextPeer configure le pair aval (suivant dans la chaîne linéaire).
func (c *Cell) SetNextPeer(link *PeerLink) {
	c.NextPeer = link
}

// sendPing émet un battement de cœur périodique vers les pairs voisins (non-bloquant).
func (c *Cell) sendPing() {
	ping := protocol.PeerSignal{
		Type:       protocol.SignalPing,
		SenderID:   c.ID,
		Generation: c.Generation,
		Timestamp:  time.Now(),
	}

	if c.IsGrid {
		for _, link := range c.Neighbors {
			if link != nil && link.ControlChan != nil {
				select {
				case link.ControlChan <- ping:
				default:
				}
			}
		}
		return
	}

	if c.PrevPeer != nil && c.PrevPeer.ControlChan != nil {
		select {
		case c.PrevPeer.ControlChan <- ping:
		default:
		}
	}

	if c.NextPeer != nil && c.NextPeer.ControlChan != nil {
		select {
		case c.NextPeer.ControlChan <- ping:
		default:
		}
	}
}

// handleControlSignal traite les signaux de contrôle reçus sur ControlIn.
func (c *Cell) handleControlSignal(ctx context.Context, sig protocol.PeerSignal) {
	switch sig.Type {
	case protocol.SignalPing:
		// Heartbeat nominal reçu d'un pair

	case protocol.SignalMitosisRequest:
		if c.IsGrid {
			c.handleMeshMitosisRequest(ctx, sig)
		} else {
			c.handleMitosisRequest(ctx, sig)
		}

	case protocol.SignalMitosisAck:
		if c.IsGrid {
			for _, link := range c.Neighbors {
				if link != nil && link.CellID == sig.Mitosis.DeadCellID {
					link.IsMitosisPending = true
					if sig.Mitosis.Generation > link.Generation {
						link.Generation = sig.Mitosis.Generation
					}
				}
			}
		}

	case protocol.SignalRouteUpdate:
		if c.IsGrid {
			c.handleMeshRouteUpdate(ctx, sig)
		} else {
			c.handleRouteUpdate(ctx, sig)
		}
	}
}

// handleMitosisRequest applique le protocole de consensus local pour déterminer
// si cette cellule doit endosser le rôle de "cellule souche".
func (c *Cell) handleMitosisRequest(ctx context.Context, sig protocol.PeerSignal) {
	deadID := sig.Mitosis.DeadCellID

	// Cas 1 : La cellule morte est notre voisin aval (NextPeer)
	// La cellule amont a TOUJOURS la priorité absolue de consensus.
	if c.NextPeer != nil && c.NextPeer.CellID == deadID {
		c.regenerateDownstreamPeer(ctx, sig.Mitosis)
		return
	}

	// Cas 2 : La cellule morte est notre voisin amont (PrevPeer)
	// On ne la régénère QUE si elle n'avait pas elle-même de voisin amont (ex: tête de chaîne Cell 1)
	if c.PrevPeer != nil && c.PrevPeer.CellID == deadID {
		if sig.Mitosis.PrevPeer == nil {
			c.regenerateUpstreamPeer(ctx, sig.Mitosis)
		}
		return
	}
}

// regenerateUpstreamPeer assume le rôle de cellule souche pour son pair amont (ex: Cell 1 régénérée par Cell 2).
func (c *Cell) regenerateUpstreamPeer(ctx context.Context, data protocol.MitosisRequestData) {
	if c.Verbose {
		fmt.Printf("[PAIR %s | Gen %d] Prise du rôle de CELLULE SOUCHE pour le pair amont '%s' (Gen %d)\n",
			c.ID, c.Generation, data.DeadCellID, data.Generation)
	}

	// 1. Reconnexion du canal de données intercellulaire
	newInterChan := make(chan []byte, 200)
	c.InChan = newInterChan

	// 2. Création du canal de contrôle pour la remplaçante
	replacementControlIn := make(chan protocol.PeerSignal, 50)

	var newInChan chan []byte
	var prevPrevPeer *PeerLink

	if data.RootInChan != nil {
		newInChan = data.RootInChan
	} else if data.PrevPeer != nil {
		newInChan = make(chan []byte, 200)
		prevPrevPeer = &PeerLink{
			CellID:      data.PrevPeer.CellID,
			Family:      data.PrevPeer.Family,
			ControlChan: data.PrevPeer.ControlChan,
		}
	}

	// 3. Instanciation de la nouvelle cellule de génération n+1
	replacement := NewCell(CellConfig{
		ID:          data.DeadCellID,
		Family:      data.Family,
		Generation:  data.Generation,
		InChan:      newInChan,
		OutChan:     newInterChan,
		ControlIn:   replacementControlIn,
		RootInChan:  data.RootInChan,
		RootOutChan: data.RootOutChan,
		PrevPeer:    prevPrevPeer,
		NextPeer: &PeerLink{
			CellID:      c.ID,
			Family:      c.Family,
			ControlChan: c.ControlIn,
		},
		Tracker: c.Tracker,
		Chaos:   c.Chaos,
		Verbose: c.Verbose,
	})

	// 4. Si le pair amont avait lui-même un pair amont, on le met à jour
	if prevPrevPeer != nil && prevPrevPeer.ControlChan != nil {
		routeSignal := protocol.PeerSignal{
			Type:     protocol.SignalRouteUpdate,
			SenderID: c.ID,
			Route: protocol.RouteUpdateData{
				TargetCellID:   replacement.ID,
				IsUpstream:     false, // Met à jour le canal de sortie du voisin amont
				NewDataChan:    newInChan,
				NewControlChan: replacementControlIn,
			},
			Timestamp: time.Now(),
		}
		select {
		case prevPrevPeer.ControlChan <- routeSignal:
		case <-ctx.Done():
		}
	}

	// 5. Mise à jour de notre propre lien amont
	c.PrevPeer = &PeerLink{
		CellID:      replacement.ID,
		Family:      replacement.Family,
		ControlChan: replacementControlIn,
	}

	// 6. Enregistrement de la mitose et activation de la remplaçante
	if c.Tracker != nil {
		c.Tracker.RecordMitosis(replacement.ID, replacement.Generation)
	}

	go replacement.Run(ctx)
}

// regenerateDownstreamPeer assume le rôle de cellule souche pour son pair aval (ex: Cell 2 régénérée par Cell 1, ou Cell 3 par Cell 2).
func (c *Cell) regenerateDownstreamPeer(ctx context.Context, data protocol.MitosisRequestData) {
	if c.Verbose {
		fmt.Printf("[PAIR %s | Gen %d] Prise du rôle de CELLULE SOUCHE pour le pair aval '%s' (Gen %d)\n",
			c.ID, c.Generation, data.DeadCellID, data.Generation)
	}

	// 1. Reconnexion des canaux de données
	newInChan := make(chan []byte, 200)
	c.OutChan = newInChan

	replacementControlIn := make(chan protocol.PeerSignal, 50)

	var newOutChan chan []byte
	var nextNextPeer *PeerLink

	if data.RootOutChan != nil {
		// La cellule morte est la terminaison du réseau (ex: Cell 3 -> c3Sink)
		newOutChan = data.RootOutChan
	} else if data.NextPeer != nil {
		// La cellule morte a un voisin aval (ex: Cell 2 -> Cell 3)
		newOutChan = make(chan []byte, 200)
		nextNextPeer = &PeerLink{
			CellID:      data.NextPeer.CellID,
			Family:      data.NextPeer.Family,
			ControlChan: data.NextPeer.ControlChan,
		}
	}

	// 2. Instanciation de la remplaçante
	replacement := NewCell(CellConfig{
		ID:          data.DeadCellID,
		Family:      data.Family,
		Generation:  data.Generation,
		InChan:      newInChan,
		OutChan:     newOutChan,
		ControlIn:   replacementControlIn,
		RootInChan:  data.RootInChan,
		RootOutChan: data.RootOutChan,
		PrevPeer: &PeerLink{
			CellID:      c.ID,
			Family:      c.Family,
			ControlChan: c.ControlIn,
		},
		NextPeer: nextNextPeer,
		Tracker:  c.Tracker,
		Chaos:    c.Chaos,
		Verbose:  c.Verbose,
	})

	// 3. Si la cellule avait un voisin aval (ex: Cell 3), on l'informe du nouveau routage
	if nextNextPeer != nil && nextNextPeer.ControlChan != nil {
		routeSignal := protocol.PeerSignal{
			Type:     protocol.SignalRouteUpdate,
			SenderID: c.ID,
			Route: protocol.RouteUpdateData{
				TargetCellID:   replacement.ID,
				IsUpstream:     true, // Nouveau flux d'entrée pour Cell 3
				NewDataChan:    newOutChan,
				NewControlChan: replacementControlIn,
			},
			Timestamp: time.Now(),
		}

		select {
		case nextNextPeer.ControlChan <- routeSignal:
		case <-ctx.Done():
		}
	}

	// 4. Mise à jour de notre propre lien aval
	c.NextPeer = &PeerLink{
		CellID:      replacement.ID,
		Family:      replacement.Family,
		ControlChan: replacementControlIn,
	}

	// 5. Enregistrement de la mitose et activation de la remplaçante
	if c.Tracker != nil {
		c.Tracker.RecordMitosis(replacement.ID, replacement.Generation)
	}

	go replacement.Run(ctx)
}

// handleRouteUpdate met à jour dynamiquement les canaux de communication d'une cellule
// suite à la notification de reconnexion émise par la cellule souche.
func (c *Cell) handleRouteUpdate(ctx context.Context, sig protocol.PeerSignal) {
	if sig.Route.IsUpstream {
		// Le voisin amont a été régénéré avec un nouveau canal de données
		c.InChan = sig.Route.NewDataChan
		if c.PrevPeer == nil {
			c.PrevPeer = &PeerLink{}
		}
		c.PrevPeer.CellID = sig.Route.TargetCellID
		c.PrevPeer.ControlChan = sig.Route.NewControlChan
	} else {
		// Le voisin aval a été régénéré avec un nouveau canal de données
		c.OutChan = sig.Route.NewDataChan
		if c.NextPeer == nil {
			c.NextPeer = &PeerLink{}
		}
		c.NextPeer.CellID = sig.Route.TargetCellID
		c.NextPeer.ControlChan = sig.Route.NewControlChan
	}
}

// handleUpstreamVacancy gère la fermeture brutale du canal de données amont.
func (c *Cell) handleUpstreamVacancy(ctx context.Context) {
	if c.PrevPeer != nil {
		sig := protocol.MitosisRequestData{
			DeadCellID: c.PrevPeer.CellID,
			Family:     c.PrevPeer.Family,
			Generation: c.Generation + 1,
			RootInChan: c.RootInChan,
		}
		c.regenerateUpstreamPeer(ctx, sig)
	}
}
