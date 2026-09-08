package cell

import (
	"context"
	"encoding/binary"
	"fmt"
	"morphocore/dna"
	"morphocore/protocol"
	"time"
)

// HeaderSize définit la taille en octets de l'en-tête de contrôle d'intégrité (hash uint32).
const HeaderSize = 4

// TelemetryRecorder permet à la cellule de notifier les événements du cycle de vie sans couplage fort.
type TelemetryRecorder interface {
	RecordApoptosis(cellID string)
	RecordMitosis(cellID string, gen uint16)
	RecordEmission(corrupted bool)
}

// NoiseInjector applique une altération stochastique sur un flux de données.
type NoiseInjector interface {
	Inject(payload []byte) []byte
	InjectWithStatus(payload []byte) ([]byte, bool)
}

// CellRecycler permet l'acquisition et la restitution de structures cellulaires
// depuis un réservoir statique pré-alloué (zéro allocation post-boot).
type CellRecycler interface {
	Acquire(family dna.Family, x, y uint8) (*Cell, uint16, error)
	Release(c *Cell)
}

// CellEventHook définit un callback invoqué lors des transitions d'état cellulaires.
type CellEventHook func(x, y int, state string, gen uint16)

// CellConfig contient les paramètres nécessaires à l'instanciation ou à la mitose d'une cellule.
type CellConfig struct {
	ID          string
	Family      dna.Family
	Generation  uint16
	State       []byte
	StateBuf    [128]byte
	SlotID      int
	Pool        CellRecycler
	InChan      chan []byte
	DieChan     chan string
	SpawnChan   chan CellConfig
	OutChan     chan []byte
	ControlIn   chan protocol.PeerSignal
	PrevPeer    *PeerLink
	NextPeer    *PeerLink
	RootInChan  chan []byte
	RootOutChan chan []byte
	X           int
	Y           int
	IsGrid      bool
	TargetX     int
	TargetY     int
	Neighbors   [4]*PeerLink
	Tracker     TelemetryRecorder
	Chaos       NoiseInjector
	Verbose     bool
	EventHook   CellEventHook
}

// Cell représente une unité vivante concurrente dans le réseau morphogénétique.
type Cell struct {
	ID         string
	Family     dna.Family
	Generation uint16
	State      []byte
	StateBuf   [128]byte
	SlotID     int
	Pool       CellRecycler
	InChan     chan []byte
	DieChan    chan string
	SpawnChan  chan CellConfig

	// Canaux de contrôle et liens de voisinage (Peer-to-Peer 1D)
	ControlIn   chan protocol.PeerSignal
	PrevPeer    *PeerLink
	NextPeer    *PeerLink
	RootInChan  chan []byte
	RootOutChan chan []byte

	// Topologie 2D Mesh statique
	X             int
	Y             int
	IsGrid        bool
	TargetX       int
	TargetY       int
	Neighbors     [4]*PeerLink
	PeerLinks     [4]PeerLink
	cardinalInfos [4]protocol.PeerLinkInfo

	// Invariants et liaisons de flux
	Genome    dna.Genome
	OutChan   chan []byte
	Tracker   TelemetryRecorder
	Chaos     NoiseInjector
	Verbose   bool
	EventHook CellEventHook
}

// NewCell crée et initialise une nouvelle cellule à partir d'une configuration.
func NewCell(cfg CellConfig) *Cell {
	c := &Cell{
		ID:          cfg.ID,
		Family:      cfg.Family,
		Generation:  cfg.Generation,
		SlotID:      cfg.SlotID,
		Pool:        cfg.Pool,
		InChan:      cfg.InChan,
		DieChan:     cfg.DieChan,
		SpawnChan:   cfg.SpawnChan,
		ControlIn:   cfg.ControlIn,
		PrevPeer:    cfg.PrevPeer,
		NextPeer:    cfg.NextPeer,
		RootInChan:  cfg.RootInChan,
		RootOutChan: cfg.RootOutChan,
		X:           cfg.X,
		Y:           cfg.Y,
		IsGrid:      cfg.IsGrid,
		TargetX:     cfg.TargetX,
		TargetY:     cfg.TargetY,
		Genome:      dna.NewGenome(cfg.Family),
		OutChan:     cfg.OutChan,
		Tracker:     cfg.Tracker,
		Chaos:       cfg.Chaos,
		Verbose:     cfg.Verbose,
		EventHook:   cfg.EventHook,
	}

	if len(cfg.State) > 0 && len(cfg.State) <= len(c.StateBuf) {
		copy(c.StateBuf[:], cfg.State)
		c.State = c.StateBuf[:len(cfg.State)]
	} else {
		c.State = c.StateBuf[:0]
	}

	for i := 0; i < 4; i++ {
		c.PeerLinks[i].Direction = Direction(i)
		if cfg.Neighbors[i] != nil {
			c.PeerLinks[i] = *cfg.Neighbors[i]
			c.Neighbors[i] = &c.PeerLinks[i]
		}
	}

	if c.InChan == nil {
		c.InChan = make(chan []byte, 200)
	}
	if c.ControlIn == nil {
		c.ControlIn = make(chan protocol.PeerSignal, 100)
	}
	return c
}

// PackMessage encapsule un payload brut avec son empreinte mathématique stricte en préfixe.
// Format binaire : [4 octets uint32 big-endian FNV-1a][N octets payload].
func PackMessage(payload []byte) []byte {
	h := dna.Checksum(payload)
	buf := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[:HeaderSize], h)
	copy(buf[HeaderSize:], payload)
	return buf
}

// UnpackMessage extrait le payload et la somme de contrôle attendue.
// Retourne ok=false si le message ne satisfait pas la taille minimale de l'en-tête.
func UnpackMessage(msg []byte) (payload []byte, expectedHash uint32, ok bool) {
	if len(msg) < HeaderSize {
		return nil, 0, false
	}
	expectedHash = binary.BigEndian.Uint32(msg[:HeaderSize])
	payload = msg[HeaderSize:]
	return payload, expectedHash, true
}

// PackCorruptedMessage crée délibérément un message corrompu pour tester l'autodestruction.
func PackCorruptedMessage(payload []byte) []byte {
	msg := PackMessage(payload)
	if len(msg) > HeaderSize {
		msg[HeaderSize] ^= 0xFF
	} else if len(msg) > 0 {
		msg[0] ^= 0xFF
	}
	return msg
}

// Run exécute la boucle de cycle de vie de la cellule.
// Écoute en boucle sur InChan et ControlIn.
func (c *Cell) Run(ctx context.Context) {
	if c.EventHook != nil {
		c.EventHook(c.X, c.Y, "Normal", c.Generation)
	}

	// Battement de cœur périodique vers les voisins
	pingTicker := time.NewTicker(30 * time.Millisecond)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case sig, ok := <-c.ControlIn:
			if !ok {
				return
			}
			c.handleControlSignal(ctx, sig)

		case <-pingTicker.C:
			c.sendPing()

		case msg, ok := <-c.InChan:
			if !ok {
				// Détection de vacance si le canal amont se ferme brutalement
				c.handleUpstreamVacancy(ctx)
				return
			}

			payload, expectedHash, okFormat := UnpackMessage(msg)
			if !okFormat || !c.Genome.Verify(payload, expectedHash) {
				// Cas Corrompu : Déclenchement strict de l'Apoptose
				c.triggerApoptosis(ctx, payload, expectedHash)
				return
			}

			// Cas Nominal : Traite la donnée et met à jour l'état
			c.processNominal(ctx, msg, payload, expectedHash)
		}
	}
}

// processNominal gère le traitement des données valides et la transmission séquentielle.
func (c *Cell) processNominal(ctx context.Context, rawMsg []byte, payload []byte, hash uint32) {
	// Mise à jour de l'état local (allocation statique minimale / réutilisation de StateBuf)
	if len(payload) <= len(c.StateBuf) {
		c.State = c.StateBuf[:len(payload)]
		copy(c.State, payload)
	} else {
		c.State = make([]byte, len(payload))
		copy(c.State, payload)
	}

	if c.Verbose {
		fmt.Printf("[CELL %s | %s | Gen %d] Message validé (hash=0x%08X, len=%d), état mis à jour.\n",
			c.ID, c.Family, c.Generation, hash, len(payload))
	}

	// Transmission des données
	outMsg := rawMsg
	if c.Chaos != nil {
		corrupted, wasCorrupted := c.Chaos.InjectWithStatus(rawMsg)
		if wasCorrupted {
			if c.Tracker != nil {
				c.Tracker.RecordEmission(true)
			}
			outMsg = corrupted
		}
	}

	if c.IsGrid {
		// Routage par gradient sur la grille 2D avec contournement dynamique
		c.routeMeshMessage(ctx, outMsg)
		return
	}

	// Transmission en chaîne linéaire vers la cellule suivante si reliée
	if c.OutChan != nil {
		select {
		case c.OutChan <- outMsg:
		case <-ctx.Done():
		}
	}
}

// triggerApoptosis applique le protocole d'autodestruction décentralisé :
// 1. Enregistrement télémétrique
// 2. Alerte de mort envoyée aux pairs immédiats (SignalMitosisRequest)
// 3. Purge et remise à zéro totale de la mémoire locale (memset)
// 4. Restitution immédiate de la structure au CellPool
func (c *Cell) triggerApoptosis(ctx context.Context, payload []byte, expectedHash uint32) {
	if c.EventHook != nil {
		c.EventHook(c.X, c.Y, "Apoptose", c.Generation)
	}

	if c.Verbose {
		computed := dna.Checksum(payload)
		fmt.Printf("[CELL %s | %s | Gen %d] [APOPTOSE] Invariant violé! Attendu: 0x%08X, Obtenu: 0x%08X. Autodestruction...\n",
			c.ID, c.Family, c.Generation, expectedHash, computed)
	}

	// 1. Télémétrie
	if c.Tracker != nil {
		c.Tracker.RecordApoptosis(c.ID)
	}

	// 2. Rétrocompatibilité tests (si canaux non-nuls)
	if c.DieChan != nil {
		select {
		case c.DieChan <- c.ID:
		case <-ctx.Done():
		}
	}

	nextGen := c.Generation + 1
	nextHash := c.Genome.Mutate(c.Generation)

	if c.SpawnChan != nil {
		spawnReq := CellConfig{
			ID:         c.ID,
			Family:     c.Family,
			Generation: nextGen,
			InChan:     c.InChan,
			DieChan:    c.DieChan,
			SpawnChan:  c.SpawnChan,
			OutChan:    c.OutChan,
		}
		select {
		case c.SpawnChan <- spawnReq:
		case <-ctx.Done():
		}
	}

	// 3. Alerte de mort décentralisée aux voisins (Peer-to-Peer)
	if c.IsGrid {
		var cardinals [4]*protocol.PeerLinkInfo
		for dir := 0; dir < 4; dir++ {
			link := c.Neighbors[dir]
			if link != nil {
				c.cardinalInfos[dir] = protocol.PeerLinkInfo{
					CellID:      link.CellID,
					Family:      link.Family,
					Direction:   uint8(dir),
					X:           link.X,
					Y:           link.Y,
					ControlChan: link.ControlChan,
					DataChan:    link.DataChan,
					IsAlive:     link.IsAlive,
				}
				cardinals[dir] = &c.cardinalInfos[dir]
			}
		}

		deathAlert := protocol.PeerSignal{
			Type:       protocol.SignalMitosisRequest,
			SenderID:   c.ID,
			Generation: nextGen,
			Mitosis: protocol.MitosisRequestData{
				DeadCellID:  c.ID,
				Family:      c.Family,
				Generation:  nextGen,
				DNAHash:     nextHash,
				X:           c.X,
				Y:           c.Y,
				RootInChan:  c.RootInChan,
				RootOutChan: c.RootOutChan,
				Cardinals:   cardinals,
				InChan:      c.InChan,
				ControlIn:   c.ControlIn,
			},
			Timestamp: time.Now(),
		}

		for _, link := range c.Neighbors {
			if link != nil && link.ControlChan != nil {
				select {
				case link.ControlChan <- deathAlert:
				case <-ctx.Done():
				}
			}
		}
	} else {
		// Mode chaîne linéaire 1D (Rétrocompatibilité)
		var prevInfo, nextInfo *protocol.PeerLinkInfo
		if c.PrevPeer != nil {
			prevInfo = &protocol.PeerLinkInfo{
				CellID:      c.PrevPeer.CellID,
				Family:      c.PrevPeer.Family,
				ControlChan: c.PrevPeer.ControlChan,
			}
		}
		if c.NextPeer != nil {
			nextInfo = &protocol.PeerLinkInfo{
				CellID:      c.NextPeer.CellID,
				Family:      c.NextPeer.Family,
				ControlChan: c.NextPeer.ControlChan,
			}
		}

		deathAlert := protocol.PeerSignal{
			Type:       protocol.SignalMitosisRequest,
			SenderID:   c.ID,
			Generation: nextGen,
			Mitosis: protocol.MitosisRequestData{
				DeadCellID:  c.ID,
				Family:      c.Family,
				Generation:  nextGen,
				DNAHash:     nextHash,
				RootInChan:  c.RootInChan,
				RootOutChan: c.RootOutChan,
				PrevPeer:    prevInfo,
				NextPeer:    nextInfo,
			},
			Timestamp: time.Now(),
		}

		if c.PrevPeer != nil && c.PrevPeer.ControlChan != nil {
			select {
			case c.PrevPeer.ControlChan <- deathAlert:
			case <-ctx.Done():
			}
		}

		if c.NextPeer != nil && c.NextPeer.ControlChan != nil {
			select {
			case c.NextPeer.ControlChan <- deathAlert:
			case <-ctx.Done():
			}
		}
	}

	// 4. Purge explicite de la mémoire (memset octet par octet)
	for i := range c.StateBuf {
		c.StateBuf[i] = 0
	}
	c.State = nil

	if c.Verbose {
		fmt.Printf("[CELL %s | %s | Gen %d] [MÉMOIRE] Buffers purgés à zéro. Slot libéré.\n",
			c.ID, c.Family, c.Generation)
	}

	// 5. Restitution finale au pool statique (aucun accès mémoire ultérieur à c)
	if c.Pool != nil {
		c.Pool.Release(c)
	}
}
