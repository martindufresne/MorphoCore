package cell

import (
	"context"
	"fmt"
	"morphocore/dna"
	"morphocore/protocol"
	"time"
)

// Direction représente l'une des 4 orientations cardinales sur une grille 2D.
type Direction uint8

const (
	DirNorth Direction = 0
	DirSouth Direction = 1
	DirEast  Direction = 2
	DirWest  Direction = 3
)

func (d Direction) String() string {
	switch d {
	case DirNorth:
		return "Nord"
	case DirSouth:
		return "Sud"
	case DirEast:
		return "Est"
	case DirWest:
		return "Ouest"
	default:
		return "Inconnu"
	}
}

// Opposite retourne la direction cardinale inverse.
func (d Direction) Opposite() Direction {
	switch d {
	case DirNorth:
		return DirSouth
	case DirSouth:
		return DirNorth
	case DirEast:
		return DirWest
	case DirWest:
		return DirEast
	default:
		return DirNorth
	}
}

// SetNeighbor configure ou met à jour la liaison vers un voisin cardinal.
func (c *Cell) SetNeighbor(dir Direction, link *PeerLink) {
	if dir < 4 {
		if link != nil {
			link.Direction = dir
		}
		c.Neighbors[dir] = link
	}
}

// GetNeighbor récupère le lien d'un voisin cardinal.
func (c *Cell) GetNeighbor(dir Direction) *PeerLink {
	if dir < 4 {
		return c.Neighbors[dir]
	}
	return nil
}

// routeMeshMessage route un message sur la grille 2D en contournant dynamiquement les cellules en apoptose.
func (c *Cell) routeMeshMessage(ctx context.Context, msg []byte) bool {
	// 1. Si la cellule est le point de sortie terminal, elle livre au RootOutChan
	if (c.X == c.TargetX && c.Y == c.TargetY) || c.RootOutChan != nil {
		if c.RootOutChan != nil {
			select {
			case c.RootOutChan <- msg:
				return true
			case <-ctx.Done():
				return false
			}
		}
	}

	// 2. Détermination de la liste prioritaire de directions selon le gradient vers (TargetX, TargetY)
	candidates := c.calculateRoutingGradient()

	// 3. Essai séquentiel des candidats avec déviation dynamique si le voisin est mort ou saturé
	for _, dir := range candidates {
		link := c.Neighbors[dir]
		if link == nil || !link.IsAlive || link.DataChan == nil {
			continue // Nœud mort ou absent : contournement dynamique immédiat
		}

		select {
		case link.DataChan <- msg:
			return true // Routé avec succès
		default:
			// Canal temporairement saturé, tente la direction alternative suivante
			continue
		}
	}

	// 4. Si les canaux prioritaires étaient pleins, tentative bloquante avec timeout sur le premier vivant
	for _, dir := range candidates {
		link := c.Neighbors[dir]
		if link != nil && link.IsAlive && link.DataChan != nil {
			select {
			case link.DataChan <- msg:
				return true
			case <-time.After(20 * time.Millisecond):
				continue
			case <-ctx.Done():
				return false
			}
		}
	}

	return false
}

// calculateRoutingGradient calcule l'ordre optimal des directions pour se rapprocher de la cible (TargetX, TargetY).
func (c *Cell) calculateRoutingGradient() []Direction {
	var primary, secondary, tertiary, fallback Direction

	// Flux horizontal préférentiel d'Ouest en Est (Soma -> Axone -> Synapse)
	if c.X < c.TargetX {
		primary = DirEast
		if c.Y < c.TargetY {
			secondary = DirSouth
			tertiary = DirNorth
			fallback = DirWest
		} else {
			secondary = DirNorth
			tertiary = DirSouth
			fallback = DirWest
		}
	} else {
		// Déjà sur la colonne cible, progression verticale vers TargetY
		if c.Y < c.TargetY {
			primary = DirSouth
			secondary = DirEast
			tertiary = DirNorth
			fallback = DirWest
		} else {
			primary = DirNorth
			secondary = DirEast
			tertiary = DirSouth
			fallback = DirWest
		}
	}

	return []Direction{primary, secondary, tertiary, fallback}
}

// handleMeshMitosisRequest applique l'élection déterministe de la cellule souche sur la grille 2D.
func (c *Cell) handleMeshMitosisRequest(ctx context.Context, sig protocol.PeerSignal) {
	data := sig.Mitosis
	isFallback := len(sig.SenderID) >= 9 && sig.SenderID[:9] == "fallback:"

	// 1. Marque immédiatement le voisin défaillant comme mort pour dévier le flux
	for dir, link := range c.Neighbors {
		if link != nil && link.CellID == data.DeadCellID {
			if !isFallback {
				link.IsAlive = false
				link.IsMitosisPending = false
				if c.Verbose {
					fmt.Printf("[MESH %s (%d,%d)] Voisin %s (%s) en apoptose -> Flux dévié.\n",
						c.ID, c.X, c.Y, link.CellID, Direction(dir))
				}
				if c.EventHook != nil {
					c.EventHook(c.X, c.Y, "Routage", c.Generation)
				}
			} else if link.IsAlive || link.IsMitosisPending || link.Generation >= data.Generation {
				// En mode fallback, si le voisin a déjà été régénéré ou est pris en charge, rien à faire
				return
			}
		}
	}

	// 2. Élection déterministe de la cellule souche :
	// Ordre de priorité des voisins survivants : Ouest > Nord > Sud > Est
	// (Le voisin amont Ouest le plus proche est prioritaire pour la régénération)
	if !isFallback && !c.isElectedStemCell(data) {
		// Ce pair n'est pas le plus prioritaire : programme un fallback de sécurité
		// au cas où le voisin prioritaire serait également mort/incapable de mitose
		go func(deadID string, d protocol.MitosisRequestData) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Millisecond):
				select {
				case c.ControlIn <- protocol.PeerSignal{
					Type:     protocol.SignalMitosisRequest,
					SenderID: "fallback:" + deadID,
					Mitosis:  d,
				}:
				case <-ctx.Done():
				}
			}
		}(data.DeadCellID, data)
		return
	}

	if c.Verbose {
		fmt.Printf("[MESH %s (%d,%d)] >>> ÉLUE CELLULE SOUCHE pour régénérer %s (%d,%d) Gen %d <<<\n",
			c.ID, c.X, c.Y, data.DeadCellID, data.X, data.Y, data.Generation)
	}

	// 3. Instanciation de la remplaçante sur la grille 2D
	c.performMeshMitosis(ctx, data)
}

// isElectedStemCell détermine si la cellule courante est l'unique cellule souche élue pour la cellule morte.
func (c *Cell) isElectedStemCell(data protocol.MitosisRequestData) bool {
	// Détermine la direction relative de la cellule courante par rapport à la cellule morte
	deadX, deadY := data.X, data.Y
	myRelDir := c.relativeDirectionToDead(deadX, deadY)

	// Ordre de priorité cardinal pour l'élection : Ouest (3) > Nord (0) > Sud (1) > Est (2)
	priorityOrder := []Direction{DirWest, DirNorth, DirSouth, DirEast}

	for _, dir := range priorityOrder {
		if dir == myRelDir {
			return true // Nous sommes le voisin le plus prioritaire existant et actif !
		}
		// Vérifie si un voisin plus prioritaire que nous existe dans les cardinaux de la cellule morte
		if int(dir) < len(data.Cardinals) {
			info := data.Cardinals[dir]
			if info != nil {
				// Si le voisin est connu mort dans nos propres liens, il ne peut pas être élu
				if !info.IsAlive {
					continue
				}
				return false
			}
		}
	}

	return false
}

// relativeDirectionToDead retourne la direction de la cellule courante depuis la cellule morte.
func (c *Cell) relativeDirectionToDead(deadX, deadY int) Direction {
	if c.X == deadX-1 && c.Y == deadY {
		return DirWest // Nous sommes à l'Ouest de la cellule morte
	}
	if c.X == deadX && c.Y == deadY-1 {
		return DirNorth // Nous sommes au Nord de la cellule morte
	}
	if c.X == deadX && c.Y == deadY+1 {
		return DirSouth // Nous sommes au Sud de la cellule morte
	}
	return DirEast // Nous sommes à l'Est de la cellule morte
}

// performMeshMitosis orchestre la création de la remplaçante et la reconnexion des 4 canaux cardinaux.
func (c *Cell) performMeshMitosis(ctx context.Context, data protocol.MitosisRequestData) {
	newInChan := data.InChan
	if newInChan == nil {
		newInChan = make(chan []byte, 200)
	}
	if data.RootInChan != nil {
		newInChan = data.RootInChan
	}

	newControlIn := data.ControlIn
	if newControlIn == nil {
		newControlIn = make(chan protocol.PeerSignal, 100)
	}

	// Émet un SignalMitosisAck aux autres voisins pour notifier la prise en charge
	ack := protocol.PeerSignal{
		Type:       protocol.SignalMitosisAck,
		SenderID:   c.ID,
		Generation: data.Generation,
		Mitosis:    data,
		Timestamp:  time.Now(),
	}
	for _, nInfo := range data.Cardinals {
		if nInfo != nil && nInfo.CellID != c.ID && nInfo.ControlChan != nil {
			select {
			case nInfo.ControlChan <- ack:
			default:
			}
		}
	}

	var replacement *Cell
	var gen uint16
	if c.Pool != nil {
		var err error
		replacement, gen, err = c.Pool.Acquire(data.Family, uint8(data.X), uint8(data.Y))
		if err != nil {
			return
		}
	} else {
		replacement = NewCell(CellConfig{
			ID:         data.DeadCellID,
			Family:     data.Family,
			Generation: data.Generation,
		})
		gen = data.Generation
	}

	replacement.ID = data.DeadCellID
	replacement.Family = data.Family
	replacement.Generation = gen
	replacement.X = data.X
	replacement.Y = data.Y
	replacement.IsGrid = true
	replacement.TargetX = c.TargetX
	replacement.TargetY = c.TargetY
	replacement.InChan = newInChan
	replacement.ControlIn = newControlIn
	replacement.RootInChan = data.RootInChan
	replacement.RootOutChan = data.RootOutChan
	replacement.Genome = dna.NewGenome(data.Family)
	replacement.Tracker = c.Tracker
	replacement.Chaos = c.Chaos
	replacement.Verbose = c.Verbose
	replacement.Pool = c.Pool
	replacement.EventHook = c.EventHook

	if c.EventHook != nil {
		c.EventHook(data.X, data.Y, "Mitose", replacement.Generation)
	}

	// Reconnexion avec chaque voisin cardinal enregistré de la cellule morte (stockage statique sans allocation)
	for dirUint, neighborInfo := range data.Cardinals {
		if neighborInfo == nil {
			continue
		}
		dir := Direction(dirUint)
		oppDir := dir.Opposite()

		// Lien pour la remplaçante vers ce voisin
		replacement.PeerLinks[dir] = PeerLink{
			CellID:      neighborInfo.CellID,
			Family:      neighborInfo.Family,
			X:           neighborInfo.X,
			Y:           neighborInfo.Y,
			Direction:   dir,
			DataChan:    neighborInfo.DataChan,
			ControlChan: neighborInfo.ControlChan,
			IsAlive:     neighborInfo.IsAlive,
		}
		replacement.Neighbors[dir] = &replacement.PeerLinks[dir]

		// Si le voisin est la cellule souche courante, mise à jour directe
		if neighborInfo.CellID == c.ID {
			c.PeerLinks[oppDir] = PeerLink{
				CellID:      replacement.ID,
				Family:      replacement.Family,
				X:           replacement.X,
				Y:           replacement.Y,
				Generation:  replacement.Generation,
				Direction:   oppDir,
				DataChan:    newInChan,
				ControlChan: newControlIn,
				IsAlive:     true,
			}
			c.Neighbors[oppDir] = &c.PeerLinks[oppDir]
		} else if neighborInfo.ControlChan != nil {
			// Informe les autres voisins de la reconnexion avec la remplaçante
			routeUpdate := protocol.PeerSignal{
				Type:     protocol.SignalRouteUpdate,
				SenderID: c.ID,
				Route: protocol.RouteUpdateData{
					TargetCellID:   replacement.ID,
					Direction:      uint8(oppDir),
					X:              replacement.X,
					Y:              replacement.Y,
					Generation:     replacement.Generation,
					NewDataChan:    newInChan,
					NewControlChan: newControlIn,
					IsAlive:        true,
				},
				Timestamp: time.Now(),
			}

			select {
			case neighborInfo.ControlChan <- routeUpdate:
			case <-ctx.Done():
			}
		}
	}

	// Enregistrement de la mitose décentralisée
	if c.Tracker != nil {
		c.Tracker.RecordMitosis(replacement.ID, replacement.Generation)
	}

	// Démarrage de la goroutine de la cellule régénérée
	go replacement.Run(ctx)
}

// handleMeshRouteUpdate applique la mise à jour de canal émise par la cellule souche.
func (c *Cell) handleMeshRouteUpdate(ctx context.Context, sig protocol.PeerSignal) {
	dir := Direction(sig.Route.Direction)
	if dir >= 4 {
		return
	}

	link := c.Neighbors[dir]
	if link == nil {
		c.Neighbors[dir] = &c.PeerLinks[dir]
		link = c.Neighbors[dir]
		link.Direction = dir
	}

	link.CellID = sig.Route.TargetCellID
	link.X = sig.Route.X
	link.Y = sig.Route.Y
	link.Generation = sig.Route.Generation
	link.DataChan = sig.Route.NewDataChan
	link.ControlChan = sig.Route.NewControlChan
	link.IsAlive = sig.Route.IsAlive
	link.IsMitosisPending = false

	if c.EventHook != nil {
		c.EventHook(c.X, c.Y, "Normal", c.Generation)
	}

	if c.Verbose {
		fmt.Printf("[MESH %s (%d,%d)] Reconnexion dynamique vers %s (%s) opérationnelle.\n",
			c.ID, c.X, c.Y, link.CellID, dir)
	}
}

// BuildMeshGridWithPool initialise et interconnecte une grille 2D de cellules avec support du pool statique.
func BuildMeshGridWithPool(width, height int, recycler CellRecycler, tracker TelemetryRecorder, chaos NoiseInjector, rootIn chan []byte, rootOut chan []byte) [][]*Cell {
	grid := make([][]*Cell, width)
	for x := 0; x < width; x++ {
		grid[x] = make([]*Cell, height)
		for y := 0; y < height; y++ {
			var fam dna.Family
			switch {
			case x == 0:
				fam = dna.FamilySoma
			case x == width-1:
				fam = dna.FamilySynapse
			default:
				fam = dna.FamilyAxone
			}

			id := fmt.Sprintf("cell-%s-%d-%d", fam, x, y)
			inChan := make(chan []byte, 200)
			ctrlChan := make(chan protocol.PeerSignal, 100)

			var c *Cell
			var gen uint16 = 1
			if recycler != nil {
				var err error
				c, gen, err = recycler.Acquire(fam, uint8(x), uint8(y))
				if err != nil {
					c = NewCell(CellConfig{ID: id, Family: fam, Generation: 1})
				}
			} else {
				c = NewCell(CellConfig{
					ID:         id,
					Family:     fam,
					Generation: 1,
				})
			}

			c.ID = id
			c.Family = fam
			c.Generation = gen
			c.X = x
			c.Y = y
			c.IsGrid = true
			c.TargetX = width - 1
			c.TargetY = height - 1
			c.InChan = inChan
			c.ControlIn = ctrlChan
			c.Tracker = tracker
			c.Chaos = chaos
			c.Genome = dna.NewGenome(fam)
			c.Pool = recycler

			if x == 0 && y == 0 && rootIn != nil {
				c.InChan = rootIn
				c.RootInChan = rootIn
			}
			if x == width-1 && y == height-1 && rootOut != nil {
				c.RootOutChan = rootOut
			}

			grid[x][y] = c
		}
	}

	// Interconnexion des canaux cardinaux avec liens statiques
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			c := grid[x][y]

			// Nord : y - 1
			if y > 0 {
				northCell := grid[x][y-1]
				c.PeerLinks[DirNorth] = PeerLink{
					CellID:      northCell.ID,
					Family:      northCell.Family,
					Direction:   DirNorth,
					X:           x,
					Y:           y - 1,
					DataChan:    northCell.InChan,
					ControlChan: northCell.ControlIn,
					IsAlive:     true,
				}
				c.Neighbors[DirNorth] = &c.PeerLinks[DirNorth]
			}

			// Sud : y + 1
			if y < height-1 {
				southCell := grid[x][y+1]
				c.PeerLinks[DirSouth] = PeerLink{
					CellID:      southCell.ID,
					Family:      southCell.Family,
					Direction:   DirSouth,
					X:           x,
					Y:           y + 1,
					DataChan:    southCell.InChan,
					ControlChan: southCell.ControlIn,
					IsAlive:     true,
				}
				c.Neighbors[DirSouth] = &c.PeerLinks[DirSouth]
			}

			// Est : x + 1
			if x < width-1 {
				eastCell := grid[x+1][y]
				c.PeerLinks[DirEast] = PeerLink{
					CellID:      eastCell.ID,
					Family:      eastCell.Family,
					Direction:   DirEast,
					X:           x + 1,
					Y:           y,
					DataChan:    eastCell.InChan,
					ControlChan: eastCell.ControlIn,
					IsAlive:     true,
				}
				c.Neighbors[DirEast] = &c.PeerLinks[DirEast]
			}

			// Ouest : x - 1
			if x > 0 {
				westCell := grid[x-1][y]
				c.PeerLinks[DirWest] = PeerLink{
					CellID:      westCell.ID,
					Family:      westCell.Family,
					Direction:   DirWest,
					X:           x - 1,
					Y:           y,
					DataChan:    westCell.InChan,
					ControlChan: westCell.ControlIn,
					IsAlive:     true,
				}
				c.Neighbors[DirWest] = &c.PeerLinks[DirWest]
			}
		}
	}

	return grid
}

// BuildMeshGrid initialise et interconnecte une grille 2D de cellules avec canaux cardinaux.
func BuildMeshGrid(width, height int, tracker TelemetryRecorder, chaos NoiseInjector, rootIn chan []byte, rootOut chan []byte) [][]*Cell {
	return BuildMeshGridWithPool(width, height, nil, tracker, chaos, rootIn, rootOut)
}

// BuildMeshGridWithPoolAndHook initialise et interconnecte une grille 2D de cellules avec support du pool et d'un hook d'événements.
func BuildMeshGridWithPoolAndHook(width, height int, recycler CellRecycler, tracker TelemetryRecorder, chaos NoiseInjector, rootIn chan []byte, rootOut chan []byte, hook CellEventHook) [][]*Cell {
	grid := BuildMeshGridWithPool(width, height, recycler, tracker, chaos, rootIn, rootOut)
	if hook != nil {
		for x := 0; x < width; x++ {
			for y := 0; y < height; y++ {
				grid[x][y].EventHook = hook
			}
		}
	}
	return grid
}
