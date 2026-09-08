package protocol

import (
	"morphocore/dna"
	"time"
)

// SignalType représente la nature du message de contrôle échangé entre pairs.
type SignalType uint8

const (
	// SignalPing représente le battement de cœur périodique entre voisins.
	SignalPing SignalType = iota
	// SignalMitosisRequest représente l'alerte de mort envoyée aux pairs immédiats.
	SignalMitosisRequest
	// SignalMitosisAck représente la confirmation de prise en charge de la régénération.
	SignalMitosisAck
	// SignalRouteUpdate informe un pair d'une reconnexion dynamique de canal.
	SignalRouteUpdate
)

func (s SignalType) String() string {
	switch s {
	case SignalPing:
		return "SignalPing"
	case SignalMitosisRequest:
		return "SignalMitosisRequest"
	case SignalMitosisAck:
		return "SignalMitosisAck"
	case SignalRouteUpdate:
		return "SignalRouteUpdate"
	default:
		return "UnknownSignal"
	}
}

// PeerLinkInfo contient les coordonnées de contrôle d'un pair voisin.
type PeerLinkInfo struct {
	CellID      string
	Family      dna.Family
	Direction   uint8 // 0: Nord, 1: Sud, 2: Est, 3: Ouest
	X           int
	Y           int
	ControlChan chan PeerSignal
	DataChan    chan []byte
	IsAlive     bool
}

// MitosisRequestData contient les invariants et liens nécessaires à l'instanciation de la remplaçante.
type MitosisRequestData struct {
	DeadCellID  string
	Family      dna.Family
	Generation  uint16
	DNAHash     uint32
	X           int
	Y           int
	RootInChan  chan []byte             // Utilisé si la cellule morte était l'entrée sensorielle du réseau
	RootOutChan chan []byte             // Utilisé si la cellule morte était la sortie terminale du réseau
	PrevPeer    *PeerLinkInfo
	NextPeer    *PeerLinkInfo
	Cardinals   [4]*PeerLinkInfo        // Voisins cardinaux fixes (Nord=0, Sud=1, Est=2, Ouest=3)
	InChan      chan []byte             // Canal de données préservé
	ControlIn   chan PeerSignal         // Canal de contrôle préservé
}

// RouteUpdateData transmet les nouveaux canaux suite à la mitose et reconnexion dynamique.
type RouteUpdateData struct {
	TargetCellID   string
	Generation     uint16
	IsUpstream     bool  // Rétrocompatibilité chaîne linéaire
	Direction      uint8 // Direction cardinale concernée
	X              int
	Y              int
	NewDataChan    chan []byte
	NewControlChan chan PeerSignal
	IsAlive        bool
}

// PeerSignal encapsule le protocole de consensus local et de coordination inter-cellulaire.
type PeerSignal struct {
	Type       SignalType
	SenderID   string
	Generation uint16
	Mitosis    MitosisRequestData
	Route      RouteUpdateData
	Timestamp  time.Time
}
