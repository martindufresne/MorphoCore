package harness

import (
	"math/rand"
	"sync"
	"time"
)

// ChaosMonkey est un injecteur stochastique d'anomalies et de corruption binaire.
// Il simule les perturbations environnementales (fuzzing, bit-flips, coupures réseau).
type ChaosMonkey struct {
	ErrorRate float64
	mu        sync.Mutex
	rng       *rand.Rand
}

// NewChaosMonkey instancie un ChaosMonkey avec une probabilité d'erreur donnée (0.0 à 1.0).
func NewChaosMonkey(errorRate float64) *ChaosMonkey {
	if errorRate < 0.0 {
		errorRate = 0.0
	} else if errorRate > 1.0 {
		errorRate = 1.0
	}
	return &ChaosMonkey{
		ErrorRate: errorRate,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewChaosMonkeyWithSeed permet de créer un ChaosMonkey avec une graine fixe pour les tests reproductibles.
func NewChaosMonkeyWithSeed(errorRate float64, seed int64) *ChaosMonkey {
	if errorRate < 0.0 {
		errorRate = 0.0
	} else if errorRate > 1.0 {
		errorRate = 1.0
	}
	return &ChaosMonkey{
		ErrorRate: errorRate,
		rng:       rand.New(rand.NewSource(seed)),
	}
}

// SetErrorRate ajuste dynamiquement le taux d'erreur du ChaosMonkey (0.0 à 1.0).
func (cm *ChaosMonkey) SetErrorRate(rate float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if rate < 0.0 {
		rate = 0.0
	} else if rate > 1.0 {
		rate = 1.0
	}
	cm.ErrorRate = rate
}

// GetErrorRate retourne le taux d'erreur actuel.
func (cm *ChaosMonkey) GetErrorRate() float64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.ErrorRate
}

// Corrupt applique inconditionnellement l'une des 3 altérations stochastiques
// (inversion de bit, troncature de taille, déphasage de checksum).
func (cm *ChaosMonkey) Corrupt(payload []byte) []byte {
	if len(payload) == 0 {
		return payload
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	corrupted := make([]byte, len(payload))
	copy(corrupted, payload)

	strategy := cm.rng.Intn(3)
	switch strategy {
	case 0: // Inversion d'un bit (XOR)
		byteIdx := cm.rng.Intn(len(corrupted))
		bitIdx := uint(cm.rng.Intn(8))
		corrupted[byteIdx] ^= (1 << bitIdx)

	case 1: // Troncature de taille
		if len(corrupted) <= 4 {
			corrupted = corrupted[:cm.rng.Intn(len(corrupted))]
		} else {
			cut := cm.rng.Intn(len(corrupted))
			corrupted = corrupted[:cut]
		}

	case 2: // Déphasage du checksum (dans l'en-tête de 4 octets)
		if len(corrupted) >= 4 {
			csIdx := cm.rng.Intn(4)
			corrupted[csIdx] ^= 0xA5
		} else {
			corrupted = append(corrupted, 0xFF)
		}
	}

	return corrupted
}

// Inject applique aléatoirement soit l'inversion d'un bit (XOR), soit une troncature
// de taille, soit un déphasage du checksum lorsque le tirage stochastique déclenche l'erreur.
func (cm *ChaosMonkey) Inject(payload []byte) []byte {
	if cm.ShouldCorrupt() {
		return cm.Corrupt(payload)
	}
	return payload
}

// InjectWithStatus applique l'injection stochastique et retourne si une corruption a été appliquée.
func (cm *ChaosMonkey) InjectWithStatus(payload []byte) ([]byte, bool) {
	if cm.ShouldCorrupt() {
		return cm.Corrupt(payload), true
	}
	return payload, false
}

// ShouldCorrupt indique si l'événement actuel doit subir une corruption stochastique.
func (cm *ChaosMonkey) ShouldCorrupt() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.rng.Float64() < cm.ErrorRate
}
