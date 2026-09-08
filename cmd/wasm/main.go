//go:build js && wasm

package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"morphocore/cell"
	"morphocore/harness"
	"morphocore/pool"
)

// Constantes de topologie de la grille
const (
	GridWidth  = 3
	GridHeight = 3
)

// Variables d'état globales pour le runtime Wasm
var (
	ctx        context.Context
	cancel     context.CancelFunc
	cellPool   *pool.CellPool
	tracker    *harness.MetricsTracker
	chaos      *harness.ChaosMonkey
	rootIn     chan []byte
	rootOut    chan []byte
	gridCells  [][]*cell.Cell

	bombardMu     sync.Mutex
	isBombarding  bool
	bombardTicker *time.Ticker
	bombardStop   chan struct{}
)

// notifyJS relaie l'événement cellulaire vers le callback JavaScript global window.onMorphoCellEvent.
func notifyJS(x, y int, state string, gen uint16, mttr float64) {
	fn := js.Global().Get("onMorphoCellEvent")
	if fn.Type() == js.TypeFunction {
		fn.Invoke(x, y, state, int(gen), mttr)
	}
}

// morphoInjectPayload injecte une chaîne ou un tableau d'octets dans le nœud d'entrée (0,0).
func morphoInjectPayload(this js.Value, args []js.Value) any {
	payloadStr := fmt.Sprintf("wasm-pkt-%d", time.Now().UnixNano()%100000)
	if len(args) > 0 {
		if args[0].Type() == js.TypeString && args[0].String() != "" {
			payloadStr = args[0].String()
		} else if args[0].Type() == js.TypeObject && args[0].InstanceOf(js.Global().Get("Uint8Array")) {
			length := args[0].Get("length").Int()
			buf := make([]byte, length)
			js.CopyBytesToGo(buf, args[0])
			payloadStr = string(buf)
		}
	}

	payload := []byte(payloadStr)
	tracker.RecordEmission(false)
	packed := cell.PackMessage(payload)

	select {
	case rootIn <- packed:
		return true
	default:
		go func() {
			rootIn <- packed
		}()
		return true
	}
}

// morphoKillCell force l'apoptose immédiate d'une coordonnée (x, y) précise.
func morphoKillCell(this js.Value, args []js.Value) any {
	if len(args) < 2 {
		return false
	}
	x := args[0].Int()
	y := args[1].Int()
	if x < 0 || x >= GridWidth || y < 0 || y >= GridHeight {
		return false
	}

	target := cellPool.GetCellAt(x, y)
	if target == nil || target.InChan == nil {
		return false
	}

	killMsg := cell.PackCorruptedMessage([]byte(fmt.Sprintf("WASM-FORCE-KILL-(%d,%d)", x, y)))
	select {
	case target.InChan <- killMsg:
		return true
	default:
		go func() {
			target.InChan <- killMsg
		}()
		return true
	}
}

// morphoSetChaosRate ajuste dynamiquement le taux d'erreur du ChaosMonkey (0% à 100%).
func morphoSetChaosRate(this js.Value, args []js.Value) any {
	if len(args) < 1 {
		return chaos.GetErrorRate()
	}
	rate := args[0].Float()
	if rate > 1.0 {
		rate = rate / 100.0
	}
	if rate < 0.0 {
		rate = 0.0
	}
	chaos.SetErrorRate(rate)
	return chaos.GetErrorRate()
}

// morphoGetTelemetry retourne une chaîne JSON formatée des métriques et du pool.
func morphoGetTelemetry(this js.Value, args []js.Value) any {
	snap := tracker.Snapshot()
	var sb strings.Builder
	sb.WriteString("{\n")
	sb.WriteString("  \"telemetry\": {\n")
	sb.WriteString(fmt.Sprintf("    \"duration_seconds\": %.6f,\n", snap.DurationSeconds))
	sb.WriteString(fmt.Sprintf("    \"throughput_msg_per_sec\": %.2f,\n", snap.ThroughputMsgPerSec))
	sb.WriteString(fmt.Sprintf("    \"total_emitted\": %d,\n", snap.TotalEmitted))
	sb.WriteString(fmt.Sprintf("    \"total_delivered\": %d,\n", snap.TotalDelivered))
	sb.WriteString(fmt.Sprintf("    \"total_corrupted\": %d,\n", snap.TotalCorrupted))
	sb.WriteString(fmt.Sprintf("    \"total_apoptosis\": %d,\n", snap.TotalApoptosis))
	sb.WriteString(fmt.Sprintf("    \"total_mitosis\": %d,\n", snap.TotalMitosis))
	sb.WriteString(fmt.Sprintf("    \"survival_rate_percent\": %.2f,\n", snap.SurvivalRatePercent))
	sb.WriteString(fmt.Sprintf("    \"mttr_microseconds\": %.2f,\n", snap.MTTRMicroseconds))
	sb.WriteString(fmt.Sprintf("    \"min_recovery_microseconds\": %.2f,\n", snap.MinRecoveryTimeMicros))
	sb.WriteString(fmt.Sprintf("    \"max_recovery_microseconds\": %.2f,\n", snap.MaxRecoveryTimeMicros))
	sb.WriteString("    \"cells\": {\n")
	first := true
	for id, stats := range snap.Cells {
		if !first {
			sb.WriteString(",\n")
		}
		first = false
		sb.WriteString(fmt.Sprintf("      %q: {\n", id))
		sb.WriteString(fmt.Sprintf("        \"apoptosis_count\": %d,\n", stats.ApoptosisCount))
		sb.WriteString(fmt.Sprintf("        \"mitosis_count\": %d,\n", stats.MitosisCount))
		sb.WriteString(fmt.Sprintf("        \"max_generation\": %d\n", stats.MaxGeneration))
		sb.WriteString("      }")
	}
	sb.WriteString("\n    }\n")
	sb.WriteString("  },\n")
	sb.WriteString("  \"pool\": {\n")
	sb.WriteString(fmt.Sprintf("    \"capacity\": %d,\n", cellPool.Capacity()))
	sb.WriteString(fmt.Sprintf("    \"active_count\": %d,\n", cellPool.ActiveCount()))
	sb.WriteString(fmt.Sprintf("    \"max_active\": %d,\n", cellPool.MaxActiveCount()))
	sb.WriteString(fmt.Sprintf("    \"total_acquired\": %d,\n", cellPool.TotalAcquired()))
	sb.WriteString(fmt.Sprintf("    \"total_released\": %d\n", cellPool.TotalReleased()))
	sb.WriteString("  }\n")
	sb.WriteString("}")
	return sb.String()
}

// morphoStartBombardment démarre un flux régulier de paquets dans le réseau.
func morphoStartBombardment(this js.Value, args []js.Value) any {
	rateMsgsPerSec := 20
	if len(args) > 0 && args[0].Int() > 0 {
		rateMsgsPerSec = args[0].Int()
	}

	bombardMu.Lock()
	defer bombardMu.Unlock()
	if isBombarding {
		return true
	}

	isBombarding = true
	bombardStop = make(chan struct{})
	interval := time.Second / time.Duration(rateMsgsPerSec)
	if interval < 2*time.Millisecond {
		interval = 2 * time.Millisecond
	}
	bombardTicker = time.NewTicker(interval)

	go func() {
		var seq uint64
		for {
			select {
			case <-bombardStop:
				return
			case <-ctx.Done():
				return
			case <-bombardTicker.C:
				seq++
				payload := []byte(fmt.Sprintf("bombard-%06d", seq))
				tracker.RecordEmission(false)
				select {
				case rootIn <- cell.PackMessage(payload):
				default:
				}
			}
		}
	}()

	return true
}

// morphoStopBombardment arrête le flux automatique de paquets.
func morphoStopBombardment(this js.Value, args []js.Value) any {
	bombardMu.Lock()
	defer bombardMu.Unlock()
	if isBombarding {
		isBombarding = false
		if bombardTicker != nil {
			bombardTicker.Stop()
		}
		if bombardStop != nil {
			close(bombardStop)
		}
	}
	return true
}

func main() {
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	cellPool = pool.NewCellPool()
	chaos = harness.NewChaosMonkey(0.0) // Démarre en régime nominal 0% de corruption
	tracker = harness.NewMetricsTracker()

	rootIn = make(chan []byte, 300)
	rootOut = make(chan []byte, 300)

	// Callback pour propager les transitions d'état vers JavaScript
	eventHook := func(x, y int, state string, gen uint16) {
		var mttr float64
		if tracker != nil {
			snap := tracker.Snapshot()
			mttr = snap.MTTRMicroseconds
		}
		notifyJS(x, y, state, gen, mttr)
	}

	// Déploiement de la grille 3x3 adossée au pool statique et au hook d'événements
	gridCells = cell.BuildMeshGridWithPoolAndHook(
		GridWidth, GridHeight,
		cellPool, tracker, chaos,
		rootIn, rootOut,
		eventHook,
	)

	// Démarrage des boucles de vie cellulaires concurrentes
	for x := 0; x < GridWidth; x++ {
		for y := 0; y < GridHeight; y++ {
			go gridCells[x][y].Run(ctx)
		}
	}

	// Consommateur terminal de sortie (Sink au nœud 2,2)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-rootOut:
				if !ok {
					return
				}
				payload, hash, valid := cell.UnpackMessage(msg)
				if valid && gridCells[GridWidth-1][GridHeight-1].Genome.Verify(payload, hash) {
					tracker.RecordSuccess()
				}
			}
		}
	}()

	// Enregistrement des fonctions passerelles dans le scope global JavaScript
	js.Global().Set("morphoInjectPayload", js.FuncOf(morphoInjectPayload))
	js.Global().Set("morphoKillCell", js.FuncOf(morphoKillCell))
	js.Global().Set("morphoSetChaosRate", js.FuncOf(morphoSetChaosRate))
	js.Global().Set("morphoGetTelemetry", js.FuncOf(morphoGetTelemetry))
	js.Global().Set("morphoStartBombardment", js.FuncOf(morphoStartBombardment))
	js.Global().Set("morphoStopBombardment", js.FuncOf(morphoStopBombardment))

	// Notification initiale pour que l'interface affiche l'état nominal de chaque cellule
	time.Sleep(20 * time.Millisecond)
	for x := 0; x < GridWidth; x++ {
		for y := 0; y < GridHeight; y++ {
			c := gridCells[x][y]
			notifyJS(x, y, "Normal", c.Generation, 0)
		}
	}

	fmt.Println("[MORPHOCORE WASM] Initialisé avec succès. 9 cellules actives sur CellPool statique.")

	// Empêche la goroutine principale de terminer pour maintenir le runtime Wasm actif
	select {}
}
