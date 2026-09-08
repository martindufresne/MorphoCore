# MorphoCore

> **Concurrent, Decentralized, Self-Healing Morphogenetic Network Engine**  
> *Compatible with Standard Go, TinyGo bare-metal, and WebAssembly (Wasm)*

**[🇬🇧 English](README.md)** • **[🇫🇷 Lire en Français](README.fr.md)**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Test](https://img.shields.io/badge/Go_Test-Pass-10b981?logo=go&logoColor=white)](.)
[![TinyGo](https://img.shields.io/badge/TinyGo-0.42.0-blue?logo=webassembly&logoColor=white)](.)
[![Wasm Size](https://img.shields.io/badge/Wasm_Binary-391_KB-purple)](.)
[![Zero Alloc](https://img.shields.io/badge/Post--Boot_Allocations-0_B%2Fop-emerald)](.)
[![Race Detector](https://img.shields.io/badge/Race_Detector-Clean-brightgreen)](.)

---

## Overview

**MorphoCore** is a proof-of-concept (PoC) exploring software architectures inspired by **biological morphogenesis**. The system simulates a mesh of concurrent cellular units interconnected in a 2D grid, governed by an **immutable mathematical DNA**.

Subjected to environmental noise, random fuzzing, and stochastic bit-flips, each cell enforces a strict **zero-tolerance** policy:
1. **Immediate Apoptosis**: Upon any invariant violation or checksum mismatch, the cell terminates its execution loop, purges its sensitive local memory (`memset`), and returns its slot to a pre-allocated static pool.
2. **Dynamic Gradient Bypassing**: The continuous message flux is immediately detoured by surviving neighbors using an adaptive gradient routing algorithm.
3. **Decentralized Mitosis**: A stem cell is deterministically elected among immediate cardinal peers to reincarnate the dead node without any central orchestrator or master routine.
4. **Zero Heap Allocation Post-Boot**: The engine is hardened for embedded bare-metal and WebAssembly targets, eliminating all dynamic heap allocations post-initialization (`0.00 B/op`).

---

## Architectural Pillars

```text
                           [Root Input (0,0)]
                                   │
                     ┌─────────────┼─────────────┐
                     ▼             ▼             ▼
              [Soma (0,0)] ── [Axone (1,0)] ── [Synapse (2,0)]
                     │             │             │
                     ▼             ▼             ▼
              [Soma (0,1)] ── [Axone (1,1)] ── [Synapse (2,1)]
                     │             │             │
                     ▼             ▼             ▼
              [Soma (0,2)] ── [Axone (1,2)] ── [Synapse (2,2)]
                                                 │
                                                 ▼
                                        [Sink Output (2,2)]
```

### 1. Immutable Mathematical DNA (`dna/`)
- Strict **32-bit FNV-1a** checksum verification and mathematical signatures prefixing every payload.
- Functional biological families:
  - **Soma (x = 0)**: Ingestion, formatting, and initial injection.
  - **Axone (0 < x < N-1)**: Propagation, gradient routing, and transit.
  - **Synapse (x = N-1)**: Terminal consolidation and delivery to sink.
- Zero tolerance: Any single bit-flip or size truncation immediately triggers channel shutdown and apoptosis.

### 2. Elimination of Central Coordinator (`cell/`, `protocol/`)
- Pure peer-to-peer (P2P) architecture with no coordinator or single point of failure.
- Periodic heartbeat signals (`SignalPing`) and typed control messages (`SignalMitosisRequest`, `SignalMitosisAck`, `SignalRouteUpdate`).
- **Deterministic Election Algorithm**: Cardinal priority order `West > North > South > East` with delayed safety fallback (30 ms) to absorb simultaneous multi-node failures.

### 3. 2D Mesh Topology & Dynamic Routing (`cell/mesh.go`)
- 2D grid topology where each cell maintains up to 4 cardinal links.
- Routing via Manhattan distance minimization toward target sink `(TargetX, TargetY)`.
- Tested and verified resilience against **simultaneous adjacent failures** (e.g., simultaneous destruction of `(1,1)` and `(1,2)` with traffic seamlessly detouring via grid borders).

### 4. Static Slab Allocator & TinyGo Hardening (`pool/`)
- **Pre-allocated static slab allocator** of fixed capacity (`storage [18]CellSlot`).
- **0.00 B/op post-boot**: Rigorously validated via `testing.AllocsPerRun`.
- **Explicit Memory Purge (`memset`)**: Byte-by-byte zeroing of sensitive cell state buffers (`StateBuf [128]byte`) upon apoptosis before returning the slot to the pool.
- Pre-allocated channels and compact `[4]*PeerLink` arrays reused in-place (no heap maps, no reflection).

### 5. WebAssembly Arena & Interactive Testbench (`cmd/wasm/`)
- Compiled with TinyGo into a compact **391 KB** WebAssembly binary (< 500 KB).
- Bidirectional `syscall/js` bridge exposing engine primitives to the browser:
  - `morphoInjectPayload(str|bytes)`: Injects packets into root node.
  - `morphoKillCell(x, y)`: Targeted force-kill to trigger apoptosis on demand.
  - `morphoSetChaosRate(rate)`: Real-time stochastic corruption control.
  - `morphoGetTelemetry()`: Structured JSON export of homeostasis metrics and pool utilization.
- Real-time event notifications emitted to `window.onMorphoCellEvent(x, y, state, generation, mttr)`.

---

## Repository Structure

```text
morphocore/
├── README.md               # Main English documentation
├── README.fr.md            # French documentation
├── LICENSE                 # MIT License
├── netlify.toml            # Automated Netlify deployment configuration
├── go.mod                  # Go module definition
├── main.go                 # Native CLI endurance benchmark (1,000 stress cycles)
├── dna/
│   ├── dna.go              # Mathematical invariants, FNV-1a, biological families
│   └── dna_test.go         # DNA invariant verification and zero-allocation tests
├── cell/
│   ├── cell.go             # Cell lifecycle loop, channels, apoptosis, and memset purge
│   ├── peer.go             # P2P neighbor links and acknowledgement tracking
│   ├── mesh.go             # 2D mesh grid, gradient routing, stem cell election
│   ├── cell_test.go        # Unit tests for cell lifecycle and apoptosis
│   └── mesh_test.go        # Tests for simultaneous node failures & dynamic bypass
├── pool/
│   ├── pool.go             # Static Slab Allocator (CellPool, 18 fixed slots)
│   └── pool_test.go        # Strict validation of 0 B/op, memset, and concurrent stress
├── protocol/
│   ├── heartbeat.go        # P2P inter-cell signaling types
│   └── heartbeat_test.go   # Unit validation of signal payloads
├── harness/
│   ├── chaos.go            # Stochastic ChaosMonkey (bit-flips, truncation, phase shifts)
│   ├── chaos_test.go       # Tests for stochastic anomaly distribution
│   ├── telemetry.go        # Homeostasis tracker, throughput, MTTR, and JSON export
│   └── telemetry_test.go   # Validation of latency and recovery calculations
└── cmd/
    └── wasm/
        ├── main.go         # WebAssembly entry point (syscall/js bridge)
        ├── index.html      # Interactive WebAssembly testbench (3x3 grid, live logs, charts)
        ├── wasm_exec.js    # Official TinyGo 0.42.0 runtime glue
        └── morphocore.wasm # Compiled and optimized WebAssembly binary (391 KB)
```

---

## Requirements & Prerequisites

- **Go**: version 1.22 or higher
- **TinyGo**: version 0.35+ (tested with TinyGo 0.42.0)
- **Modern Web Browser**: Chrome, Firefox, Safari, or Edge with WebAssembly support.

---

## Usage Guide

### 1. Run Unit & Concurrency Tests (Standard Go with Race Detector)

```bash
go test -v -count=1 -race ./...
```

### 2. Run Tests with TinyGo

```bash
tinygo test ./...
```

### 3. Verify Zero Heap Allocation Post-Boot

```bash
go test -v -run TestPool_ZeroAllocationsPostBoot ./pool
```
*Expected output: `Allocations per run: 0.00 B/op (0 allocs)`.*

### 4. Run Native CLI Endurance Benchmark (1,000 Cycles)

```bash
go run main.go
```

This benchmark executes:
- **Phase 1**: Nominal transit of 200 packets.
- **Phase 2**: Simultaneous grouped attack on `(1,1)` and `(1,2)` under continuous high-frequency traffic.
- **Phase 3**: Horizontal attack on `(0,1)` and `(1,1)` with decentralized election and fallback safety.
- **Final Report**: Full homeostasis report and slab allocator runtime metrics.

---

## Interactive WebAssembly Arena

### Compiling the Wasm Binary

```bash
tinygo build -no-debug -opt=z -o cmd/wasm/morphocore.wasm -target=wasm ./cmd/wasm/main.go

# Optional size optimization via Binaryen
wasm-opt -Oz cmd/wasm/morphocore.wasm -o cmd/wasm/morphocore.wasm
```

### Running the Arena Locally

Start a local HTTP server serving the `cmd/wasm` directory:

```bash
python3 -m http.server 8080 --directory cmd/wasm
```

Then navigate to: **[http://localhost:8080/index.html](http://localhost:8080/index.html)**

### Deploying to Netlify

The repository includes a ready-to-use [`netlify.toml`](netlify.toml). To deploy:
1. Connect your repository to [Netlify](https://www.netlify.com/).
2. Netlify will automatically detect `cmd/wasm` as the publish directory and set the required `Content-Type: application/wasm` header.
3. Your live interactive arena is instantly available worldwide.

### Arena Features:
- **Precision Strike**: Click on any cell in the 3x3 matrix to force immediate apoptosis.
- **Grouped Burst Attack**: One-click destruction of `(1,1)` and `(1,2)` simultaneously to watch gradient traffic bypass in real time.
- **Continuous Bombardment**: Adjustable throughput slider (5 to 100 msg/s).
- **Chaos Slider**: Dynamic stochastic corruption injection (0% to 50%).
- **Slab Allocator Gauge**: Live visualization of pool slot utilization (`X / 18 slots`) and `memset` memory wipes.
- **JSON Session Export**: One-click download of full resilience and MTTR session telemetry.

---

## Benchmarks & Resilience Metrics

| Metric | Typical Value | Description |
|---|---|---|
| **Mean Time to Repair (MTTR)** | **~21 µs to 200 µs** | Latency elapsed between apoptosis and traffic resumption |
| **Network Survival Rate** | **100.0%** | Zero runtime crashes, complete autonomous self-healing |
| **Post-Boot Heap Allocations** | **0.00 B/op** | Zero garbage collection pauses during nominal & mitigation cycles |
| **Static Pool Capacity** | **18 slots** | 9 active cells + 9 mitigation margin slots |
| **Wasm Binary Footprint** | **391 KB** | Ultra-lightweight instant browser download (< 500 KB) |

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
