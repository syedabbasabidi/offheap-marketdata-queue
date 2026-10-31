# AGENTS.md

## Overview

This workspace contains two related Java 11 / Maven projects for **ultra-low-latency market data transport**:

- **`offheap-marketdata-queue`** — Core library providing lock-free circular queues (off-heap via memory-mapped files and on-heap via VarHandle). This is the foundational dependency.
- **`udp-marketdata-queue`** — Network layer that bridges two `CircularMMFQueue` instances across machines using TCP or UDP broker pairs. Depends on `offheap-marketdata-queue` (published as `com.abidi:offheap-marketdata-queue:2.0`).

## Architecture & Data Flow

```
Producer → CircularMMFQueue (MMF @ /tmp/producer) → ProducerBroker ──TCP/UDP──→ ConsumerBroker → CircularMMFQueue (MMF @ /tmp/consumer) → Consumer
```

### Queue implementations (`offheap-marketdata-queue`)
| Class | Type | Concurrency | Storage |
|---|---|---|---|
| `CircularMMFQueue` | Interface for the two off-heap queues below | Single producer / single consumer | Memory-mapped files (persisted via Linux dirty page cache) |
| `CircularMMFQueueFixedSize` | Off-heap circular queue, one fixed-size slot per message | SPSC | Memory-mapped files |
| `CircularMMFQueueVariableSize` | Off-heap byte ring, each message stored next to its length (16 B header, 16 B aligned, power-of-two capacity) | SPSC | Memory-mapped files |
| `SPSCLockFreeCircularQueue` | On-heap ring buffer | SPSC, lock-free via `VarHandle` | `String[]` array |
| `MPMCLockFreeCircularQueue` | On-heap ring buffer | MPMC, lock-free via CAS (`VarHandle`) | `Slot[]` with volatile sequence numbers |

### Key design patterns
- **Cache-line padding**: Queue classes pad fields with 7 unused `long` values (`p00`–`p06`) to prevent false sharing. Preserve this layout when modifying queue fields.
- **VarHandle memory ordering**: Uses `setRelease`/`getAcquire` (writer→reader) and `setOpaque`/`getOpaque` (reader→writer) — intentionally asymmetric. Do not replace with `volatile` or `AtomicLong`.
- **Fixed-size 64-byte messages**: `MarketData` serializes to exactly 64 bytes with a defined field layout (see byte-index constants). `MarketDataCons` deserializes by reading the same byte offsets. Any field change requires updating both classes and their index constants in lockstep.
- **Ack-based dequeue**: `CircularMMFQueue.getWithAck()` + `ack()` enables at-most-once delivery used by network brokers. The simpler `get()` auto-advances the reader index.

### Network brokers (`udp-marketdata-queue`)
- `TCPProducerBroker` / `TCPConsumerBroker` — reliable delivery with ack over TCP sockets.
- `UDPProducerBroker` / `UDPConsumerBroker` — same pattern over UDP with retry-on-timeout.
- Port constants live in `com.abidi.constants.Config`; queue paths in the same file (`/tmp/producer`, `/tmp/consumer`).

## Build & Test

```bash
# Build core library first (required as dependency for udp project)
cd offheap-marketdata-queue && mvn clean install

# Build network layer
cd udp-marketdata-queue && mvn clean compile -Pgithub

# Run unit tests (JUnit 5)
cd offheap-marketdata-queue && mvn test
```

Queue sizes **must be powers of two** (enforced at construction). Tests use small sizes like 10; production defaults to `100_000` (core) or `1_000_000` (udp).

## Performance Benchmarking

Latency tests use **OpenHFT JLBH** (not JMH for end-to-end), run as `main()` methods:

- `JLBHQueueLatencyMeasurer` — measures MPMC/SPSC queue add latency.
- `JLBHProducerMMFCircularQueue` / `JLBHConsumerMMFCircularQueue` — MMF queue latency.
- `JLBHTCPProducerBroker` / `JLBHUDPProducerBroker` — end-to-end broker latency.

JMH (`jmh-core` + `jmh-generator-annprocess`) is available as a test dependency for microbenchmarks.

Linux tuning for MMF queue performance:
```bash
sysctl -w vm.dirty_writeback_centisecs=50
sudo hdparm -W0 /dev/sdX   # disable write caching
```

## Conventions

- **Zero-allocation hot path**: Queue `add`/`get` methods avoid object allocation. `ByteUtils` reuses pre-allocated `ByteBuffer` instances. Do not introduce `new` in hot-path methods.
- **No frameworks**: Pure Java — no Spring, no DI. Components are instantiated directly via constructors or `main()`.
- **Logging**: SLF4J + Logback. Hot-path logging is guarded with `LOG.isDebugEnabled()` or uses `LOG.debug()` only.
- **MarketData dual-class pattern**: `MarketData` (producer-side serializer) and `MarketDataCons` (consumer-side deserializer) are intentional separate classes operating on the same `byte[64]` layout.
- **CPU affinity**: Producer/consumer pin to specific CPUs via `Affinity.setAffinity()` (OpenHFT). CPU indices are hardcoded constants (`PIN_TO_CPU`).

