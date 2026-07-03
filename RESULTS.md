# Fleet benchmark — ASYNC_STICKY vs ASYNC_DISTRIBUTED

**Setup:** 5 workers (own gRPC channels, `max_concurrency=16` each) alternating across **2 relay-server
instances** (`:9090`, `:9091`) on one shared Postgres. Workflow (identical in both modes, loan-approval-style):
`validate → PARALLEL[creditScore, fraudCheck, kycCheck] → decide` — 5 tasks/run, each simulating 8ms of work.
200 runs per mode, 10 concurrent submitters, 20-run warmup. Latency = submit → COMPLETED
(sticky: the `runAsync` round trip; distributed: fire-and-forget + 20ms status sampling, so distributed numbers
carry ≤20ms measurement noise). Machine: local dev laptop, 2026-07-03, schema V9.

## Work spreading (the point of distributed mode)

30 fire-and-forget runs = 150 task executions, pulled competitively by 5 pollers:

```
fleet-w1: 33   fleet-w2: 30   fleet-w3: 32   fleet-w4: 19   fleet-w5: 36
```

Benchmark phase (1000 executions): `w1=231 w2=191 w3=175 w4=183 w5=220` — evenly spread, no coordination,
purely competitive claims. In sticky mode each submitter executes all 5 tasks of its own runs.

## Latency / throughput

Poll cadence 100ms (conservative default):

| mode | runs | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| ASYNC_STICKY | 200 | 200 | 40.3 | 46.7 | 50.3 | 104.7 | 105.5 | 1.00s | **201.0** |
| ASYNC_DISTRIBUTED | 200 | 200 | 170.2 | 370.4 | 516.4 | 1676.7 | 1751.6 | 8.70s | 23.0 |

Poll cadence 25ms (`-Dfleet.poll-ms=25`):

| mode | runs | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| ASYNC_STICKY | 200 | 200 | 43.2 | 48.7 | 84.8 | 114.1 | 114.8 | 1.13s | 177.6 |
| ASYNC_DISTRIBUTED | 200 | 200 | 101.2 | 221.6 | 312.6 | 423.5 | 453.8 | 4.69s | **42.6** |

(all values ms unless noted; 0 failures in every phase — including the tx deadlock-retry machinery holding
under 10-way concurrent parallel-join completions against 2 competing servers)

## Reading the numbers

- **Sticky wins raw latency** — the submitter drives every wave itself with zero queue hops: p50 ≈ compute
  (3 sequential waves × 8ms) + gRPC round trips.
- **Distributed latency = queue cadence, not compute.** Each of the 3 waves waits for a poller pull, so the
  floor is ~3 × (pull wait) + compute; the p99 tail is queue depth: 1000 dispatches drained at
  ~(workers × batch 16) per poll tick. Halving the poll interval roughly halved p50 and cut p99 4×.
- **What distributed buys:** fire-and-forget submits (the submitter is free immediately), fleet-wide load
  spreading, worker interchangeability (any worker crash → others pull its redelivered work), and capacity
  capping per worker. The cost is pull-cadence latency — tune `poll-ms` per deployment, or move to
  server-streaming push delivery if sub-50ms distributed latency is ever required.

Reproduce: two servers up (`:9090` + `:9091`), then `java [-Dfleet.poll-ms=25] -jar target/relay-fleet-1.0.0.jar`.

---

# JVM / Netty / spring-gRPC tuning pass (2026-07-03)

**Changes measured** (all @25ms poll, 200 runs/mode, 10 submitters, 2 servers):
- **Server executor → virtual-thread-per-call** (`GrpcServerExecutorProvider`): Relay handlers block on JDBC
  (one event = one tx); gRPC's default shared executor serialized them under concurrency (everything ran on
  `grpc-default-executor-0`). This was the bottleneck.
- Netty: `TCP_NODELAY` child option, `SO_BACKLOG 256` (`ServerBuilderCustomizer<NettyServerBuilder>`).
- Server keepalive props (`spring.grpc.server.keepalive.permit.*`) + client channels keep-alive 30s +
  vthread callback executor (SDK auto-config + fleet).
- Hikari `maximum-pool-size 32` (with vthreads, the DB pool IS the concurrency limit — default 10 throttled).
- Fixed a silently-ignored property: `spring.grpc.server.shutdown-grace-period` → the real name is
  `spring.grpc.server.shutdown.grace-period`.

| variant | sticky p50 | p90 | p99 | runs/s | dist p50 | p99 | runs/s |
|---|---|---|---|---|---|---|---|
| pre-tuning baseline | 48.7 | 84.8 | 114.1 | 177.6 | 221.6 | 423.5 | 42.6 |
| **V1 code-tuned, default G1** | **44.8** | **50.5** | **95.4** | **211.9** | 218.9 | **388.5** | 42.7 |
| V2 = V1 + ZGC, 512m fixed, pretouch | 47.8 | 53.7 | 108.2 | 196.1 | 221.0 | 387.8 | 43.3 |
| V3 = V1 + G1, 512m fixed, compact headers | 45.4 | 57.9 | 100.2 | 195.8 | 235.3 | 415.0 | 40.7 |

**Reading:** the executor + pool fix is the real win — sticky **p90 −40% (84.8→50.5ms)**, p99 −16%,
throughput **+19%**, far outside run-to-run noise (±5–10%). GC/heap variants are noise at this scale: the
workload allocates little and the heap is small, so GC was never the bottleneck — the RPC executor and DB pool
were. ZGC / compact object headers are worth re-measuring under production heap sizes and allocation rates,
not here. Distributed mode barely moves in all variants — as established, its latency floor is poll cadence,
not server runtime.

**Kept as defaults:** V1 (code-level tuning, stock G1/heap). No JVM flags required.

---

# Linux run — constrained "production-shape" topology (2026-07-03)

**Machine:** Ubuntu (kernel 7.0), AMD Ryzen 5 7535HS (6c/12t), ~13GB RAM, local NVMe, Postgres 16.14
same box. Java 25 (Corretto), schema V9, engine `0.1.0-SNAPSHOT` @ a442556 + surefire-env-pin fix.
Full reactor green on this box first (127 tests, 0 failures).

**Deliberate sizing (user-requested):** each relay-server pinned to **2 logical CPUs** (one SMT pair via
`taskset`; A=0,1 B=2,3) with a **4GB budget** (`-Xms512m -Xmx3g`); fleet client (5 workers + 10 submitters,
one JVM) pinned to 4 logical CPUs with **`-Xmx1g`** — 1GB was comfortably enough (no GC pressure, 0 failures).
Postgres unpinned (floats, mostly on the 4 spare threads). Same workload as above: 200 runs/mode,
10 submitters, 20-run warmup, 5×8ms tasks.

**epoll confirmed active** on both servers (`NettyGrpcServerFactory` + `netty_transport_native_epoll_x86_64`
visible in `/proc/<pid>/maps`) — checklist item 1 done. **Deviations from the checklist:** Postgres shares
the box and storage (item 2), bench client shares the box (item 3), and the CPU governor was **powersave**
(performance governor needs sudo; retest tails with it before trusting p99 for capacity planning).

Poll cadence 100ms:

| mode | runs | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| ASYNC_STICKY | 200 | 200 | 51.0 | 67.2 | 83.0 | 104.9 | 111.1 | 1.42s | **141.1** |
| ASYNC_DISTRIBUTED | 200 | 200 | 173.9 | 323.6 | 430.4 | 540.2 | 562.0 | 6.77s | 29.6 |

Poll cadence 25ms (two runs — tail variance shown deliberately):

| run | mode | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| 1 | ASYNC_STICKY | 200/200 | 46.6 | 61.9 | 77.2 | 121.2 | 136.9 | 1.31s | 152.4 |
| 1 | ASYNC_DISTRIBUTED | 200/200 | 108.7 | 216.0 | 347.1 | 1258.4 | 1319.5 | 5.76s | 34.7 |
| 2 | ASYNC_STICKY | 200/200 | 44.4 | 56.4 | 69.6 | 105.5 | 113.4 | 1.23s | **163.1** |
| 2 | ASYNC_DISTRIBUTED | 200/200 | 108.5 | 214.8 | 303.0 | 382.6 | 424.7 | 4.63s | **43.2** |

**Reading:**
- **The model survives 2-core servers intact.** Sticky p50 67→56ms vs 45-49ms on the unconstrained macOS
  laptop — only ~25% slower on a sixth of the cores, because handlers are JDBC-blocking vthreads, not
  CPU-bound. Zero failures anywhere; deadlock-retry machinery held as before.
- **Distributed is still pure poll cadence:** p50 ≈ 216ms @ 25ms poll, ~324ms @ 100ms — same shape as macOS;
  server CPU size is irrelevant to it, exactly as predicted.
- **Distributed p99 is bimodal across runs** (382ms vs 1258ms at 25ms): occasionally a wave misses a whole
  pull round on the constrained cores. Suspect powersave governor + SMT-pair contention; re-measure with the
  performance governor before reading anything into distributed tails.
- **Worker heap:** 1GB is plenty for a 5-worker fleet JVM at this rate; no need to try smaller unless
  memory-squeezing matters.
- JVM variants (ZGC / compact headers) not re-measured here — heaps are capped at 3g/1g, so this box still
  can't answer the "production heap" question from item 4.

---

# Linux soak — SYNC vs ASYNC_STICKY, 30 min sustained each (2026-07-03)

Same box + constrained topology as above (2-logical-CPU/4GB servers, fleet client 4 CPUs `-Xmx1g`,
Postgres 16.14 stock config same box, powersave governor). Loan-approval workflow, closed loop with
**10 concurrent submitters** per mode, 30s warmup, fresh DB. Harness: `SoakRunner`
(`--spring.profiles.active=soak`, knobs `-Dsoak.minutes/-Dsoak.submitters/-Dsoak.modes`); raw
per-minute log + 10s telemetry CSV in `artifacts/`.

Per run — SYNC: worker walks the graph locally (PARALLEL branches **sequential** → 5×8ms = 40ms serial
compute) + 12 checkpoint RPCs (start + 2 recordStep/task + complete), each a server-side tx.
STICKY: server drives 3 dispatch waves (branches concurrent → 24ms serial compute), ~6-8 heavier
event txs (graph walk + expression eval + scope joins happen server-side).

| mode | runs (30 min) | err | runs/s | min | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|
| SYNC | 288,334 | **0** | 160.2 | 49.9 | 58.1 | 85.4 | 96.4 | 183.0 | 225.7 |
| ASYNC_STICKY | 334,339 | **0** | **185.7** | 40.7 | **52.4** | 61.1 | **76.4** | 166.4 | 205.1 |

Telemetry averages over each 30-min phase (CPU in single-core units; each server's budget = 200%):

| phase | serverA | serverB | fleet | postgres | pg commits/s | hikari pending>0 |
|---|---|---|---|---|---|---|
| SYNC | 72% | 57% | 31% | 73% | 2,006 | 1 sample / 300 |
| STICKY | 112% | 87% | 34% | ~295%* | 1,201 | (same) |

*includes autovacuum working off the by-then 1-2GB of SYNC-phase data; per-pid sampling has
backend-churn noise. DB grew 0 → 590MB (SYNC) → **2.09GB** total (~3.3KB durable trail per run).
GC (G1, 3g cap): typical young pause ~1.5ms; 676/456 pauses over 65 min; **max 65.9ms** — rare, and
it lines up with the p99.9 band, not p99.

**Findings / bottleneck attribution (in order):**
1. **Closed-loop latency, not server capacity, caps throughput at 10 submitters** — runs/s ≈
   submitters ÷ p50 in both modes (10/0.052 ≈ 192). Servers sat at 36% (SYNC) / 56% (STICKY) of their
   2-core budgets and the Hikari pool (32) queued in 1 sample out of 300. There is roughly 2× headroom
   at this sizing — scale submitters to find the true knee.
2. **Postgres is the first real server-side ceiling, and it's config, not engine.** The SYNC timeline
   sawtooths (176/s early → 126-156/s dips around min 22-29 → 169/s again in min 30): stalls line up
   with autovacuum on the hot `node_output` partitions (~11.5k dead tuples each) and WAL-forced
   checkpoints (`checkpoints_req > checkpoints_timed`) on a **stock config**: `shared_buffers=128MB`
   (DB is 16× that by the end), `max_wal_size=1GB`, `wal_buffers=4MB`. Not a leak: full recovery
   within the run. First fixes: shared_buffers ≈ 25% RAM, max_wal_size 8-16GB +
   `checkpoint_completion_target=0.9`, `wal_buffers=64MB`, more aggressive per-partition autovacuum.
3. **STICKY beats SYNC on both axes** (p50 52 vs 58ms, 186 vs 160 runs/s) despite server-side
   orchestration: its PARALLEL branches actually run concurrently (24 vs 40ms compute floor) and it
   makes ~half the RPCs. The price is server CPU (112% vs 72%) and heavier per-event DB work. SYNC's
   12 light checkpoint txs push commit rate (2,006/s) but its latency is dominated by its own serial
   graph walk. If SYNC-mode throughput ever matters: batch/pipeline the STARTED+COMPLETED checkpoint
   pairs (halves its RPC count) — engine change, parked.
4. **Engine correctness under soak: clean.** 622,673 runs, 0 errors, 0 dead letters, no drift in p50
   over 30 min in either mode, no pool exhaustion, no GC pressure (fleet worker at 1GB heap: max
   pause 33ms, mostly <2ms).

---

# Optimization pass + validation (2026-07-03, same box/topology)

**Applied** (from the soak findings, in bottleneck order):
- **Postgres** (ALTER SYSTEM, cluster-wide, user-approved): `shared_buffers` 128MB→**2GB**,
  `max_wal_size` 1GB→**8GB**, `checkpoint_completion_target` **0.9**, `wal_buffers` 4MB→**64MB**,
  `autovacuum_vacuum_scale_factor` 0.2→**0.05**, `autovacuum_vacuum_cost_limit` **2000**.
- **CPU governor** powersave→**performance**.
- **Server JVM**: fixed heap `-Xms3g -Xmx3g`, `-XX:+UseCompactObjectHeaders`,
  `--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow` (future-proofs Netty's
  Unsafe fast paths against JDK 26+ blocking; not a speed change today). Worker JVM: same native-access
  flags, `-Xms1g -Xmx1g`.

Validation: fresh DB, 10 min/point (vs 30-min baseline — long enough since the baseline's degradation
showed within 10 min), same workflow/warmup. Raw logs in `artifacts/validation.log` + `telemetry2.csv`.

| run | mode @ submitters | runs | err | runs/s | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|
| baseline | SYNC @10 | 288,334 | 0 | 160.2 | 58.1 | 85.4 | 96.4 | 183.0 | 225.7 |
| **tuned** | SYNC @10 | 105,101 | 0 | **175.2** | 56.8 | **58.7** | **63.3** | **110.3** | 201.7 |
| baseline | STICKY @10 | 334,339 | 0 | 185.7 | 52.4 | 61.1 | 76.4 | 166.4 | 205.1 |
| **tuned** | STICKY @10 | 119,987 | 0 | **200.0** | **49.8** | **52.5** | **59.6** | **67.0** | **78.6** |
| tuned | STICKY @20 | 169,587 | 0 | **282.6** | 69.3 | 81.4 | 96.3 | 114.6 | 216.9 |
| tuned | STICKY @40 | 185,588 | 0 | **309.3** | 133.7 | 159.6 | 192.7 | 254.9 | 325.2 |

**Reading:**
- **The tail is gone, not trimmed.** STICKY @10: p99 76→60ms, p99.9 166→67ms, max 205→79ms. The
  entire >80ms population vanished — that was the powersave governor + Postgres checkpoint/vacuum
  stalls, not the engine. SYNC p90 85→59ms (−31%), and its per-minute timeline is now flat
  (170-177 runs/s every minute; the 126 runs/s sawtooth dips are eliminated).
- **Throughput knee found: ~300 runs/s at this 2-core×2-server sizing.** 10→20 submitters: +41%
  throughput (283/s) with server A at **197% of its 200% CPU budget** — saturated. 20→40: only +9%
  more (309/s) while p50 doubled (69→134ms) — pure queueing past the knee. The ceiling is the
  2-core server CPU, and it lands on server A first because the 5-worker fleet splits 3/2 across
  the two servers (~170 runs/s per server core-pair, uneven load).
- Capacity rule of thumb from this data: **~85 loan-approval runs/s per server core** (STICKY,
  5×8ms tasks, tuned Postgres on same box) with p99 < 100ms while below ~80% CPU. Scale out
  servers (or cores) and keep worker→server assignment balanced before any further stack tuning.
- 0 errors in all 580k validation runs; DB to 2.4GB; hikari pending stayed 0 at every load point —
  pool 32 is right for 2-core servers.

## Benchmarking on Linux (checklist)

The macOS numbers above are valid for RELATIVE comparisons only. For capacity-planning numbers, rerun on
Ubuntu (or wherever production runs):
1. **epoll is now wired**: relay-server ships `netty-transport-native-epoll` (x86_64 + aarch_64) — on Linux the
   non-shaded grpc-netty server picks the native transport automatically (verify in the startup log; force with
   `spring.grpc.server.netty.transport` if needed). Workers get epoll free via grpc-netty-shaded.
2. Put Postgres on separate storage/host — one-event-one-transaction puts fsync latency inside handler latency.
3. Separate the bench client from the servers (everything shared one laptop here).
4. Re-measure the JVM variants (ZGC / compact object headers) at production heap sizes before trusting the
   "noise" verdict; disable CPU frequency scaling / use performance governor for stable tails.
