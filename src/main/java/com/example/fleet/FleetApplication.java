package com.example.fleet;

import com.cs.relay.worker.GrpcCoordinationClient;
import com.cs.relay.worker.Worker;
import com.example.fleet.Model.LoanIn;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fleet demo + benchmark. Boots FIVE workers (own gRPC channels + ids, alternating across TWO relay-server
 * instances at :9090/:9091), all running the same loan-approval-style workflow
 * (validate → PARALLEL[credit, fraud, kyc] → decide) in BOTH async modes, then:
 *   1. proves ASYNC_DISTRIBUTED work-spreading (per-worker execution counts), and
 *   2. benchmarks ASYNC_STICKY vs ASYNC_DISTRIBUTED end-to-end (p50/p90/p99 + throughput).
 */
@SpringBootApplication(excludeName = "com.cs.relay.worker.spring.RelayWorkerAutoConfiguration")
public class FleetApplication {

    public static void main(String[] args) { SpringApplication.run(FleetApplication.class, args); }

    static final int WORKERS = 5;
    static final int RUNS = 200;               // per benchmarked mode
    static final int SUBMITTERS = 10;          // concurrent submitters per phase
    static final int WARMUP = 20;
    static final long POLL_MS = Long.getLong("fleet.poll-ms", 100);   // pull-loop interval (distributed consumption)
    static final long STATUS_SAMPLE_MS = 20;   // completion sampling for fire-and-forget runs (adds ≤20ms noise)

    record Fleet(List<Worker> workers, List<ManagedChannel> channels) {}

    @Bean
    @org.springframework.context.annotation.Profile("!soak")   // --spring.profiles.active=soak runs SoakRunner instead
    CommandLineRunner demo() {
        return args -> {
            ObjectMapper json = new ObjectMapper();
            Fleet fleet = connectFleet(json);
            List<Worker> ws = fleet.workers();
            try {
                banner("FLEET: 5 workers x 2 servers (w1,w3,w5 -> :9090 | w2,w4 -> :9091)");

                // ---------- 1. work-spreading proof (30 distributed runs, watch who executes) ----------
                banner("ASYNC_DISTRIBUTED work spreading — 30 fire-and-forget runs, 5 competing pollers");
                FleetTasks.resetCounters();
                List<String> roots = new ArrayList<>();
                for (int i = 0; i < 30; i++)
                    roots.add(ws.get(i % WORKERS).runAsync("loan-distributed", input(i)).rootId());
                awaitAll(ws.get(0), roots, 60_000);
                FleetTasks.perWorkerTotals().forEach((w, n) -> System.out.printf("   %-10s executed %3d task(s)%n", w, n));
                System.out.printf("   (30 runs x 5 tasks = %d executions pulled competitively)%n",
                        FleetTasks.perWorkerTotals().values().stream().mapToLong(Long::longValue).sum());

                // ---------- 2. benchmark ----------
                banner("BENCHMARK — " + RUNS + " runs/mode, " + SUBMITTERS + " concurrent submitters, 2 server instances");
                System.out.printf("   task work: 5 x %dms/run | distributed poll: %dms x %d workers | status sampling: %dms%n%n",
                        FleetTasks.WORK_MS, POLL_MS, WORKERS, STATUS_SAMPLE_MS);
                for (int i = 0; i < WARMUP; i++) {                                     // warmup both paths (untimed)
                    ws.get(i % WORKERS).runAsync("loan-sticky", input(i));
                    awaitAll(ws.get(0), List.of(ws.get(i % WORKERS).runAsync("loan-distributed", input(i)).rootId()), 30_000);
                }

                FleetTasks.resetCounters();
                Result sticky = benchSticky(ws);
                Map<String, Long> stickySpread = FleetTasks.perWorkerTotals();

                FleetTasks.resetCounters();
                Result dist = benchDistributed(ws);
                Map<String, Long> distSpread = FleetTasks.perWorkerTotals();

                banner("RESULTS");
                System.out.printf("   %-18s %5s %5s %8s %8s %8s %8s %8s %10s %8s%n",
                        "mode", "runs", "ok", "min", "p50", "p90", "p99", "max", "makespan", "runs/s");
                System.out.println("   " + sticky.row("ASYNC_STICKY"));
                System.out.println("   " + dist.row("ASYNC_DISTRIBUTED"));
                System.out.println();
                System.out.println("   task executions per worker (5 tasks/run):");
                System.out.printf("     sticky:      %s%n", stickySpread);
                System.out.printf("     distributed: %s%n", distSpread);
                System.out.println("""

                   notes: sticky latency = runAsync round trip (the submitter drives every wave itself).
                          distributed latency = submit -> COMPLETED observed by %dms status sampling; each of the
                          3 waves waits for a poller pull (<=%dms x 5 staggered workers), so its floor is queue
                          cadence, not compute — the trade for fleet-wide spreading + fire-and-forget submits."""
                        .formatted(STATUS_SAMPLE_MS, POLL_MS));
            } finally {
                for (Worker w : ws) w.drain(5_000);
                for (ManagedChannel ch : fleet.channels()) { ch.shutdown(); ch.awaitTermination(3, TimeUnit.SECONDS); }
            }
        };
    }

    // ---------- fleet wiring ----------
    private Fleet connectFleet(ObjectMapper json) throws Exception {
        String sticky = load("relay/workflows/loan-sticky.json");
        String dist = load("relay/workflows/loan-distributed.json");
        List<Worker> ws = new ArrayList<>();
        List<ManagedChannel> chs = new ArrayList<>();
        for (int i = 1; i <= WORKERS; i++) {
            String target = (i % 2 == 1) ? "localhost:9090" : "localhost:9091";   // alternate across the 2 servers
            ManagedChannel ch = NettyChannelBuilder.forTarget(target).usePlaintext()
                    .keepAliveTime(30, TimeUnit.SECONDS).keepAliveWithoutCalls(true)   // warm channels between polls
                    .executor(Executors.newVirtualThreadPerTaskExecutor())             // callback offload
                    .build();
            Worker w = new Worker("default", new GrpcCoordinationClient(ch, "fleet-w" + i, 16));
            FleetTasks.registerOn(w, "fleet-w" + i, json);
            w.registerSpec(sticky).registerSpec(dist);
            w.startHeartbeats(2_000);
            w.startRecoveryPoller(POLL_MS, 16);            // the distributed work loop (5 competing pollers)
            ws.add(w); chs.add(ch);
        }
        return new Fleet(ws, chs);
    }

    private static String load(String cp) throws Exception {
        try (var in = FleetApplication.class.getClassLoader().getResourceAsStream(cp)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> input(int i) {
        return Map.of("applicant", "user-" + i, "amount", 1_000 + i);
    }

    // ---------- benchmark phases ----------
    record Result(long[] nanos, int ok, long makespanNanos) {
        String row(String mode) {
            long[] s = nanos.clone(); Arrays.sort(s);
            double rps = ok / (makespanNanos / 1e9);
            return String.format("%-18s %5d %5d %7.1fms %7.1fms %7.1fms %7.1fms %7.1fms %8.2fs %8.1f",
                    mode, s.length, ok, ms(s[0]), ms(pct(s, 50)), ms(pct(s, 90)), ms(pct(s, 99)),
                    ms(s[s.length - 1]), makespanNanos / 1e9, rps);
        }
        private static long pct(long[] sorted, int p) {
            return sorted[Math.min(sorted.length - 1, (int) Math.ceil(p / 100.0 * sorted.length) - 1)];
        }
        private static double ms(long n) { return n / 1e6; }
    }

    /** sticky: the submitter drives — latency is simply the runAsync round trip. */
    private Result benchSticky(List<Worker> ws) throws Exception {
        long[] lat = new long[RUNS];
        AtomicInteger next = new AtomicInteger(), ok = new AtomicInteger();
        long t0 = System.nanoTime();
        try (var pool = Executors.newFixedThreadPool(SUBMITTERS)) {
            List<Future<?>> fs = new ArrayList<>();
            for (int s = 0; s < SUBMITTERS; s++) fs.add(pool.submit(() -> {
                int i;
                while ((i = next.getAndIncrement()) < RUNS) {
                    long a = System.nanoTime();
                    var run = ws.get(i % WORKERS).runAsync("loan-sticky", input(i));
                    lat[i] = System.nanoTime() - a;
                    if (run.completed()) ok.incrementAndGet();
                }
                return null;
            }));
            for (Future<?> f : fs) f.get(5, TimeUnit.MINUTES);
        }
        return new Result(lat, ok.get(), System.nanoTime() - t0);
    }

    /** distributed: fire-and-forget submit, then sample status until COMPLETED. */
    private Result benchDistributed(List<Worker> ws) throws Exception {
        long[] lat = new long[RUNS];
        AtomicInteger next = new AtomicInteger(), ok = new AtomicInteger();
        long t0 = System.nanoTime();
        try (var pool = Executors.newFixedThreadPool(SUBMITTERS)) {
            List<Future<?>> fs = new ArrayList<>();
            for (int s = 0; s < SUBMITTERS; s++) fs.add(pool.submit(() -> {
                int i;
                while ((i = next.getAndIncrement()) < RUNS) {
                    long a = System.nanoTime();
                    String root = ws.get(i % WORKERS).runAsync("loan-distributed", input(i)).rootId();
                    String st = "PENDING";
                    long deadline = a + TimeUnit.SECONDS.toNanos(60);
                    while (!"COMPLETED".equals(st) && !"FAILED".equals(st) && System.nanoTime() < deadline) {
                        Thread.sleep(STATUS_SAMPLE_MS);
                        st = ws.get(i % WORKERS).status(root);
                    }
                    lat[i] = System.nanoTime() - a;
                    if ("COMPLETED".equals(st)) ok.incrementAndGet();
                }
                return null;
            }));
            for (Future<?> f : fs) f.get(5, TimeUnit.MINUTES);
        }
        return new Result(lat, ok.get(), System.nanoTime() - t0);
    }

    private static void awaitAll(Worker w, List<String> roots, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        for (String r : roots) {
            String st = w.status(r);
            while (!"COMPLETED".equals(st) && !"FAILED".equals(st) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
                st = w.status(r);
            }
        }
    }

    private static void banner(String s) { System.out.println("\n===== " + s + " ====="); }
}
