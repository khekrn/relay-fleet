package com.example.fleet;

import com.cs.relay.worker.GrpcCoordinationClient;
import com.cs.relay.worker.Worker;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Sustained soak benchmark — SYNC vs ASYNC_STICKY on the loan-approval workflow
 * (validate → PARALLEL[credit, fraud, kyc] → decide; 5 × 8ms tasks), closed-loop with N concurrent
 * submitters for a fixed wall-clock duration per mode. Prints a per-minute timeline (throughput +
 * percentiles) to expose degradation over time, then a final summary per mode.
 *
 * SYNC: the worker walks the graph locally (branches sequential — 5 × 8ms serial compute) and
 * checkpoints every step to the server (startExecution + 2 recordStep/task + completeExecution
 * ≈ 12 unary calls/run, each a server-side transaction).
 * ASYNC_STICKY: the server drives; the submitter executes 3 dispatch waves (parallel branches
 * concurrent — ~24ms serial compute) ≈ 8 calls/run.
 *
 * Activate with --spring.profiles.active=soak. Knobs: -Dsoak.minutes=30 -Dsoak.submitters=10
 * -Dsoak.modes=SYNC,STICKY -Dsoak.warmup-seconds=30
 */
@Component
@Profile("soak")
public class SoakRunner implements CommandLineRunner {

    static final int WORKERS = 5;
    static final int SUBMITTERS = Integer.getInteger("soak.submitters", 10);
    static final long MINUTES = Long.getLong("soak.minutes", 30);
    static final long WARMUP_S = Long.getLong("soak.warmup-seconds", 30);
    static final String MODES = System.getProperty("soak.modes", "SYNC,STICKY");

    private final ObjectMapper json = new ObjectMapper();

    record Fleet(List<Worker> workers, List<ManagedChannel> channels) {}

    @Override
    public void run(String... args) throws Exception {
        Fleet fleet = connect();
        try {
            System.out.printf("===== SOAK: %d min/mode, %d submitters, modes=[%s], %d workers x 2 servers =====%n",
                    MINUTES, SUBMITTERS, MODES, WORKERS);
            for (String mode : MODES.split(",")) runMode(mode.trim().toUpperCase(), fleet.workers());
        } finally {
            for (Worker w : fleet.workers()) w.drain(5_000);
            for (ManagedChannel ch : fleet.channels()) { ch.shutdown(); ch.awaitTermination(3, TimeUnit.SECONDS); }
        }
    }

    private Fleet connect() throws Exception {
        String sync = load("relay/workflows/loan-sync.json");
        String sticky = load("relay/workflows/loan-sticky.json");
        List<Worker> ws = new ArrayList<>();
        List<ManagedChannel> chs = new ArrayList<>();
        for (int i = 1; i <= WORKERS; i++) {
            String target = (i % 2 == 1) ? "localhost:9090" : "localhost:9091";
            ManagedChannel ch = NettyChannelBuilder.forTarget(target).usePlaintext()
                    .keepAliveTime(30, TimeUnit.SECONDS).keepAliveWithoutCalls(true)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .build();
            Worker w = new Worker("default", new GrpcCoordinationClient(ch, "soak-w" + i, 16));
            FleetTasks.registerOn(w, "soak-w" + i, json);
            w.registerSpec(sync).registerSpec(sticky);
            w.startHeartbeats(2_000);
            ws.add(w); chs.add(ch);
        }
        return new Fleet(ws, chs);
    }

    private static String load(String cp) throws Exception {
        try (var in = SoakRunner.class.getClassLoader().getResourceAsStream(cp)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void runMode(String mode, List<Worker> ws) throws Exception {
        String wf = switch (mode) {
            case "SYNC" -> "loan-sync";
            case "STICKY" -> "loan-sticky";
            default -> throw new IllegalArgumentException("unknown soak mode: " + mode);
        };
        System.out.printf("%n===== %s (%s) — warmup %ds, then %d min sustained =====%n", mode, wf, WARMUP_S, MINUTES);
        loop(mode, ws, wf, WARMUP_S * 1_000L, null);
        Samples s = new Samples((int) Math.min(20_000_000L, MINUTES * 60 * 2_500));
        loop(mode, ws, wf, MINUTES * 60_000L, s);
        s.summary(mode);
    }

    /** closed loop: SUBMITTERS threads submit back-to-back until the deadline. s == null → warmup (untimed). */
    private void loop(String mode, List<Worker> ws, String wf, long durationMs, Samples s) throws Exception {
        long t0 = System.nanoTime();
        long end = t0 + durationMs * 1_000_000L;
        AtomicInteger seq = new AtomicInteger();
        Thread reporter = null;
        if (s != null) { s.start(t0); reporter = startReporter(mode, s); }
        try (ExecutorService pool = Executors.newFixedThreadPool(SUBMITTERS)) {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < SUBMITTERS; t++) fs.add(pool.submit(() -> {
                while (System.nanoTime() < end) {
                    int i = seq.getAndIncrement();
                    Worker w = ws.get(i % WORKERS);
                    Map<String, Object> in = Map.of("applicant", "user-" + i, "amount", 1_000 + (i % 9_000));
                    long a = System.nanoTime();
                    boolean ok;
                    try {
                        if (wf.equals("loan-sync")) { w.runSync(wf, in); ok = true; }
                        else ok = w.runAsync(wf, in).completed();
                    } catch (RuntimeException e) {
                        ok = false;
                        if (s != null && s.errors.sum() < 5) System.out.println("   [err] " + e);
                    }
                    if (s != null) s.record(System.nanoTime() - a, ok);
                }
                return null;
            }));
            for (Future<?> f : fs) f.get(durationMs + 120_000, TimeUnit.MILLISECONDS);
        }
        if (s != null) s.stop();
        if (reporter != null) { reporter.interrupt(); reporter.join(2_000); }
    }

    /** one progress line per minute: last-minute throughput + percentiles, cumulative totals. */
    private Thread startReporter(String mode, Samples s) {
        Thread t = new Thread(() -> {
            int lastIdx = 0, minute = 0;
            while (true) {
                try { Thread.sleep(60_000); } catch (InterruptedException e) { return; }
                minute++;
                int n = Math.min(s.n.get(), s.lat.length);
                long[] slice = Arrays.copyOfRange(s.lat, lastIdx, n);
                Arrays.sort(slice);
                System.out.printf("   [%s %02d/%02dmin] last-min: %,d ok, %.1f/s, p50 %.1fms p99 %.1fms max %.1fms | total: %,d ok, %,d err%n",
                        mode, minute, MINUTES, slice.length, slice.length / 60.0,
                        ms(pct(slice, 50)), ms(pct(slice, 99)), slice.length == 0 ? 0 : ms(slice[slice.length - 1]),
                        n, s.errors.sum());
                lastIdx = n;
            }
        }, "soak-reporter");
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ---------- sampling / reporting ----------
    static final class Samples {
        final long[] lat; final int[] sec;
        final AtomicInteger n = new AtomicInteger();
        final LongAdder errors = new LongAdder(), overflow = new LongAdder();
        volatile long t0Nanos, tEndNanos;

        Samples(int cap) { lat = new long[cap]; sec = new int[cap]; }
        void start(long t0) { this.t0Nanos = t0; }
        void stop() { this.tEndNanos = System.nanoTime(); }
        void record(long nanos, boolean ok) {
            if (!ok) { errors.increment(); return; }
            int i = n.getAndIncrement();
            if (i >= lat.length) { overflow.increment(); return; }
            lat[i] = nanos;
            sec[i] = (int) ((System.nanoTime() - t0Nanos) / 1_000_000_000L);
        }

        void summary(String mode) {
            int total = Math.min(n.get(), lat.length);
            long elapsed = tEndNanos - t0Nanos;
            long[] all = Arrays.copyOf(lat, total);
            Arrays.sort(all);
            System.out.printf("%n===== %s SUMMARY — %,d ok, %,d err, %.1f min =====%n",
                    mode, total, errors.sum(), elapsed / 60e9);
            System.out.printf("   throughput: %.1f runs/s sustained%n", total / (elapsed / 1e9));
            System.out.printf("   latency:    min %.1f | p50 %.1f | p90 %.1f | p99 %.1f | p99.9 %.1f | max %.1f ms%n",
                    ms(all.length == 0 ? 0 : all[0]), ms(pct(all, 50)), ms(pct(all, 90)),
                    ms(pct(all, 99)), ms(pct9(all)), ms(all.length == 0 ? 0 : all[all.length - 1]));
            if (overflow.sum() > 0) System.out.printf("   (%,d samples dropped — buffer full)%n", overflow.sum());

            // per-minute timeline: spot drift/degradation over the soak
            int minutes = (int) Math.ceil(elapsed / 60e9);
            System.out.println("   per-minute timeline:");
            System.out.printf("   %-6s %8s %8s %8s %8s %8s %8s%n", "min", "runs", "runs/s", "p50", "p90", "p99", "max");
            int from = 0;
            for (int m = 0; m < minutes; m++) {
                int to = from;
                while (to < total && sec[to] < (m + 1) * 60) to++;
                // sec[] is written racily out of order at the margin — indexes are ~sorted; good enough per-minute
                long[] slice = Arrays.copyOfRange(lat, from, to);
                Arrays.sort(slice);
                if (slice.length > 0)
                    System.out.printf("   %-6d %,8d %8.1f %7.1fms %7.1fms %7.1fms %7.1fms%n",
                            m + 1, slice.length, slice.length / 60.0,
                            ms(pct(slice, 50)), ms(pct(slice, 90)), ms(pct(slice, 99)), ms(slice[slice.length - 1]));
                from = to;
            }
        }
    }

    private static long pct(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(p / 100.0 * sorted.length) - 1)];
    }
    private static long pct9(long[] sorted) {   // p99.9
        if (sorted.length == 0) return 0;
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(0.999 * sorted.length) - 1)];
    }
    private static double ms(long nanos) { return nanos / 1e6; }
}
