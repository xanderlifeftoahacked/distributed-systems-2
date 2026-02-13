package distr.cli.commands;

import distr.cli.CliState;
import distr.cli.bench.BenchResult;
import distr.cli.bench.BenchRunner;
import distr.common.ReplicationMode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Command(name = "bench")
public final class BenchCommand extends BaseCommand {
    @Option(names = {"--threads"}, defaultValue = "16")
    private int threads;

    @Option(names = {"--totalOps"}, defaultValue = "20000")
    private long totalOps;

    @Option(names = {"--putRatio"}, defaultValue = "0.8")
    private double putRatio;

    @Option(names = {"--default"}, description = "Run required 18 benchmark runs")
    private boolean runDefault;

    @Option(names = {"--repeats"}, defaultValue = "3")
    private int repeats;

    @Option(names = {"--out"}, description = "Output CSV path")
    private Path outPath;

    @Override
    public void run() {
        CliState state = loadState();
        if (state.getNodes().isEmpty() || state.getLeaderNodeId() == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        Path out = outPath != null ? outPath : Path.of("benchmarks", "results.csv");
        List<BenchResult> results = new ArrayList<>();
        if (runDefault) {
            results.addAll(runDefaultBenchmarks(state));
        } else {
            results.addAll(runSingle(state));
        }
        writeResults(out, results, outPath == null || !Files.exists(out));
        System.out.println("OK");
    }

    private List<BenchResult> runSingle(CliState state) {
        String mode = state.getReplicationMode().toWire();
        int rf = state.getRf();
        int k = state.getSemiSyncAcks();
        BenchRunner runner = new BenchRunner(state);
        List<BenchResult> results = new ArrayList<>();
        int count = Math.max(1, repeats);
        for (int i = 0; i < count; i++) {
            results.add(runner.run(mode, rf, k, threads, putRatio, totalOps));
        }
        return results;
    }

    private List<BenchResult> runDefaultBenchmarks(CliState state) {
        List<BenchResult> results = new ArrayList<>();
        int[] rfs = new int[]{1, 2, 3};
        double[] ratios = new double[]{0.8, 0.2};
        String[] modes = new String[]{"sync", "async"};
        int count = Math.max(1, repeats);
        for (String mode : modes) {
            for (int rf : rfs) {
                for (double ratio : ratios) {
                    applyConfig(state, mode, rf, 1);
                    for (int i = 0; i < count; i++) {
                        results.add(new BenchRunner(state).run(mode, rf, state.getSemiSyncAcks(), threads, ratio, totalOps));
                    }
                }
            }
        }
        String[] modesB = new String[]{"async", "semi-sync", "sync"};
        for (String mode : modesB) {
            for (double ratio : ratios) {
                int k = mode.equals("semi-sync") ? 1 : state.getSemiSyncAcks();
                applyConfig(state, mode, 3, k);
                for (int i = 0; i < count; i++) {
                    results.add(new BenchRunner(state).run(mode, 3, state.getSemiSyncAcks(), threads, ratio, totalOps));
                }
            }
        }
        saveState(state);
        return results;
    }

    private void applyConfig(CliState state, String mode, int rf, int k) {
        state.setReplicationMode(ReplicationMode.fromString(mode));
        state.setRf(rf);
        state.setSemiSyncAcks(k);
        state.setDelayRange(0, 0);
        saveState(state);
        broadcastClusterUpdate(state);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void writeResults(Path out, List<BenchResult> results, boolean writeHeader) {
        try {
            Files.createDirectories(out.getParent());
        } catch (IOException e) {
            return;
        }
        List<String> lines = new ArrayList<>();
        if (writeHeader) {
            lines.add("replicationMode,rf,k,threads,putRatio,totalOps,throughputOpsSec,avgMs,p50Ms,p75Ms,p95Ms,p99Ms");
        }
        for (BenchResult result : results) {
            lines.add(result.toCsvRow());
        }
        try {
            if (writeHeader) {
                Files.write(out, lines);
            } else {
                Files.write(out, lines, java.nio.file.StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            return;
        }
    }
}
