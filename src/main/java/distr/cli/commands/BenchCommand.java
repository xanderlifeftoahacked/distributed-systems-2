package distr.cli.commands;

import distr.cli.CliState;
import distr.cli.bench.BenchResult;
import distr.cli.bench.BenchRunner;
import distr.common.ClusterMode;
import distr.common.NodeRole;
import distr.common.ReplicationMode;
import distr.common.Topology;
import distr.common.NodeInfo;

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

    @Option(names = {"--totalOps"}, defaultValue = "200000")
    private long totalOps;

    @Option(names = {"--putRatio"}, defaultValue = "0.8")
    private double putRatio;

    @Option(names = {"--default"}, description = "Run required benchmark set from TASK.md")
    private boolean runDefault;

    @Option(names = {"--repeats"}, defaultValue = "3")
    private int repeats;

    @Option(names = {"--out"}, description = "Output CSV path")
    private Path outPath;

    @Option(names = {"--keySpace"}, defaultValue = "10000")
    private int keySpace;

    @Override
    public void run() {
        CliState state = loadState();
        if (state.getNodes().isEmpty() || state.getWriteLeaders().isEmpty()) {
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
            results.add(runner.run(mode, rf, k, threads, putRatio, totalOps, keySpace));
        }
        return results;
    }

    private List<BenchResult> runDefaultBenchmarks(CliState state) {
        List<BenchResult> results = new ArrayList<>();
        int count = Math.max(1, repeats);

        ensureMultiLeadersIfMissing(state);

        double[] ratios = new double[]{0.8, 0.2};
        Topology[] topologies = new Topology[]{Topology.MESH, Topology.RING, Topology.STAR};
        for (Topology topology : topologies) {
            for (double ratio : ratios) {
                applyConfig(state, ClusterMode.MULTI, topology, "async", 3, 1, 0, 0);
                for (int i = 0; i < count; i++) {
                    results.add(new BenchRunner(state).run("async", state.getRf(), state.getSemiSyncAcks(), 16, ratio, Math.max(totalOps, 200_000), 10_000));
                }
            }
        }

        int[] conflictKeySpaces = new int[]{5, 10_000};
        for (int ks : conflictKeySpaces) {
            applyConfig(state, ClusterMode.MULTI, Topology.MESH, "async", 3, 1, 0, 0);
            for (int i = 0; i < count; i++) {
                results.add(new BenchRunner(state).run("async", state.getRf(), state.getSemiSyncAcks(), 32, 1.0, Math.max(totalOps, 200_000), ks));
            }
        }

        applyConfig(state, ClusterMode.SINGLE, Topology.MESH, "async", 1, 1, 0, 0);
        for (int i = 0; i < count; i++) {
            results.add(new BenchRunner(state).run("async", state.getRf(), state.getSemiSyncAcks(), 16, 0.8, Math.max(totalOps, 200_000), 10_000));
        }

        applyConfig(state, ClusterMode.SINGLE, Topology.MESH, "sync", 3, 1, 0, 0);
        for (int i = 0; i < count; i++) {
            results.add(new BenchRunner(state).run("sync", state.getRf(), state.getSemiSyncAcks(), 16, 0.8, Math.max(totalOps, 200_000), 10_000));
        }

        Topology[] compareTopologies = new Topology[]{Topology.MESH, Topology.STAR};
        for (Topology topology : compareTopologies) {
            applyConfig(state, ClusterMode.MULTI, topology, "async", 3, 1, 0, 0);
            for (int i = 0; i < count; i++) {
                results.add(new BenchRunner(state).run("async", state.getRf(), state.getSemiSyncAcks(), 16, 0.8, Math.max(totalOps, 200_000), 10_000));
            }
        }

        saveState(state);
        return results;
    }

    private void ensureMultiLeadersIfMissing(CliState state) {
        if (!state.getWriteLeaders().isEmpty()) {
            return;
        }
        int marked = 0;
        for (NodeInfo node : new ArrayList<>(state.getNodes().values())) {
            NodeRole role = marked < 2 ? NodeRole.LEADER : NodeRole.FOLLOWER;
            state.upsertNode(new NodeInfo(node.nodeId(), node.host(), node.port(), role));
            marked++;
        }
        if (state.getLeaderNodeId() == null && !state.getNodes().isEmpty()) {
            state.setLeaderNodeId(new ArrayList<>(state.getNodes().values()).get(0).nodeId());
        }
    }

    private void applyConfig(
            CliState state,
            ClusterMode clusterMode,
            Topology topology,
            String mode,
            int rf,
            int k,
            int delayMin,
            int delayMax
    ) {
        state.setMode(clusterMode);
        state.setTopology(topology);
        if (topology == Topology.STAR && state.getStarCenterNodeId() == null && !state.getNodes().isEmpty()) {
            state.setStarCenterNodeId(new ArrayList<>(state.getNodes().values()).get(0).nodeId());
        }
        state.setReplicationMode(ReplicationMode.fromString(mode));
        state.setRf(rf);
        state.setSemiSyncAcks(k);
        state.setDelayRange(delayMin, delayMax);
        if (clusterMode == ClusterMode.SINGLE && state.getLeaderNodeId() == null && !state.getNodes().isEmpty()) {
            state.setLeaderNodeId(new ArrayList<>(state.getNodes().values()).get(0).nodeId());
        }
        if (clusterMode == ClusterMode.MULTI) {
            ensureMultiLeadersIfMissing(state);
        }
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
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
        } catch (IOException e) {
            return;
        }
        List<String> lines = new ArrayList<>();
        if (writeHeader) {
            lines.add("clusterMode,topology,replicationMode,rf,k,threads,putRatio,totalOps,keySpace,throughputOpsSec,avgMs,p50Ms,p75Ms,p95Ms,p99Ms");
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
