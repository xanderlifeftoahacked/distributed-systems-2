package distr.cli.bench;

import distr.cli.CliState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NetworkClient;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class BenchRunner {
    private final CliState state;
    private final NodeInfo leader;

    public BenchRunner(CliState state) {
        this.state = state;
        String leaderId = state.getLeaderNodeId();
        this.leader = leaderId == null ? null : state.getNode(leaderId).orElse(null);
    }

    public BenchResult run(String replicationMode, int rf, int k, int threads, double putRatio, long totalOps) {
        if (leader == null) {
            throw new IllegalStateException("Leader not set");
        }
        long baseOps = totalOps / threads;
        long remainder = totalOps % threads;
        long[] latencies = new long[(int) totalOps];
        long[] offsets = new long[threads];
        long cursor = 0;
        for (int i = 0; i < threads; i++) {
            offsets[i] = cursor;
            cursor += baseOps + (i < remainder ? 1 : 0);
        }
        CountDownLatch latch = new CountDownLatch(threads);
        var executor = Executors.newFixedThreadPool(threads);
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int threadIndex = t;
            final long opsForThread = baseOps + (threadIndex < remainder ? 1 : 0);
            final long offset = offsets[threadIndex];
            executor.execute(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < opsForThread; i++) {
                    boolean isPut = random.nextDouble() < putRatio;
                    String key = "k" + random.nextInt(1_000_000);
                    long opStart = System.nanoTime();
                    if (isPut) {
                        String value = "v" + random.nextInt(1_000_000);
                        sendPut(key, value, "bench-" + threadIndex);
                    } else {
                        sendGet(key, "bench-" + threadIndex);
                    }
                    long opEnd = System.nanoTime();
                    int index = (int) (offset + i);
                    if (index < latencies.length) {
                        latencies[index] = opEnd - opStart;
                    }
                }
                latch.countDown();
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long end = System.nanoTime();
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        double elapsedSec = (end - start) / 1_000_000_000.0;
        long total = baseOps * threads + remainder;
        LatencyStats stats = new LatencyStats(latencies);
        return new BenchResult(
                replicationMode,
                rf,
                k,
                threads,
                putRatio,
                total,
                total / elapsedSec,
                stats.avgMs(),
                stats.p50Ms(),
                stats.p75Ms(),
                stats.p95Ms(),
                stats.p99Ms()
        );
    }

    private void sendPut(String key, String value, String clientId) {
        ObjectNode request = JsonUtil.object();
        request.put(Constants.TYPE, Constants.CLIENT_PUT);
        request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
        request.put(Constants.CLIENT_ID, clientId);
        request.put(Constants.KEY, key);
        request.put(Constants.VALUE, value);
        try {
            NetworkClient.sendRequest(leader.host(), leader.port(), request, Constants.DEFAULT_TIMEOUT_MS);
        } catch (IOException e) {
            return;
        }
    }

    private void sendGet(String key, String clientId) {
        ObjectNode request = JsonUtil.object();
        request.put(Constants.TYPE, Constants.CLIENT_GET);
        request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
        request.put(Constants.CLIENT_ID, clientId);
        request.put(Constants.KEY, key);
        try {
            NetworkClient.sendRequest(leader.host(), leader.port(), request, Constants.DEFAULT_TIMEOUT_MS);
        } catch (IOException e) {
            return;
        }
    }
}
