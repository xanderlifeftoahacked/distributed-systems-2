package distr.cli.bench;

public record BenchResult(
        String clusterMode,
        String topology,
        String replicationMode,
        int rf,
        int k,
        int threads,
        double putRatio,
        long totalOps,
        int keySpace,
        double throughputOpsSec,
        double avgMs,
        double p50Ms,
        double p75Ms,
        double p95Ms,
        double p99Ms
) {
    public String toCsvRow() {
        return String.join(",",
                clusterMode,
                topology,
                replicationMode,
                Integer.toString(rf),
                Integer.toString(k),
                Integer.toString(threads),
                Double.toString(putRatio),
                Long.toString(totalOps),
                Integer.toString(keySpace),
                Double.toString(throughputOpsSec),
                String.format("%.3f", avgMs),
                String.format("%.3f", p50Ms),
                String.format("%.3f", p75Ms),
                String.format("%.3f", p95Ms),
                String.format("%.3f", p99Ms)
        );
    }
}

