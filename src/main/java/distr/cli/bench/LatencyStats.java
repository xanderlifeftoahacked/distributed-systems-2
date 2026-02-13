package distr.cli.bench;

import java.util.Arrays;

public final class LatencyStats {
    private final long[] valuesNanos;

    public LatencyStats(long[] valuesNanos) {
        this.valuesNanos = valuesNanos;
    }

    public double avgMs() {
        long sum = 0L;
        for (long v : valuesNanos) {
            sum += v;
        }
        return nanosToMs(sum / (double) valuesNanos.length);
    }

    public double p50Ms() {
        return percentile(0.50);
    }

    public double p75Ms() {
        return percentile(0.75);
    }

    public double p95Ms() {
        return percentile(0.95);
    }

    public double p99Ms() {
        return percentile(0.99);
    }

    private double percentile(double p) {
        long[] sorted = Arrays.copyOf(valuesNanos, valuesNanos.length);
        Arrays.sort(sorted);
        int index = (int) Math.ceil(p * sorted.length) - 1;
        if (index < 0) {
            index = 0;
        }
        if (index >= sorted.length) {
            index = sorted.length - 1;
        }
        return nanosToMs(sorted[index]);
    }

    private double nanosToMs(double nanos) {
        return nanos / 1_000_000.0;
    }
}

