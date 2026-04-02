package distr.node;

public record ValueEntry(String value, boolean tombstone, long lamport, String nodeId) {
}

