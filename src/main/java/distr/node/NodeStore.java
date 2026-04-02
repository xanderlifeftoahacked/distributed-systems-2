package distr.node;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class NodeStore {
    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();

    public void applyPut(String key, String value, long lamport, String nodeId) {
        map.compute(key, (k, existing) -> {
            if (shouldReplace(existing, lamport, nodeId)) {
                return new ValueEntry(value, false, lamport, nodeId);
            }
            return existing;
        });
    }

    public void applyDelete(String key, long lamport, String nodeId) {
        map.compute(key, (k, existing) -> {
            if (shouldReplace(existing, lamport, nodeId)) {
                return new ValueEntry(null, true, lamport, nodeId);
            }
            return existing;
        });
    }

    public ValueEntry get(String key) {
        return map.get(key);
    }

    public Map<String, String> dump() {
        Map<String, String> out = new LinkedHashMap<>();
        for (var entry : map.entrySet()) {
            if (!entry.getValue().tombstone()) {
                out.put(entry.getKey(), entry.getValue().value());
            }
        }
        return out;
    }

    public Map<String, ValueEntry> dumpWithMetadata() {
        return new LinkedHashMap<>(map);
    }

    private boolean shouldReplace(ValueEntry existing, long lamport, String nodeId) {
        if (existing == null) {
            return true;
        }
        if (lamport > existing.lamport()) {
            return true;
        }
        if (lamport < existing.lamport()) {
            return false;
        }
        String incoming = nodeId == null ? "" : nodeId;
        String current = existing.nodeId() == null ? "" : existing.nodeId();
        return incoming.compareTo(current) >= 0;
    }
}

