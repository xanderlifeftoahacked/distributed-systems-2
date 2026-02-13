package distr.node;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class NodeStore {
    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();

    public void applyPut(String key, String value, long seq) {
        map.compute(key, (k, existing) -> {
            if (existing == null || seq >= existing.seq()) {
                return new ValueEntry(value, false, seq);
            }
            return existing;
        });
    }

    public void applyDelete(String key, long seq) {
        map.compute(key, (k, existing) -> {
            if (existing == null || seq >= existing.seq()) {
                return new ValueEntry(null, true, seq);
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
}

