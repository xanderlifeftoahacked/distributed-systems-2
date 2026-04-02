package distr.node;

import distr.common.Constants;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DedupStore {
    private final ConcurrentHashMap<String, Long> seen = new ConcurrentHashMap<>();

    public boolean seenOrAdd(String opId) {
        if (opId == null || opId.isBlank()) {
            return false;
        }
        long now = System.currentTimeMillis();
        Long existing = seen.putIfAbsent(opId, now);
        return existing != null;
    }

    public void cleanup() {
        long cutoff = System.currentTimeMillis() - Constants.DEDUP_TTL_MS;
        Iterator<Map.Entry<String, Long>> iterator = seen.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (entry.getValue() < cutoff) {
                iterator.remove();
            }
        }
    }
}

