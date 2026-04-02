package distr.common;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ListUtil {
    private ListUtil() {
    }

    public static <K, V> Collection<V> copyValues(Map<K, V> map) {
        return List.copyOf(new LinkedHashMap<>(map).values());
    }
}

