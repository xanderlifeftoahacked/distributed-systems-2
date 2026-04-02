package distr.common;

public enum ReplicationMode {
    ASYNC,
    SYNC,
    SEMI_SYNC;

    public static ReplicationMode fromString(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case "async" -> ASYNC;
            case "sync" -> SYNC;
            case "semi-sync" -> SEMI_SYNC;
            case "semi_sync" -> SEMI_SYNC;
            default -> null;
        };
    }

    public String toWire() {
        return switch (this) {
            case ASYNC -> "async";
            case SYNC -> "sync";
            case SEMI_SYNC -> "semi-sync";
        };
    }
}

