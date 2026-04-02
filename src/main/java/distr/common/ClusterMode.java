package distr.common;

public enum ClusterMode {
    SINGLE,
    MULTI;

    public static ClusterMode fromString(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case Constants.MODE_SINGLE -> SINGLE;
            case Constants.MODE_MULTI -> MULTI;
            default -> null;
        };
    }

    public String toWire() {
        return switch (this) {
            case SINGLE -> Constants.MODE_SINGLE;
            case MULTI -> Constants.MODE_MULTI;
        };
    }
}

