package distr.common;

public enum Topology {
    MESH,
    RING,
    STAR;

    public static Topology fromString(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case Constants.TOPOLOGY_MESH -> MESH;
            case Constants.TOPOLOGY_RING -> RING;
            case Constants.TOPOLOGY_STAR -> STAR;
            default -> null;
        };
    }

    public String toWire() {
        return switch (this) {
            case MESH -> Constants.TOPOLOGY_MESH;
            case RING -> Constants.TOPOLOGY_RING;
            case STAR -> Constants.TOPOLOGY_STAR;
        };
    }
}

