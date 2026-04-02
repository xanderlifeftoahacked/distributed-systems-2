package distr.common;

public enum NodeRole {
    LEADER,
    FOLLOWER;

    public static NodeRole fromString(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case Constants.ROLE_LEADER -> LEADER;
            case Constants.ROLE_FOLLOWER -> FOLLOWER;
            default -> null;
        };
    }

    public String toWire() {
        return switch (this) {
            case LEADER -> Constants.ROLE_LEADER;
            case FOLLOWER -> Constants.ROLE_FOLLOWER;
        };
    }
}

