package distr.node;

public record ReplicationResult(boolean success, String errorCode) {
    public static ReplicationResult ok() {
        return new ReplicationResult(true, null);
    }

    public static ReplicationResult error(String code) {
        return new ReplicationResult(false, code);
    }

    public boolean isOk() {
        return success;
    }
}
