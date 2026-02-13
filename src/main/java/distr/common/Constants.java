package distr.common;

public final class Constants {
    public static final String TYPE = "type";
    public static final String REQUEST_ID = "requestId";
    public static final String CLIENT_ID = "clientId";
    public static final String STATUS = "status";
    public static final String ERROR_CODE = "errorCode";
    public static final String ERROR_MESSAGE = "errorMessage";
    public static final String LEADER_ID = "leaderNodeId";
    public static final String VALUE = "value";
    public static final String KEY = "key";
    public static final String FOUND = "found";
    public static final String NODES = "nodes";
    public static final String NODE_ID = "nodeId";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String OP_ID = "operationId";
    public static final String ORIGIN_NODE_ID = "originNodeId";
    public static final String OP_TYPE = "operationType";
    public static final String SEQ = "seq";
    public static final String FROM_NODE_ID = "fromNodeId";
    public static final String REPLICATION_MODE = "replicationMode";
    public static final String RF = "rf";
    public static final String SEMI_SYNC_ACKS = "semiSyncAcks";
    public static final String DELAY_MIN_MS = "delayMinMs";
    public static final String DELAY_MAX_MS = "delayMaxMs";
    public static final String STATUS_OK = "OK";
    public static final String STATUS_ERROR = "ERROR";
    public static final String ERROR_NOT_LEADER = "NOT_LEADER";
    public static final String ERROR_NOT_ENOUGH_REPLICAS = "NOT_ENOUGH_REPLICAS";
    public static final String ERROR_TIMEOUT = "TIMEOUT";
    public static final String ERROR_BAD_REQUEST = "BAD_REQUEST";
    public static final String ERROR_UNKNOWN_NODE = "UNKNOWN_NODE";
    public static final String CLIENT_PUT = "CLIENT_PUT";
    public static final String CLIENT_GET = "CLIENT_GET";
    public static final String CLIENT_DUMP = "CLIENT_DUMP";
    public static final String CLIENT_DELETE = "CLIENT_DELETE";
    public static final String CLUSTER_UPDATE = "CLUSTER_UPDATE";
    public static final String REPL_PUT = "REPL_PUT";
    public static final String REPL_DELETE = "REPL_DELETE";
    public static final String REPL_ACK = "REPL_ACK";
    public static final String PUT = "PUT";
    public static final String DELETE = "DELETE";
    public static final int DEFAULT_TIMEOUT_MS = 2000;
    public static final int RETRY_DELAY_MS = 200;
    public static final long DEDUP_TTL_MS = 5 * 60 * 1000L;
    private Constants() {
    }
}

