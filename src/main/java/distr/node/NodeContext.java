package distr.node;

import distr.common.ClusterState;

import java.util.concurrent.atomic.AtomicLong;

public final class NodeContext {
    private final String nodeId;
    private final String host;
    private final int port;
    private final ClusterState clusterState;
    private final NodeStore store;
    private final AtomicLong seq;
    private final ReplicationManager replicationManager;

    public NodeContext(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.clusterState = new ClusterState();
        this.store = new NodeStore();
        this.seq = new AtomicLong(0L);
        this.replicationManager = new ReplicationManager(this);
    }

    public String nodeId() {
        return nodeId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public ClusterState clusterState() {
        return clusterState;
    }

    public NodeStore store() {
        return store;
    }

    public long nextSeq() {
        return seq.incrementAndGet();
    }

    public ReplicationManager replicationManager() {
        return replicationManager;
    }
}

