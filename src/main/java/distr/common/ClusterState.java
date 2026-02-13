package distr.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class ClusterState {
    private final Map<String, NodeInfo> nodes;
    private String leaderNodeId;
    private ReplicationMode replicationMode;
    private int rf;
    private int semiSyncAcks;
    private int delayMinMs;
    private int delayMaxMs;

    public ClusterState() {
        this.nodes = new LinkedHashMap<>();
        this.leaderNodeId = null;
        this.replicationMode = ReplicationMode.ASYNC;
        this.rf = 1;
        this.semiSyncAcks = 1;
        this.delayMinMs = 0;
        this.delayMaxMs = 0;
    }

    public synchronized void upsertNode(NodeInfo node) {
        nodes.put(node.nodeId(), node);
    }

    public synchronized void removeNode(String nodeId) {
        nodes.remove(nodeId);
    }

    public synchronized Map<String, NodeInfo> getNodes() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
    }

    public synchronized Collection<NodeInfo> nodesValues() {
        return ListUtil.copyValues(nodes);
    }

    public synchronized Optional<NodeInfo> getNode(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public synchronized String getLeaderNodeId() {
        return leaderNodeId;
    }

    public synchronized void setLeaderNodeId(String leaderNodeId) {
        this.leaderNodeId = leaderNodeId;
    }

    public synchronized ReplicationMode getReplicationMode() {
        return replicationMode;
    }

    public synchronized void setReplicationMode(ReplicationMode replicationMode) {
        this.replicationMode = replicationMode;
    }

    public synchronized int getRf() {
        return rf;
    }

    public synchronized void setRf(int rf) {
        this.rf = rf;
    }

    public synchronized int getSemiSyncAcks() {
        return semiSyncAcks;
    }

    public synchronized void setSemiSyncAcks(int semiSyncAcks) {
        this.semiSyncAcks = semiSyncAcks;
    }

    public synchronized int getDelayMinMs() {
        return delayMinMs;
    }

    public synchronized int getDelayMaxMs() {
        return delayMaxMs;
    }

    public synchronized void setDelayRangeMs(int min, int max) {
        this.delayMinMs = min;
        this.delayMaxMs = max;
    }

    public synchronized void applyFrom(ClusterState other) {
        nodes.clear();
        for (NodeInfo node : other.nodesValues()) {
            nodes.put(node.nodeId(), node);
        }
        this.leaderNodeId = other.getLeaderNodeId();
        this.replicationMode = other.getReplicationMode();
        this.rf = other.getRf();
        this.semiSyncAcks = other.getSemiSyncAcks();
        this.delayMinMs = other.getDelayMinMs();
        this.delayMaxMs = other.getDelayMaxMs();
    }

    public synchronized ObjectNode toJson() {
        ObjectNode root = JsonUtil.object();
        root.put(Constants.TYPE, Constants.CLUSTER_UPDATE);
        ArrayNode array = root.putArray(Constants.NODES);
        for (NodeInfo node : nodes.values()) {
            array.add(node.toJson());
        }
        if (leaderNodeId != null) {
            root.put(Constants.LEADER_ID, leaderNodeId);
        }
        if (replicationMode != null) {
            root.put(Constants.REPLICATION_MODE, replicationMode.toWire());
        }
        root.put(Constants.RF, rf);
        root.put(Constants.SEMI_SYNC_ACKS, semiSyncAcks);
        root.put(Constants.DELAY_MIN_MS, delayMinMs);
        root.put(Constants.DELAY_MAX_MS, delayMaxMs);
        return root;
    }

    public static ClusterState fromJson(ObjectNode root) {
        ClusterState state = new ClusterState();
        if (root.has(Constants.NODES)) {
            for (var node : root.withArray(Constants.NODES)) {
                if (node instanceof ObjectNode obj) {
                    NodeInfo info = NodeInfo.fromJson(obj);
                    if (info.nodeId() != null) {
                        state.upsertNode(info);
                    }
                }
            }
        }
        state.leaderNodeId = root.path(Constants.LEADER_ID).asText(null);
        state.replicationMode = ReplicationMode.fromString(root.path(Constants.REPLICATION_MODE).asText(null));
        state.rf = root.path(Constants.RF).asInt(1);
        state.semiSyncAcks = root.path(Constants.SEMI_SYNC_ACKS).asInt(1);
        state.delayMinMs = root.path(Constants.DELAY_MIN_MS).asInt(0);
        state.delayMaxMs = root.path(Constants.DELAY_MAX_MS).asInt(0);
        return state;
    }
}
