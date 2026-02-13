package distr.cli;

import distr.common.ClusterState;
import distr.common.NodeInfo;
import distr.common.ReplicationMode;
import distr.common.Constants;
import distr.common.JsonUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class CliState {
    private final Map<String, NodeInfo> nodes = new LinkedHashMap<>();
    private String leaderNodeId;
    private ReplicationMode replicationMode = ReplicationMode.ASYNC;
    private int rf = 1;
    private int semiSyncAcks = 1;
    private int delayMinMs = 0;
    private int delayMaxMs = 0;
    private String defaultClientId = "cli";

    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }

    public Optional<NodeInfo> getNode(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public void upsertNode(NodeInfo node) {
        nodes.put(node.nodeId(), node);
    }

    public void removeNode(String nodeId) {
        nodes.remove(nodeId);
        if (nodeId != null && nodeId.equals(leaderNodeId)) {
            leaderNodeId = null;
        }
    }

    public String getLeaderNodeId() {
        return leaderNodeId;
    }

    public void setLeaderNodeId(String leaderNodeId) {
        this.leaderNodeId = leaderNodeId;
    }

    public ReplicationMode getReplicationMode() {
        return replicationMode;
    }

    public void setReplicationMode(ReplicationMode replicationMode) {
        this.replicationMode = replicationMode;
    }

    public int getRf() {
        return rf;
    }

    public void setRf(int rf) {
        this.rf = rf;
    }

    public int getSemiSyncAcks() {
        return semiSyncAcks;
    }

    public void setSemiSyncAcks(int semiSyncAcks) {
        this.semiSyncAcks = semiSyncAcks;
    }

    public int getDelayMinMs() {
        return delayMinMs;
    }

    public int getDelayMaxMs() {
        return delayMaxMs;
    }

    public void setDelayRange(int min, int max) {
        this.delayMinMs = min;
        this.delayMaxMs = max;
    }

    public String getDefaultClientId() {
        return defaultClientId;
    }

    public void setDefaultClientId(String defaultClientId) {
        this.defaultClientId = defaultClientId;
    }

    public ClusterState toClusterState() {
        ClusterState state = new ClusterState();
        for (NodeInfo node : nodes.values()) {
            state.upsertNode(node);
        }
        state.setLeaderNodeId(leaderNodeId);
        state.setReplicationMode(replicationMode);
        state.setRf(rf);
        state.setSemiSyncAcks(semiSyncAcks);
        state.setDelayRangeMs(delayMinMs, delayMaxMs);
        return state;
    }

    public ObjectNode toJson() {
        ObjectNode root = JsonUtil.object();
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
        root.put(Constants.CLIENT_ID, defaultClientId);
        return root;
    }

    public static CliState fromJson(ObjectNode root) {
        CliState state = new CliState();
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
        state.defaultClientId = root.path(Constants.CLIENT_ID).asText("cli");
        return state;
    }
}

