package distr.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ClusterState {
    private final Map<String, NodeInfo> nodes;
    private String leaderNodeId;
    private ClusterMode mode;
    private Topology topology;
    private String starCenterNodeId;
    private ReplicationMode replicationMode;
    private int rf;
    private int semiSyncAcks;
    private int delayMinMs;
    private int delayMaxMs;

    public ClusterState() {
        this.nodes = new LinkedHashMap<>();
        this.leaderNodeId = null;
        this.mode = ClusterMode.SINGLE;
        this.topology = Topology.MESH;
        this.starCenterNodeId = null;
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

    public synchronized ClusterMode getMode() {
        return mode;
    }

    public synchronized void setMode(ClusterMode mode) {
        this.mode = mode;
    }

    public synchronized Topology getTopology() {
        return topology;
    }

    public synchronized void setTopology(Topology topology) {
        this.topology = topology;
    }

    public synchronized String getStarCenterNodeId() {
        return starCenterNodeId;
    }

    public synchronized void setStarCenterNodeId(String starCenterNodeId) {
        this.starCenterNodeId = starCenterNodeId;
    }

    public synchronized List<NodeInfo> getWriteLeaders() {
        if (mode == ClusterMode.SINGLE) {
            if (leaderNodeId == null) {
                return List.of();
            }
            NodeInfo node = nodes.get(leaderNodeId);
            return node == null ? List.of() : List.of(node);
        }
        return nodes.values().stream().filter(n -> n.role() == NodeRole.LEADER).toList();
    }

    public synchronized boolean canAcceptWrite(String nodeId) {
        if (nodeId == null) {
            return false;
        }
        if (mode == ClusterMode.SINGLE) {
            return nodeId.equals(leaderNodeId);
        }
        NodeInfo node = nodes.get(nodeId);
        return node != null && node.role() == NodeRole.LEADER;
    }

    public synchronized String suggestLeaderNodeId() {
        List<NodeInfo> leaders = getWriteLeaders();
        if (!leaders.isEmpty()) {
            return leaders.get(0).nodeId();
        }
        return leaderNodeId;
    }

    public synchronized Optional<NodeInfo> ringNext(String nodeId) {
        if (nodeId == null || nodes.isEmpty()) {
            return Optional.empty();
        }
        List<NodeInfo> sorted = nodes.values().stream().sorted(Comparator.comparing(NodeInfo::nodeId)).toList();
        int index = -1;
        for (int i = 0; i < sorted.size(); i++) {
            if (nodeId.equals(sorted.get(i).nodeId())) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            return Optional.empty();
        }
        if (sorted.size() == 1) {
            return Optional.empty();
        }
        return Optional.of(sorted.get((index + 1) % sorted.size()));
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
        this.mode = other.getMode();
        this.topology = other.getTopology();
        this.starCenterNodeId = other.getStarCenterNodeId();
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
        if (mode != null) {
            root.put(Constants.MODE, mode.toWire());
        }
        if (topology != null) {
            root.put(Constants.TOPOLOGY, topology.toWire());
        }
        if (starCenterNodeId != null) {
            root.put(Constants.STAR_CENTER_NODE_ID, starCenterNodeId);
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
        state.mode = ClusterMode.fromString(root.path(Constants.MODE).asText(null));
        if (state.mode == null) {
            state.mode = ClusterMode.SINGLE;
        }
        state.topology = Topology.fromString(root.path(Constants.TOPOLOGY).asText(null));
        if (state.topology == null) {
            state.topology = Topology.MESH;
        }
        state.starCenterNodeId = root.path(Constants.STAR_CENTER_NODE_ID).asText(null);
        state.replicationMode = ReplicationMode.fromString(root.path(Constants.REPLICATION_MODE).asText(null));
        if (state.replicationMode == null) {
            state.replicationMode = ReplicationMode.ASYNC;
        }
        state.rf = root.path(Constants.RF).asInt(1);
        state.semiSyncAcks = root.path(Constants.SEMI_SYNC_ACKS).asInt(1);
        state.delayMinMs = root.path(Constants.DELAY_MIN_MS).asInt(0);
        state.delayMaxMs = root.path(Constants.DELAY_MAX_MS).asInt(0);
        return state;
    }
}
