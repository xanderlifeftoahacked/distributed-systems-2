package distr.node;

import distr.common.ClusterMode;
import distr.common.ClusterState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NetworkClient;
import distr.common.NodeInfo;
import distr.common.ReplicationMode;
import distr.common.Topology;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class ReplicationManager {
    private static final Logger LOG = Logger.getLogger(ReplicationManager.class.getName());
    private final NodeContext context;
    private final ConcurrentHashMap<String, OperationState> operations = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public ReplicationManager(NodeContext context) {
        this.context = context;
        scheduler.scheduleAtFixedRate(this::retryPending, 200, Constants.RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    public ReplicationResult handleClientPut(String key, String value) {
        ClusterState cluster = context.clusterState();
        if (!cluster.canAcceptWrite(context.nodeId())) {
            return ReplicationResult.error(Constants.ERROR_NOT_LEADER_FOR_WRITE);
        }
        long lamport = context.nextLamport();
        context.store().applyPut(key, value, lamport, context.nodeId());
        return replicate(Constants.REPL_PUT, key, value, lamport, context.nodeId());
    }

    public ReplicationResult handleClientDelete(String key) {
        ClusterState cluster = context.clusterState();
        if (!cluster.canAcceptWrite(context.nodeId())) {
            return ReplicationResult.error(Constants.ERROR_NOT_LEADER_FOR_WRITE);
        }
        long lamport = context.nextLamport();
        context.store().applyDelete(key, lamport, context.nodeId());
        return replicate(Constants.REPL_DELETE, key, null, lamport, context.nodeId());
    }

    private ReplicationResult replicate(String replType, String key, String value, long lamport, String versionNodeId) {
        ClusterState cluster = context.clusterState();
        List<NodeInfo> targets = initialTargets(cluster, context.nodeId());
        String opId = UUID.randomUUID().toString();
        if (cluster.getMode() == ClusterMode.SINGLE) {
            int rf = cluster.getRf();
            int clusterSize = cluster.getNodes().size();
            if (rf > clusterSize) {
                return ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
            }
            int requiredFollowers = Math.max(0, rf - 1);
            if (requiredFollowers > targets.size()) {
                return ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
            }
            OperationState state = new OperationState(opId, replType, key, value, lamport, versionNodeId, targets);
            operations.put(opId, state);
            sendReplication(state, state.targets());
            ReplicationMode mode = cluster.getReplicationMode();
            if (mode == ReplicationMode.ASYNC || requiredFollowers == 0) {
                return ReplicationResult.ok();
            }
            int hotAcks = requiredFollowers;
            if (mode == ReplicationMode.SEMI_SYNC) {
                hotAcks = Math.min(cluster.getSemiSyncAcks(), requiredFollowers);
            }
            boolean ok = state.waitForAcks(hotAcks, Constants.DEFAULT_TIMEOUT_MS);
            return ok ? ReplicationResult.ok() : ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
        }
        ObjectNode req = buildReplicationRequest(opId, replType, key, value, lamport, versionNodeId, context.nodeId(), context.nodeId());
        sendToTargets(req, targets);
        return ReplicationResult.ok();
    }

    public void forward(ObjectNode inbound, String sourceNodeId) {
        ClusterState cluster = context.clusterState();
        List<NodeInfo> targets = forwardTargets(cluster, context.nodeId(), sourceNodeId);
        if (targets.isEmpty()) {
            return;
        }
        ObjectNode out = inbound.deepCopy();
        out.put(Constants.SOURCE_NODE_ID, context.nodeId());
        sendToTargets(out, targets);
    }

    public void onAck(String opId, String fromNodeId) {
        OperationState state = operations.get(opId);
        if (state == null) {
            return;
        }
        state.ack(fromNodeId);
        LOG.fine("ACK opId=" + opId + " from=" + fromNodeId + " count=" + state.ackedCount());
    }

    private void sendReplication(OperationState state, List<NodeInfo> targets) {
        ObjectNode req = buildReplicationRequest(
                state.opId(),
                state.replType(),
                state.key(),
                state.value(),
                state.lamport(),
                state.versionNodeId(),
                context.nodeId(),
                context.nodeId()
        );
        sendToTargets(req, targets);
    }

    private ObjectNode buildReplicationRequest(
            String opId,
            String replType,
            String key,
            String value,
            long lamport,
            String versionNodeId,
            String originNodeId,
            String sourceNodeId
    ) {
        ObjectNode req = JsonUtil.object();
        req.put(Constants.TYPE, replType);
        req.put(Constants.OP_ID, opId);
        req.put(Constants.ORIGIN_NODE_ID, originNodeId);
        req.put(Constants.SOURCE_NODE_ID, sourceNodeId);
        req.put(Constants.KEY, key);
        ObjectNode version = req.putObject(Constants.VERSION);
        version.put(Constants.LAMPORT, lamport);
        version.put(Constants.NODE_ID, versionNodeId);
        if (Constants.REPL_PUT.equals(replType) && value != null) {
            req.put(Constants.VALUE, value);
        }
        return req;
    }

    private void sendToTargets(ObjectNode req, List<NodeInfo> targets) {
        for (NodeInfo node : targets) {
            NetworkClient.sendOneWay(node.host(), node.port(), req, Constants.DEFAULT_TIMEOUT_MS);
        }
    }

    private List<NodeInfo> initialTargets(ClusterState cluster, String selfNodeId) {
        if (cluster.getMode() == ClusterMode.SINGLE) {
            List<NodeInfo> out = new ArrayList<>();
            for (NodeInfo node : cluster.nodesValues()) {
                if (!selfNodeId.equals(node.nodeId())) {
                    out.add(node);
                }
            }
            return out;
        }
        return forwardTargets(cluster, selfNodeId, selfNodeId);
    }

    private List<NodeInfo> forwardTargets(ClusterState cluster, String selfNodeId, String sourceNodeId) {
        Topology topology = cluster.getTopology();
        if (topology == null) {
            topology = Topology.MESH;
        }
        return switch (topology) {
            case MESH -> meshTargets(cluster, selfNodeId, sourceNodeId);
            case RING -> ringTargets(cluster, selfNodeId, sourceNodeId);
            case STAR -> starTargets(cluster, selfNodeId, sourceNodeId);
        };
    }

    private List<NodeInfo> meshTargets(ClusterState cluster, String selfNodeId, String sourceNodeId) {
        List<NodeInfo> out = new ArrayList<>();
        for (NodeInfo node : cluster.nodesValues()) {
            if (selfNodeId.equals(node.nodeId())) {
                continue;
            }
            if (sourceNodeId != null && sourceNodeId.equals(node.nodeId())) {
                continue;
            }
            out.add(node);
        }
        return out;
    }

    private List<NodeInfo> ringTargets(ClusterState cluster, String selfNodeId, String sourceNodeId) {
        NodeInfo next = cluster.ringNext(selfNodeId).orElse(null);
        if (next == null) {
            return List.of();
        }
        if (sourceNodeId != null && sourceNodeId.equals(next.nodeId())) {
            return List.of();
        }
        return List.of(next);
    }

    private List<NodeInfo> starTargets(ClusterState cluster, String selfNodeId, String sourceNodeId) {
        String center = cluster.getStarCenterNodeId();
        if (center == null) {
            return meshTargets(cluster, selfNodeId, sourceNodeId);
        }
        if (selfNodeId.equals(center)) {
            return meshTargets(cluster, selfNodeId, sourceNodeId);
        }
        if (sourceNodeId != null && sourceNodeId.equals(center)) {
            return List.of();
        }
        NodeInfo node = cluster.getNode(center).orElse(null);
        if (node == null) {
            return List.of();
        }
        return List.of(node);
    }

    private void retryPending() {
        for (OperationState state : operations.values()) {
            List<NodeInfo> targets = state.unackedTargets();
            if (targets.isEmpty()) {
                continue;
            }
            sendReplication(state, targets);
        }
    }

    private void cleanup() {
        long cutoff = System.currentTimeMillis() - Constants.DEDUP_TTL_MS;
        operations.entrySet().removeIf(entry -> entry.getValue().createdAt() < cutoff || entry.getValue().isSatisfied(context.clusterState()));
    }

    private static final class OperationState {
        private final String opId;
        private final String replType;
        private final String key;
        private final String value;
        private final long lamport;
        private final String versionNodeId;
        private final long createdAt;
        private final List<NodeInfo> targets;
        private final Set<String> acked = ConcurrentHashMap.newKeySet();

        private OperationState(String opId, String replType, String key, String value, long lamport, String versionNodeId, List<NodeInfo> targets) {
            this.opId = opId;
            this.replType = replType;
            this.key = key;
            this.value = value;
            this.lamport = lamport;
            this.versionNodeId = versionNodeId;
            this.targets = targets;
            this.createdAt = System.currentTimeMillis();
        }

        public String opId() {
            return opId;
        }

        public String replType() {
            return replType;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public long lamport() {
            return lamport;
        }

        public String versionNodeId() {
            return versionNodeId;
        }

        public long createdAt() {
            return createdAt;
        }

        public List<NodeInfo> targets() {
            return targets;
        }

        public List<NodeInfo> unackedTargets() {
            List<NodeInfo> list = new ArrayList<>();
            for (NodeInfo node : targets) {
                if (!acked.contains(node.nodeId())) {
                    list.add(node);
                }
            }
            return list;
        }

        public void ack(String nodeId) {
            acked.add(nodeId);
        }

        public boolean waitForAcks(int required, int timeoutMs) {
            if (required <= 0) {
                return true;
            }
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (acked.size() >= required) {
                    return true;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return acked.size() >= required;
        }

        public boolean isSatisfied(ClusterState clusterState) {
            int followersRequired = Math.max(0, clusterState.getRf() - 1);
            return acked.size() >= followersRequired;
        }

        public int ackedCount() {
            return acked.size();
        }
    }
}
