package distr.node;

import distr.common.ClusterState;
import distr.common.Constants;
import distr.common.NetworkClient;
import distr.common.NodeInfo;
import distr.common.ReplicationMode;
import distr.common.JsonUtil;

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
        long seq = context.nextSeq();
        context.store().applyPut(key, value, seq);
        return replicate(Constants.PUT, key, value, seq);
    }

    public ReplicationResult handleClientDelete(String key) {
        long seq = context.nextSeq();
        context.store().applyDelete(key, seq);
        return replicate(Constants.DELETE, key, null, seq);
    }

    private ReplicationResult replicate(String opType, String key, String value, long seq) {
        ClusterState cluster = context.clusterState();
        String leaderId = cluster.getLeaderNodeId();
        if (leaderId == null || !leaderId.equals(context.nodeId())) {
            return ReplicationResult.error(Constants.ERROR_NOT_LEADER);
        }
        int clusterSize = cluster.getNodes().size();
        int rf = cluster.getRf();
        if (rf > clusterSize) {
            return ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
        }
        int followersRequired = Math.max(0, rf - 1);
        List<NodeInfo> followers = new ArrayList<>();
        for (NodeInfo node : cluster.nodesValues()) {
            if (!node.nodeId().equals(context.nodeId())) {
                followers.add(node);
            }
        }
        if (followersRequired > followers.size()) {
            return ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
        }
        String opId = UUID.randomUUID().toString();
        OperationState state = new OperationState(opId, opType, key, value, seq, followers);
        operations.put(opId, state);
        LOG.info("Replicate opId=" + opId + " type=" + opType + " rf=" + rf + " mode=" + cluster.getReplicationMode());
        sendReplication(state);
        ReplicationMode mode = cluster.getReplicationMode();
        if (mode == ReplicationMode.ASYNC || followersRequired == 0) {
            return ReplicationResult.ok();
        }
        int hotAcks = followersRequired;
        if (mode == ReplicationMode.SEMI_SYNC) {
            int k = cluster.getSemiSyncAcks();
            hotAcks = Math.min(k, followersRequired);
        }
        boolean ok = state.waitForAcks(hotAcks, Constants.DEFAULT_TIMEOUT_MS);
        if (!ok) {
            LOG.info("Replicate opId=" + opId + " failed requiredAcks=" + hotAcks + " got=" + state.ackedCount());
        }
        return ok ? ReplicationResult.ok() : ReplicationResult.error(Constants.ERROR_NOT_ENOUGH_REPLICAS);
    }

    private void sendReplication(OperationState state) {
        for (NodeInfo node : state.targets()) {
            ObjectNode req = JsonUtil.object();
            req.put(Constants.TYPE, state.opType());
            req.put(Constants.OP_ID, state.opId());
            req.put(Constants.ORIGIN_NODE_ID, context.nodeId());
            req.put(Constants.KEY, state.key());
            req.put(Constants.SEQ, state.seq());
            if (Constants.PUT.equals(state.opType())) {
                req.put(Constants.VALUE, state.value());
                req.put(Constants.OP_TYPE, Constants.PUT);
                req.put(Constants.TYPE, Constants.REPL_PUT);
            } else {
                req.put(Constants.OP_TYPE, Constants.DELETE);
                req.put(Constants.TYPE, Constants.REPL_DELETE);
            }
            NetworkClient.sendOneWay(node.host(), node.port(), req, Constants.DEFAULT_TIMEOUT_MS);
        }
    }

    public void onAck(String opId, String fromNodeId) {
        OperationState state = operations.get(opId);
        if (state == null) {
            return;
        }
        state.ack(fromNodeId);
        LOG.fine("ACK opId=" + opId + " from=" + fromNodeId + " count=" + state.ackedCount());
    }

    private void retryPending() {
        for (OperationState state : operations.values()) {
            for (NodeInfo node : state.unackedTargets()) {
                ObjectNode req = JsonUtil.object();
                req.put(Constants.OP_ID, state.opId());
                req.put(Constants.ORIGIN_NODE_ID, context.nodeId());
                req.put(Constants.KEY, state.key());
                req.put(Constants.SEQ, state.seq());
                if (Constants.PUT.equals(state.opType())) {
                    req.put(Constants.VALUE, state.value());
                    req.put(Constants.OP_TYPE, Constants.PUT);
                    req.put(Constants.TYPE, Constants.REPL_PUT);
                } else {
                    req.put(Constants.OP_TYPE, Constants.DELETE);
                    req.put(Constants.TYPE, Constants.REPL_DELETE);
                }
                NetworkClient.sendOneWay(node.host(), node.port(), req, Constants.DEFAULT_TIMEOUT_MS);
            }
        }
    }

    private void cleanup() {
        long cutoff = System.currentTimeMillis() - Constants.DEDUP_TTL_MS;
        operations.entrySet().removeIf(entry -> entry.getValue().createdAt() < cutoff || entry.getValue().isSatisfied(context.clusterState()));
    }

    private static final class OperationState {
        private final String opId;
        private final String opType;
        private final String key;
        private final String value;
        private final long seq;
        private final long createdAt;
        private final List<NodeInfo> targets;
        private final Set<String> acked = ConcurrentHashMap.newKeySet();

        private OperationState(String opId, String opType, String key, String value, long seq, List<NodeInfo> targets) {
            this.opId = opId;
            this.opType = opType;
            this.key = key;
            this.value = value;
            this.seq = seq;
            this.targets = targets;
            this.createdAt = System.currentTimeMillis();
        }

        public String opId() {
            return opId;
        }

        public String opType() {
            return opType;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public long seq() {
            return seq;
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
