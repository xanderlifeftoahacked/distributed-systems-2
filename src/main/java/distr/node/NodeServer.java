package distr.node;

import distr.common.ClusterState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NetworkClient;
import distr.common.NodeInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NodeServer {
    private static final Logger LOG = Logger.getLogger(NodeServer.class.getName());
    private final NodeContext context;
    private final DedupStore dedupStore = new DedupStore();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor();

    public NodeServer(NodeContext context) {
        this.context = context;
        cleanupExecutor.execute(this::cleanupLoop);
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(context.port())) {
            LOG.info("Listening on " + context.host() + ":" + context.port());
            while (true) {
                Socket socket = serverSocket.accept();
                executor.execute(() -> handleConnection(socket));
            }
        }
    }

    private void cleanupLoop() {
        while (true) {
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            dedupStore.cleanup();
        }
    }

    private void handleConnection(Socket socket) {
        try (socket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            if (line == null) {
                return;
            }
            ObjectNode request;
            try {
                request = JsonUtil.parseObject(line);
            } catch (JsonProcessingException e) {
                ObjectNode response = errorResponse(null, Constants.ERROR_BAD_REQUEST, "Invalid JSON");
                sendResponse(writer, response);
                LOG.warning("Invalid JSON from " + socket.getRemoteSocketAddress());
                return;
            }
            ObjectNode response = handleRequest(request);
            if (response != null) {
                sendResponse(writer, response);
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "Connection error", e);
            return;
        }
    }

    private void sendResponse(BufferedWriter writer, ObjectNode response) throws IOException {
        writer.write(JsonUtil.toJson(response));
        writer.write("\n");
        writer.flush();
    }

    private ObjectNode handleRequest(ObjectNode request) {
        String type = request.path(Constants.TYPE).asText(null);
        String requestId = request.path(Constants.REQUEST_ID).asText(null);
        if (type == null) {
            return errorResponse(requestId, Constants.ERROR_BAD_REQUEST, "Missing type");
        }
        return switch (type) {
            case Constants.CLIENT_PUT -> handleClientPut(request, requestId);
            case Constants.CLIENT_GET -> handleClientGet(request, requestId);
            case Constants.CLIENT_DUMP -> handleClientDump(request, requestId);
            case Constants.CLIENT_DELETE -> handleClientDelete(request, requestId);
            case Constants.CLUSTER_UPDATE -> handleClusterUpdate(request, requestId);
            case Constants.REPL_PUT -> handleReplicationPut(request);
            case Constants.REPL_DELETE -> handleReplicationDelete(request);
            case Constants.REPL_ACK -> handleReplicationAck(request);
            default -> errorResponse(requestId, Constants.ERROR_BAD_REQUEST, "Unknown type");
        };
    }

    private ObjectNode handleClusterUpdate(ObjectNode request, String requestId) {
        ClusterState incoming = ClusterState.fromJson(request);
        context.clusterState().applyFrom(incoming);
        LOG.info("Cluster update: leader=" + incoming.getLeaderNodeId() + " rf=" + incoming.getRf() + " mode=" + incoming.getReplicationMode());
        return okResponse(requestId);
    }

    private ObjectNode handleClientPut(ObjectNode request, String requestId) {
        String key = request.path(Constants.KEY).asText(null);
        String value = request.path(Constants.VALUE).asText(null);
        if (!isValidKey(key) || value == null) {
            return errorResponse(requestId, Constants.ERROR_BAD_REQUEST, "Invalid key or value");
        }
        ClusterState cluster = context.clusterState();
        if (!Objects.equals(cluster.getLeaderNodeId(), context.nodeId())) {
            ObjectNode response = errorResponse(requestId, Constants.ERROR_NOT_LEADER, "Not a leader");
            if (cluster.getLeaderNodeId() != null) {
                response.put(Constants.LEADER_ID, cluster.getLeaderNodeId());
            }
            LOG.info("Reject PUT key=" + key + " not leader");
            return response;
        }
        ReplicationResult result = context.replicationManager().handleClientPut(key, value);
        if (result.isOk()) {
            LOG.info("PUT key=" + key + " ok");
            return okResponse(requestId);
        }
        ObjectNode response = errorResponse(requestId, result.errorCode(), result.errorCode());
        if (Constants.ERROR_NOT_LEADER.equals(result.errorCode()) && cluster.getLeaderNodeId() != null) {
            response.put(Constants.LEADER_ID, cluster.getLeaderNodeId());
        }
        LOG.info("PUT key=" + key + " error=" + result.errorCode());
        return response;
    }

    private ObjectNode handleClientDelete(ObjectNode request, String requestId) {
        String key = request.path(Constants.KEY).asText(null);
        if (!isValidKey(key)) {
            return errorResponse(requestId, Constants.ERROR_BAD_REQUEST, "Invalid key");
        }
        ClusterState cluster = context.clusterState();
        if (!Objects.equals(cluster.getLeaderNodeId(), context.nodeId())) {
            ObjectNode response = errorResponse(requestId, Constants.ERROR_NOT_LEADER, "Not a leader");
            if (cluster.getLeaderNodeId() != null) {
                response.put(Constants.LEADER_ID, cluster.getLeaderNodeId());
            }
            LOG.info("Reject DELETE key=" + key + " not leader");
            return response;
        }
        ReplicationResult result = context.replicationManager().handleClientDelete(key);
        if (result.isOk()) {
            LOG.info("DELETE key=" + key + " ok");
            return okResponse(requestId);
        }
        ObjectNode response = errorResponse(requestId, result.errorCode(), result.errorCode());
        if (Constants.ERROR_NOT_LEADER.equals(result.errorCode()) && cluster.getLeaderNodeId() != null) {
            response.put(Constants.LEADER_ID, cluster.getLeaderNodeId());
        }
        LOG.info("DELETE key=" + key + " error=" + result.errorCode());
        return response;
    }

    private ObjectNode handleClientGet(ObjectNode request, String requestId) {
        String key = request.path(Constants.KEY).asText(null);
        if (!isValidKey(key)) {
            return errorResponse(requestId, Constants.ERROR_BAD_REQUEST, "Invalid key");
        }
        ValueEntry entry = context.store().get(key);
        ObjectNode response = okResponse(requestId);
        if (entry == null || entry.tombstone()) {
            response.put(Constants.FOUND, false);
        } else {
            response.put(Constants.FOUND, true);
            response.put(Constants.VALUE, entry.value());
        }
        LOG.fine("GET key=" + key + " found=" + response.path(Constants.FOUND).asBoolean());
        return response;
    }

    private ObjectNode handleClientDump(ObjectNode request, String requestId) {
        Map<String, String> dump = context.store().dump();
        ObjectNode response = okResponse(requestId);
        ObjectNode payload = response.putObject(Constants.VALUE);
        for (Map.Entry<String, String> entry : dump.entrySet()) {
            payload.put(entry.getKey(), entry.getValue());
        }
        return response;
    }

    private ObjectNode handleReplicationPut(ObjectNode request) {
        return handleReplicationApply(request, true);
    }

    private ObjectNode handleReplicationDelete(ObjectNode request) {
        return handleReplicationApply(request, false);
    }

    private ObjectNode handleReplicationApply(ObjectNode request, boolean isPut) {
        String opId = request.path(Constants.OP_ID).asText(null);
        String origin = request.path(Constants.ORIGIN_NODE_ID).asText(null);
        String key = request.path(Constants.KEY).asText(null);
        long seq = request.path(Constants.SEQ).asLong(0L);
        if (!isValidKey(key) || opId == null || origin == null) {
            return null;
        }
        boolean seen = dedupStore.seenOrAdd(opId);
        if (!seen) {
            delayReplication();
            if (isPut) {
                String value = request.path(Constants.VALUE).asText(null);
                if (value != null) {
                    context.store().applyPut(key, value, seq);
                }
            } else {
                context.store().applyDelete(key, seq);
            }
        }
        sendAck(origin, opId);
        if (!seen) {
            LOG.fine("Applied opId=" + opId + " key=" + key);
        } else {
            LOG.fine("Duplicate opId=" + opId + " key=" + key);
        }
        return null;
    }

    private ObjectNode handleReplicationAck(ObjectNode request) {
        String opId = request.path(Constants.OP_ID).asText(null);
        String fromNodeId = request.path(Constants.FROM_NODE_ID).asText(null);
        if (opId != null && fromNodeId != null) {
            context.replicationManager().onAck(opId, fromNodeId);
            LOG.fine("ACK opId=" + opId + " from=" + fromNodeId);
        }
        return null;
    }

    private void sendAck(String originNodeId, String opId) {
        ClusterState cluster = context.clusterState();
        NodeInfo target = cluster.getNode(originNodeId).orElse(null);
        if (target == null) {
            return;
        }
        ObjectNode ack = JsonUtil.object();
        ack.put(Constants.TYPE, Constants.REPL_ACK);
        ack.put(Constants.OP_ID, opId);
        ack.put(Constants.FROM_NODE_ID, context.nodeId());
        NetworkClient.sendOneWay(target.host(), target.port(), ack, Constants.DEFAULT_TIMEOUT_MS);
    }

    private void delayReplication() {
        ClusterState cluster = context.clusterState();
        int min = cluster.getDelayMinMs();
        int max = cluster.getDelayMaxMs();
        if (max <= 0 || max < min) {
            return;
        }
        int delay = ThreadLocalRandom.current().nextInt(min, max + 1);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean isValidKey(String key) {
        return key != null && !key.isBlank() && !key.contains(" ");
    }

    private ObjectNode okResponse(String requestId) {
        ObjectNode response = JsonUtil.object();
        if (requestId != null) {
            response.put(Constants.REQUEST_ID, requestId);
        }
        response.put(Constants.STATUS, Constants.STATUS_OK);
        return response;
    }

    private ObjectNode errorResponse(String requestId, String code, String message) {
        ObjectNode response = JsonUtil.object();
        if (requestId != null) {
            response.put(Constants.REQUEST_ID, requestId);
        }
        response.put(Constants.STATUS, Constants.STATUS_ERROR);
        response.put(Constants.ERROR_CODE, code);
        response.put(Constants.ERROR_MESSAGE, message);
        return response;
    }
}

