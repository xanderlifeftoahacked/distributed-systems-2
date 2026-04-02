package distr.common;

import com.fasterxml.jackson.databind.node.ObjectNode;

public record NodeInfo(String nodeId, String host, int port, NodeRole role) {
    public NodeInfo(String nodeId, String host, int port) {
        this(nodeId, host, port, NodeRole.FOLLOWER);
    }

    public ObjectNode toJson() {
        ObjectNode node = JsonUtil.object();
        node.put(Constants.NODE_ID, nodeId);
        node.put(Constants.HOST, host);
        node.put(Constants.PORT, port);
        node.put(Constants.ROLE, role == null ? NodeRole.FOLLOWER.toWire() : role.toWire());
        return node;
    }

    public static NodeInfo fromJson(ObjectNode node) {
        String id = node.path(Constants.NODE_ID).asText(null);
        String host = node.path(Constants.HOST).asText(null);
        int port = node.path(Constants.PORT).asInt(-1);
        NodeRole role = NodeRole.fromString(node.path(Constants.ROLE).asText(null));
        if (role == null) {
            role = NodeRole.FOLLOWER;
        }
        return new NodeInfo(id, host, port, role);
    }
}

