package distr.cli.commands;

import distr.cli.CliState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Command(name = "get")
public final class GetCommand extends BaseCommand {
    @Parameters(index = "0")
    private String key;

    @Option(names = {"--target"})
    private String targetNodeId;

    @Option(names = {"--client"})
    private String clientId;

    @Option(names = {"--read"}, description = "any or leader")
    private String readMode = "any";

    @Override
    public void run() {
        CliState state = loadState();
        NodeInfo node = resolveTarget(state);
        if (node == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        ObjectNode request = JsonUtil.object();
        request.put(Constants.TYPE, Constants.CLIENT_GET);
        request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
        request.put(Constants.CLIENT_ID, clientId != null ? clientId : state.getDefaultClientId());
        request.put(Constants.KEY, key);
        try {
            ObjectNode response = sendRequest(node, request);
            System.out.println(response.toString());
        } catch (IOException e) {
            System.err.println("TIMEOUT");
        }
    }

    private NodeInfo resolveTarget(CliState state) {
        if (targetNodeId != null) {
            return state.getNode(targetNodeId).orElse(null);
        }
        if ("leader".equalsIgnoreCase(readMode)) {
            String leaderId = state.getLeaderNodeId();
            if (leaderId == null) {
                return null;
            }
            return state.getNode(leaderId).orElse(null);
        }
        List<NodeInfo> nodes = new ArrayList<>(state.getNodes().values());
        if (nodes.isEmpty()) {
            return null;
        }
        return nodes.get(0);
    }
}

