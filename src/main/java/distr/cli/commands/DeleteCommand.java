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
import java.util.UUID;

@Command(name = "delete")
public final class DeleteCommand extends BaseCommand {
    @Parameters(index = "0")
    private String key;

    @Option(names = {"--target"})
    private String targetNodeId;

    @Option(names = {"--client"})
    private String clientId;

    @Override
    public void run() {
        CliState state = loadState();
        NodeInfo node = resolveTarget(state);
        if (node == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        ObjectNode request = JsonUtil.object();
        request.put(Constants.TYPE, Constants.CLIENT_DELETE);
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
        java.util.List<NodeInfo> leaders = state.getWriteLeaders();
        if (!leaders.isEmpty()) {
            return leaders.get(0);
        }
        return null;
    }
}

