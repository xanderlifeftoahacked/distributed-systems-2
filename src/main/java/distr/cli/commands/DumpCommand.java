package distr.cli.commands;

import distr.cli.CliState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.UUID;

@Command(name = "dump")
public final class DumpCommand extends BaseCommand {
    @Option(names = {"--target"})
    private String targetNodeId;

    @Override
    public void run() {
        CliState state = loadState();
        String target = targetNodeId != null ? targetNodeId : state.getLeaderNodeId();
        if (target == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        NodeInfo node = state.getNode(target).orElse(null);
        if (node == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        ObjectNode request = JsonUtil.object();
        request.put(Constants.TYPE, Constants.CLIENT_DUMP);
        request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
        request.put(Constants.CLIENT_ID, state.getDefaultClientId());
        try {
            ObjectNode response = sendRequest(node, request);
            System.out.println(response.toString());
        } catch (IOException e) {
            System.err.println("TIMEOUT");
        }
    }
}

