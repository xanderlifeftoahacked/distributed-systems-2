package distr.cli.commands;

import distr.cli.CliState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import picocli.CommandLine.Command;

import java.io.IOException;
import java.util.UUID;

@Command(name = "clusterDump")
public final class ClusterDumpCommand extends BaseCommand {
    @Override
    public void run() {
        CliState state = loadState();
        for (NodeInfo node : state.getNodes().values()) {
            ObjectNode request = JsonUtil.object();
            request.put(Constants.TYPE, Constants.CLIENT_DUMP);
            request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
            request.put(Constants.CLIENT_ID, state.getDefaultClientId());
            try {
                ObjectNode response = sendRequest(node, request);
                System.out.println(node.nodeId() + " " + response.toString());
            } catch (IOException e) {
                System.out.println(node.nodeId() + " TIMEOUT");
            }
        }
    }
}

