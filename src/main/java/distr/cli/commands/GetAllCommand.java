package distr.cli.commands;

import distr.cli.CliState;
import distr.common.Constants;
import distr.common.JsonUtil;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.UUID;

@Command(name = "getAll")
public final class GetAllCommand extends BaseCommand {
    @Parameters(index = "0")
    private String key;

    @Override
    public void run() {
        CliState state = loadState();
        for (NodeInfo node : state.getNodes().values()) {
            ObjectNode request = JsonUtil.object();
            request.put(Constants.TYPE, Constants.CLIENT_GET);
            request.put(Constants.REQUEST_ID, UUID.randomUUID().toString());
            request.put(Constants.CLIENT_ID, state.getDefaultClientId());
            request.put(Constants.KEY, key);
            try {
                ObjectNode response = sendRequest(node, request);
                System.out.println(node.nodeId() + " " + response.toString());
            } catch (IOException e) {
                System.out.println(node.nodeId() + " TIMEOUT");
            }
        }
    }
}

