package distr.cli.commands;

import distr.CliMain;
import distr.cli.CliState;
import distr.cli.CliStateStore;
import distr.common.ClusterState;
import distr.common.Constants;
import distr.common.NetworkClient;
import distr.common.NodeInfo;

import com.fasterxml.jackson.databind.node.ObjectNode;

import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseCommand implements Runnable {
    @CommandLine.ParentCommand
    private CliMain parent;

    protected CliState loadState() {
        Path path = parent.statePath();
        return new CliStateStore(path).load();
    }

    protected void saveState(CliState state) {
        Path path = parent.statePath();
        new CliStateStore(path).save(state);
    }

    protected void broadcastClusterUpdate(CliState state) {
        ClusterState cluster = state.toClusterState();
        ObjectNode update = cluster.toJson();
        update.put(Constants.TYPE, Constants.CLUSTER_UPDATE);
        update.put(Constants.REQUEST_ID, java.util.UUID.randomUUID().toString());
        update.put(Constants.CLIENT_ID, state.getDefaultClientId());
        List<NodeInfo> nodes = new ArrayList<>(state.getNodes().values());
        for (NodeInfo node : nodes) {
            try {
                NetworkClient.sendRequest(node.host(), node.port(), update, Constants.DEFAULT_TIMEOUT_MS);
            } catch (IOException e) {
                continue;
            }
        }
    }

    protected ObjectNode sendRequest(NodeInfo node, ObjectNode request) throws IOException {
        return NetworkClient.sendRequest(node.host(), node.port(), request, Constants.DEFAULT_TIMEOUT_MS);
    }
}
