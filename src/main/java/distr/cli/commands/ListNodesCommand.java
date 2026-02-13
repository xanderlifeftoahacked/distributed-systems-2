package distr.cli.commands;

import distr.cli.CliState;
import distr.common.NodeInfo;

import picocli.CommandLine.Command;

@Command(name = "listNodes")
public final class ListNodesCommand extends BaseCommand {
    @Override
    public void run() {
        CliState state = loadState();
        for (NodeInfo node : state.getNodes().values()) {
            String leaderMarker = node.nodeId().equals(state.getLeaderNodeId()) ? "*" : "";
            System.out.println(node.nodeId() + " " + node.host() + ":" + node.port() + leaderMarker);
        }
    }
}

