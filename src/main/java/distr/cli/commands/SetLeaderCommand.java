package distr.cli.commands;

import distr.cli.CliState;
import distr.common.NodeInfo;
import distr.common.NodeRole;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setLeader")
public final class SetLeaderCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Override
    public void run() {
        CliState state = loadState();
        if (!state.getNodes().containsKey(nodeId)) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        state.setLeaderNodeId(nodeId);
        NodeInfo current = state.getNode(nodeId).orElse(null);
        if (current != null) {
            state.upsertNode(new NodeInfo(current.nodeId(), current.host(), current.port(), NodeRole.LEADER));
        }
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

