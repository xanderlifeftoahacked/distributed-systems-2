package distr.cli.commands;

import distr.cli.CliState;
import distr.common.ClusterMode;
import distr.common.NodeInfo;
import distr.common.NodeRole;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "demoteLeader")
public final class DemoteLeaderCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Override
    public void run() {
        CliState state = loadState();
        NodeInfo current = state.getNode(nodeId).orElse(null);
        if (current == null) {
            System.err.println("UNKNOWN_NODE");
            return;
        }

        // In single mode, do not allow removing the active write leader.
        if (state.getMode() == ClusterMode.SINGLE && nodeId.equals(state.getLeaderNodeId())) {
            System.err.println("INVALID_CONFIG");
            return;
        }

        long leaderCount = state.getNodes().values().stream().filter(n -> n.role() == NodeRole.LEADER).count();
        if (state.getMode() == ClusterMode.MULTI && current.role() == NodeRole.LEADER && leaderCount <= 1) {
            System.err.println("INVALID_CONFIG");
            return;
        }

        state.upsertNode(new NodeInfo(current.nodeId(), current.host(), current.port(), NodeRole.FOLLOWER));

        if (nodeId.equals(state.getLeaderNodeId())) {
            String fallbackLeader = state.getNodes().values().stream()
                    .filter(n -> !n.nodeId().equals(nodeId) && n.role() == NodeRole.LEADER)
                    .map(NodeInfo::nodeId)
                    .findFirst()
                    .orElse(null);
            state.setLeaderNodeId(fallbackLeader);
        }

        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

