package distr.cli.commands;

import distr.cli.CliState;
import distr.common.ClusterMode;
import distr.common.NodeInfo;
import distr.common.NodeRole;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setMode")
public final class SetModeCommand extends BaseCommand {
    @Parameters(index = "0")
    private String mode;

    @Override
    public void run() {
        CliState state = loadState();
        ClusterMode parsed = ClusterMode.fromString(mode);
        if (parsed == null) {
            System.err.println("INVALID_MODE");
            return;
        }
        if (parsed == ClusterMode.MULTI) {
            boolean hasLeader = false;
            for (NodeInfo node : state.getNodes().values()) {
                if (node.role() == NodeRole.LEADER) {
                    hasLeader = true;
                    break;
                }
            }
            if (!hasLeader) {
                System.err.println("INVALID_CONFIG");
                return;
            }
        }
        state.setMode(parsed);
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

