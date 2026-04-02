package distr.cli.commands;

import distr.cli.CliState;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setStarCenter")
public final class SetStarCenterCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Override
    public void run() {
        CliState state = loadState();
        if (!state.getNodes().containsKey(nodeId)) {
            System.err.println("UNKNOWN_NODE");
            return;
        }
        state.setStarCenterNodeId(nodeId);
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

