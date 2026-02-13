package distr.cli.commands;

import distr.cli.CliState;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "removeNode")
public final class RemoveNodeCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Override
    public void run() {
        CliState state = loadState();
        state.removeNode(nodeId);
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

