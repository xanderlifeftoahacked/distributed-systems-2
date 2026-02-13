package distr.cli.commands;

import distr.cli.CliState;
import distr.common.NodeInfo;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "addNode")
public final class AddNodeCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Parameters(index = "1")
    private String host;

    @Parameters(index = "2")
    private int port;

    @Override
    public void run() {
        CliState state = loadState();
        state.upsertNode(new NodeInfo(nodeId, host, port));
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

